package gtrs

import (
	"context"
	"errors"
	"strings"

	"github.com/redis/go-redis/v9"
)

// ErrAckBadRetVal is caused by XACK not accepting an request by returning 0.
// This usually indicates that the id is wrong or the stream has no groups.
var ErrAckBadRetVal = errors.New("XAck made no acknowledgement")

// GroupConsumerConfig provides basic configuration for GroupConsumer.
type GroupConsumerConfig struct {
	StreamConsumerConfig
	AckBufferSize uint
}

// GroupConsumer is a consumer that reads from a consumer group and similar to a StreamConsumer.
// Message acknowledgements can be sent asynchronously via Ack(). The consumer has to be closed
// to release resources and stop goroutines.
type GroupConsumer[T any] struct {
	Consumer[T]
	ctx context.Context
	rdb redis.Cmdable

	cfg GroupConsumerConfig

	// The following might look like totally over engineered
	// but is the minimal setup for seamless communication and
	// control of three goroutines (fetch, ack, consume).
	consumeChan  chan Message[T]    // the usual non-buffered out facing consume chan
	fetchErrChan chan error         // fetch errors
	fetchChan    chan fetchMessage  // fetch results
	ackErrChan   chan innerAckError // ack errors
	ackChan      chan string        // ack requests
	ackBusyChan  chan struct{}      // block for waiting for ack to empty request ack chan
	lostAcksChan chan string        // failed acks stuck in local variables on cancel

	lostAcks []string

	name   string
	group  string
	stream string
	seenId string

	closeFunc func()
}

// NewGroupConsumer creates a new GroupConsumer with optional configuration.
func NewGroupConsumer[T any](ctx context.Context, rdb redis.Cmdable, group, name, stream, lastID string, cfgs ...GroupConsumerConfig) *GroupConsumer[T] {
	cfg := GroupConsumerConfig{
		StreamConsumerConfig: StreamConsumerConfig{
			Block:      0,
			Count:      100,
			BufferSize: 20,
		},
		AckBufferSize: 10,
	}

	if len(cfgs) > 0 {
		cfg = cfgs[0]
	}

	ctx, closeFunc := context.WithCancel(ctx)

	gc := &GroupConsumer[T]{
		rdb:          rdb,
		ctx:          ctx,
		cfg:          cfg,
		consumeChan:  make(chan Message[T]),
		fetchErrChan: make(chan error, 1),
		fetchChan:    make(chan fetchMessage, cfg.BufferSize),
		ackErrChan:   make(chan innerAckError, 5),
		ackChan:      make(chan string, cfg.AckBufferSize),
		ackBusyChan:  make(chan struct{}),
		lostAcksChan: make(chan string, 5),
		name:         name,
		group:        group,
		stream:       stream,
		seenId:       lastID,
		closeFunc:    closeFunc,
	}

	go gc.fetchLoop()
	go gc.consumeLoop()
	go gc.acknowledgeLoop()

	return gc
}

// Chan returns the main channel with new messages.
//
// This channel is closed when:
// - the consumer is closed
// - immediately on context cancel
// - in case of a ReadError
func (gc *GroupConsumer[T]) Chan() <-chan Message[T] {
	return gc.consumeChan
}

// Ack requests an asynchronous XAck acknowledgement request for the passed message.
//
// NOTE: Ack sometimes provides backpressure, so it should be only used inside the consumer loop
// or with another goroutine handling errors from the consumer channel. Otherwise it may deadlock.
func (gc *GroupConsumer[T]) Ack(msg Message[T]) {
	if !sendCheckCancel(gc.ctx, gc.ackChan, msg.ID) {
		// the inner context in cancelled, so wait for ack recovery
		for range gc.consumeChan {
			panic("unreachable")
		}
		// just append it to the lost acks.
		gc.lostAcks = append(gc.lostAcks, msg.ID)
	}
}

// AwaitAcks blocks until all so far requested ack requests are processed
// and returns a slice of Messages with AckErrors that happened during wait.
func (gc *GroupConsumer[T]) AwaitAcks() []Message[T] {
	var out []Message[T]

	for {
		select {
		case <-gc.ctx.Done():
			return out
		case <-gc.ackBusyChan:
			return out
		case err := <-gc.ackErrChan:
			out = append(out, ackErrToMessage[T](err, gc.stream))
		}
	}
}

// CloseGetRemainingAcks closes the consumer (if not already closed) and returns
// a slice of unprocessed ack requests. An ack request in unprocessed if it
// wasn't sent or its error wasn't consumed.
func (gc *GroupConsumer[T]) Close() []string {
	select {
	case <-gc.ctx.Done():
	default:
		gc.closeFunc()
	}

	// Wait for main consumer to close, as it calls recoverRemainingAcks
	for range gc.consumeChan {
		panic("unreachable")
	}

	return gc.lostAcks
}

// consumeLoop fills the consumeChan with new messages from all sources.
// It is the last goroutine to exit and does remaing ack message recovery.
func (gc *GroupConsumer[T]) consumeLoop() {
	defer close(gc.consumeChan)
	defer gc.recoverRemainingAcks()

	var msg fetchMessage

	for {
		// Explicit check for context cancellation.
		// In case select chooses other channels over cancellation in a streak.
		if checkCancel(gc.ctx) {
			return
		}

		// Listen for fetch message, fetch error, acknowledge error or context cancellation.
		select {
		case <-gc.ctx.Done():
			return
		case err := <-gc.fetchErrChan:
			sendCheckCancel(gc.ctx, gc.consumeChan, Message[T]{Err: ReadError{Err: err}})
			return
		case err := <-gc.ackErrChan:
			if !sendCheckCancel(gc.ctx, gc.consumeChan, ackErrToMessage[T](err, gc.stream)) {
				gc.lostAcksChan <- err.id
			}
			continue
		case msg = <-gc.fetchChan:
		}

		// Eager consume ack messages to keep buffer free and avoid deadlock
		gc.eagerAckErrorDrain()

		// Send message.
		sendCheckCancel(gc.ctx, gc.consumeChan, toMessage[T](msg.message, msg.stream))
	}
}

// acknowledgeLoop send XAcks for ids received from ackChan.
func (gc *GroupConsumer[T]) acknowledgeLoop() {
	defer close(gc.ackErrChan)
	defer close(gc.ackChan)
	defer close(gc.ackBusyChan)

	var msg string

	for {
		// Explicit cancellation check
		if checkCancel(gc.ctx) {
			return
		}

		// The following construct:
		// A. takes a message from ackChan if one is available
		// B. blocks on ackBusyChan and ackChan. Only in this case
		// the ack worker is "not busy" and can answer `AwaitAcks` requests.

		select {
		case <-gc.ctx.Done():
			return
		case msg = <-gc.ackChan:
		default:
			select {
			case <-gc.ctx.Done():
				return
			case gc.ackBusyChan <- struct{}{}:
				continue
			case msg = <-gc.ackChan:
			}
		}

		err := gc.ack(msg)

		// Failed to send ack error. Add to lostAcksChan
		if err != nil && !sendCheckCancel(gc.ctx, gc.ackErrChan, innerAckError{id: msg, cause: err}) {
			gc.lostAcksChan <- msg
		}
	}
}

// fetchLoop fills the fetchChan with new entries from redis.
func (gc *GroupConsumer[T]) fetchLoop() {
	defer close(gc.fetchErrChan)
	defer close(gc.fetchChan)

	if err := gc.createGroup(); err != nil {
		gc.fetchErrChan <- err
		// Don't close channels preemptively
		<-gc.ctx.Done()
		return
	}

	for {
		res, err := gc.read()

		if err != nil {
			gc.fetchErrChan <- err
			<-gc.ctx.Done()
			return
		}

		for _, stream := range res {
			for _, rawMsg := range stream.Messages {
				msg := fetchMessage{stream: stream.Stream, message: rawMsg}
				select {
				case <-gc.ctx.Done():
					return
				case gc.fetchChan <- msg:
					if gc.seenId != ">" {
						gc.seenId = msg.message.ID
					}
				}
			}

			// Switch to '>' on empty response
			if len(stream.Messages) == 0 && gc.seenId != ">" {
				gc.seenId = ">"
			}
		}
	}
}

// createGroup creates a redis group, silently skips error if it exists already
func (gc *GroupConsumer[T]) createGroup() error {
	createId := gc.seenId
	if createId == ">" {
		createId = "$"
	}
	_, err := gc.rdb.XGroupCreateMkStream(gc.ctx, gc.stream, gc.group, createId).Result()
	// BUSYGROUP means the group already exists.
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// recoverRemainingAcks collects all remaining acks from channels to remainingAcks
func (gc *GroupConsumer[T]) recoverRemainingAcks() {
	defer close(gc.lostAcksChan)

	// Wait for ackLoop to stop
	for range gc.ackBusyChan {
	}

	// Wait for ackChan to close and consume it.
	for id := range gc.ackChan {
		gc.lostAcks = append(gc.lostAcks, id)
	}

	// Wait for ackErrChan to close and consume it.
	for err := range gc.ackErrChan {
		gc.lostAcks = append(gc.lostAcks, err.id)
	}

	// Empty lostAcksChan.
	var more = true
	for more {
		select {
		case id := <-gc.lostAcksChan:
			gc.lostAcks = append(gc.lostAcks, id)
		default:
			more = false
		}
	}
}

// eagerAckErrorDrain drains the ackErrChan while it has available messages
func (gc *GroupConsumer[T]) eagerAckErrorDrain() {
	var more = true
	for more {
		select {
		case <-gc.ctx.Done():
			return
		case err, gm := <-gc.ackErrChan:
			if gm && !sendCheckCancel(gc.ctx, gc.consumeChan, ackErrToMessage[T](err, gc.stream)) {
				gc.lostAcksChan <- err.id
			}
			more = gm
		default:
			more = false
		}
	}
}

// ack sends an XAck message.
func (gc *GroupConsumer[T]) ack(id string) error {
	i, err := gc.rdb.XAck(gc.ctx, gc.stream, gc.group, id).Result()
	if err == nil && i == 0 {
		return ErrAckBadRetVal
	}
	return err
}

// read reads the next portion of messages.
func (gc *GroupConsumer[T]) read() ([]redis.XStream, error) {
	return gc.rdb.XReadGroup(gc.ctx, &redis.XReadGroupArgs{
		Group:    gc.group,
		Consumer: gc.name,
		Streams:  []string{gc.stream, gc.seenId},
		Count:    gc.cfg.Count,
		Block:    gc.cfg.Block,
	}).Result()

}
