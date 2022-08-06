package gtrs

import (
	"context"
	"errors"

	"github.com/go-redis/redis/v8"
)

var ErrAckBadRetVal = errors.New("XAck made no acknowledgement (zero return)")

// GroupConsumerConfig provides basic configuration for GroupConsumer.
type GroupConsumerConfig struct {
	SimpleConsumerConfig
	AckBufferSize uint
}

// SimpleConsumer is a consumer that reads from a consumer group.
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
		SimpleConsumerConfig: SimpleConsumerConfig{
			Block:      0,
			Count:      0,
			BufferSize: 10,
		},
		AckBufferSize: 5,
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
		ackErrChan:   make(chan innerAckError, 1),
		ackChan:      make(chan string, cfg.AckBufferSize),
		ackBusyChan:  make(chan struct{}),
		lostAcksChan: make(chan string, 3),
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

// Ack requests an asynchronous XAck acknowledgement call for the passed message.
// Returns false if the consumer is closed and this request was not recorded.
//
// Ack blocks only if the inner ack buffer is full or errors are not processed.
// If multiple ack requests fail, the consumer will wait for the errors to be processed
// via Chan(). Its best to use ack only inside a consume loop.
func (gc *GroupConsumer[T]) Ack(msg Message[T]) bool {
	return sendCheckCancel(gc.ctx, gc.ackChan, msg.ID)
}

// AwaitAcks blocks until all so far requested ack requests are processed.
func (gc *GroupConsumer[T]) AwaitAcks() {
	select {
	case <-gc.ctx.Done():
	case <-gc.ackBusyChan:
	}
}

// Chan returns the channel for reading messages.
func (gc *GroupConsumer[T]) Chan() <-chan Message[T] {
	return gc.consumeChan
}

// CloseGetRemainingAcks closes the consumer (if not already) and returns
// a slice of unprocessed ack requests.
//
// NOTE: this function CLOSES the consumer and can only be called ONCE.
// This is because it drains all internal channels and waits for the goroutine
// to complete.
func (gc *GroupConsumer[T]) CloseGetRemainingAcks() []string {
	select {
	case <-gc.ctx.Done():
	default:
		gc.Close()
	}

	// Wait for main consumer to close, as it calls recoverRemainingAcks
	for range gc.consumeChan {
	}

	return gc.lostAcks
}

// fetchLoop fills the fetchChan with new entries.
func (gc *GroupConsumer[T]) fetchLoop() {
	defer close(gc.fetchErrChan)
	defer close(gc.fetchChan)

	for {
		res, err := gc.read()

		if err != nil {
			gc.fetchErrChan <- err
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

			if len(stream.Messages) == 0 && gc.seenId != ">" {
				gc.seenId = ">"
			}
		}
	}
}

// recoverRemainingAcks collects all remaining acks from channels to remainingAcks
func (gc *GroupConsumer[T]) recoverRemainingAcks() {
	defer close(gc.lostAcksChan)

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

// consumeLoop fills the consumeChan with new messages from all sources.
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
			if !sendCheckCancel(gc.ctx, gc.consumeChan,
				Message[T]{
					ID: err.id, Stream: gc.stream,
					Err: AckError{Err: err.cause},
				},
			) {
				gc.lostAcksChan <- err.id
			}
			continue
		case msg = <-gc.fetchChan:
		}

		// Send message.
		sendCheckCancel(gc.ctx, gc.consumeChan, toMessage[T](msg.message, msg.stream))
		gc.seenId = msg.message.ID
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

// ack sends an XAck message.
func (gc *GroupConsumer[T]) ack(id string) error {
	i, err := gc.rdb.XAck(gc.ctx, gc.stream, gc.group, id).Result()
	if i == 0 {
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

// Close stops the consumer and closes all channels.
func (gc *GroupConsumer[T]) Close() {
	gc.closeFunc()
}
