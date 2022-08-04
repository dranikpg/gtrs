package gtrs

import (
	"context"

	"github.com/go-redis/redis/v8"
)

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
	consumeChan   chan Message[T]    // the usual non-buffered out facing consume chan
	fetchErrChan  chan error         // fetch errors
	fetchChan     chan fetchMessage  // fetch results
	ackErrChan    chan innerAckError // ack errors
	ackChan       chan string        // ack requests
	ackBusyChan   chan struct{}      // block for waiting for ack to empty request ack chan
	ackRemainChan chan []string      // one sized immediately closed chan for storing unfinished acks

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
		AckBufferSize: 0,
	}

	if len(cfgs) > 0 {
		cfg = cfgs[0]
	}

	ctx, closeFunc := context.WithCancel(ctx)

	gc := &GroupConsumer[T]{
		rdb:           rdb,
		ctx:           ctx,
		cfg:           cfg,
		consumeChan:   make(chan Message[T]),
		fetchErrChan:  make(chan error, 1),
		fetchChan:     make(chan fetchMessage, cfg.BufferSize),
		ackErrChan:    make(chan innerAckError, 1),
		ackChan:       make(chan string, cfg.AckBufferSize),
		ackBusyChan:   make(chan struct{}),
		ackRemainChan: make(chan []string, 1),
		name:          name,
		group:         group,
		stream:        stream,
		seenId:        lastID,
		closeFunc:     closeFunc,
	}

	go gc.fetchLoop()
	go gc.consumeLoop()
	go gc.acknowledgeLoop()

	return gc
}

// Ack requests an asynchronous XAck acknowledgement call for the passed message.
// This call blocks only if the inner ack buffer is full.
func (gc *GroupConsumer[T]) Ack(msg Message[T]) {
	gc.ackChan <- msg.ID
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

// RemainingAcks closes the consumer (if not already) and returns
// a slice of unprocessed ack requests.
//
// NOTE: this function CLOSES the consumer and can only be called ONCE.
// This is because it drains all internal channels and waits for the goroutine
// to complete.
func (gc *GroupConsumer[T]) RemainingAcks() []string {
	select {
	case <-gc.ctx.Done():
	default:
		gc.Close()
	}
	return <-gc.ackRemainChan
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

// consumeLoop fills the consumeChan with new messages from all sources.
func (gc *GroupConsumer[T]) consumeLoop() {
	defer close(gc.consumeChan)

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
			sendCheckCancel(gc.ctx, gc.consumeChan,
				Message[T]{
					ID: err.id, Stream: gc.stream,
					Err: AckError{Err: err.cause},
				},
			)
			continue
		case msg = <-gc.fetchChan:
		}

		// Send message.
		sendCheckCancel(gc.ctx, gc.consumeChan, toMessage[T](msg.message, msg.stream))
		gc.seenId = msg.message.ID
	}
}

// collectMissedAck drains ackErrChan and ackChan for missed ack requests.
func (gc *GroupConsumer[T]) collectMissedAck() {
	var ids []string
	for more := true; more; {
		select {
		case msg := <-gc.ackErrChan:
			ids = append(ids, msg.id)
		case msg := <-gc.ackChan:
			ids = append(ids, msg)
		default:
			more = false
		}
	}
	gc.ackRemainChan <- ids
}

// acknowledgeLoop send XAcks for ids received from ackChan.
func (gc *GroupConsumer[T]) acknowledgeLoop() {
	defer close(gc.ackErrChan)
	defer close(gc.ackBusyChan)
	defer close(gc.ackChan)
	defer gc.collectMissedAck()

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

		if err != nil {
			sendCheckCancel(gc.ctx, gc.ackErrChan, innerAckError{id: msg, cause: err})
		}
	}
}

// ack sends an XAck message.
func (gc *GroupConsumer[T]) ack(id string) error {
	_, err := gc.rdb.XAck(gc.ctx, gc.stream, gc.group, id).Result()
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
