package gtrs

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type Consumer[T any] interface {
	Chan() <-chan Message[T]
}

type StreamIds = map[string]string

// SimpleConsumerConfig provides basic configuration for SimpleConsumer.
type SimpleConsumerConfig struct {
	Block      time.Duration // Milliseconds to block before timing out. 0 means infinite.
	Count      int64         // Maximum number of elements per request.
	BufferSize uint          // Size of internal buffer.
}

// SimpleConsumer is a consumer that reads from one or multiple channels.
type SimpleConsumer[T any] struct {
	Consumer[T]
	ctx context.Context
	rdb redis.Cmdable

	cfg SimpleConsumerConfig

	fetchErrChan chan error
	fetchChan    chan fetchMessage
	consumeChan  chan Message[T] // user facing non buffered channel
	seenIds      StreamIds

	closeFunc func()
}

// NewConsumer creates a new SimpleConsumer with optional configuration.
func NewConsumer[T any](ctx context.Context, rdb redis.Cmdable, ids StreamIds, cfgs ...SimpleConsumerConfig) *SimpleConsumer[T] {
	cfg := SimpleConsumerConfig{
		Block:      0,
		Count:      0,
		BufferSize: 10,
	}

	if len(cfgs) > 0 {
		cfg = cfgs[0]
	}

	ctx, closeFunc := context.WithCancel(ctx)

	sc := &SimpleConsumer[T]{
		ctx:          ctx,
		rdb:          rdb,
		cfg:          cfg,
		fetchErrChan: make(chan error),
		fetchChan:    make(chan fetchMessage, cfg.BufferSize),
		consumeChan:  make(chan Message[T]),
		seenIds:      ids,
		closeFunc:    closeFunc,
	}

	go sc.fetchLoop()
	go sc.consumeLoop()

	return sc
}

// Chan returns the channel for reading messages.
func (sc *SimpleConsumer[T]) Chan() <-chan Message[T] {
	return sc.consumeChan
}

// SeenIds returns a StreamIds that shows, up to which entry the streams were consumed.
//
// The StreamIds can be used to construct a new SimpleConsumer that will
// pick up where this left off.
func (sc *SimpleConsumer[T]) CloseGetSeenIds() StreamIds {
	select {
	case <-sc.ctx.Done():
	default:
		sc.Close()
	}

	// Await close.
	<-sc.consumeChan

	//e.g. return last seen ids
	return sc.seenIds
}

// Close stops the consumer and closes all channels.
func (sc *SimpleConsumer[T]) Close() {
	sc.closeFunc()
}

// fetchLoop fills the fetchChan with new stream messages.
func (sc *SimpleConsumer[T]) fetchLoop() {
	defer close(sc.fetchErrChan)
	defer close(sc.fetchChan)

	fetchedIds := copyMap(sc.seenIds)
	stBuf := make([]string, 2*len(fetchedIds))

	for {
		// Explicit check for context cancellation.
		// In case select chooses other channels over cancellation in a streak.
		if checkCancel(sc.ctx) {
			return
		}

		res, err := sc.read(fetchedIds, stBuf)
		if err != nil {
			sendCheckCancel(sc.ctx, sc.consumeChan, Message[T]{Err: ReadError{Err: err}})
			return
		}

		for _, stream := range res {
			for _, rawMsg := range stream.Messages {
				msg := fetchMessage{stream: stream.Stream, message: rawMsg}
				sendCheckCancel(sc.ctx, sc.fetchChan, msg)
				fetchedIds[stream.Stream] = rawMsg.ID
			}
		}
	}
}

// consumeLoop forwards messages from fetchChan and errorChan to consumeChan.
func (sc *SimpleConsumer[T]) consumeLoop() {
	defer close(sc.consumeChan)

	var msg fetchMessage

	for {
		// Explicit cancellation check.
		if checkCancel(sc.ctx) {
			return
		}

		// Listen for fetch message, error or context cancellation.
		select {
		case <-sc.ctx.Done():
			return
		case err := <-sc.fetchErrChan:
			sendCheckCancel(sc.ctx, sc.consumeChan, Message[T]{Err: ReadError{Err: err}})
			return
		case msg = <-sc.fetchChan:
		}

		// Send message to consumer.
		if !sendCheckCancel(sc.ctx, sc.consumeChan, toMessage[T](msg.message, msg.stream)) {
			sc.seenIds[msg.stream] = msg.message.ID
		}
	}
}

// read calls XREAD to read the next portion of messages from the streams.
func (sc *SimpleConsumer[T]) read(fetchIds map[string]string, stBuf []string) ([]redis.XStream, error) {
	idx, offset := 0, len(fetchIds)
	for k, v := range fetchIds {
		stBuf[idx] = k
		stBuf[idx+offset] = v
		idx += 1
	}
	return sc.rdb.XRead(sc.ctx, &redis.XReadArgs{
		Streams: stBuf,
		Block:   sc.cfg.Block,
		Count:   sc.cfg.Count,
	}).Result()
}
