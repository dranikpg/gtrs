package gtrs

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Consumer is a generic consumer interface
type Consumer[T any] interface {
	Chan() <-chan Message[T]
	Close()
}

type StreamIDs = map[string]string

// StreamConsumerConfig provides basic configuration for StreamConsumer.
// It can be passed as the last argument to NewConsumer.
type StreamConsumerConfig struct {
	Block      time.Duration // milliseconds to block before timing out. 0 means infinite
	Count      int64         // maximum number of entries per request. 0 means not limited
	BufferSize uint          // how many entries to prefetch at most
}

// StreamConsumer is a consumer that reads from one or multiple redis streams.
// The consumer has to be closed to release resources and stop goroutines.
type StreamConsumer[T any] struct {
	Consumer[T]
	ctx context.Context
	rdb redis.Cmdable

	cfg StreamConsumerConfig

	fetchErrChan chan error
	fetchChan    chan fetchMessage
	consumeChan  chan Message[T] // user facing non buffered channel
	seenIds      StreamIDs

	closeFunc func()
}

// NewConsumer creates a new StreamConsumer with optional configuration.
func NewConsumer[T any](ctx context.Context, rdb redis.Cmdable, ids StreamIDs, cfgs ...StreamConsumerConfig) *StreamConsumer[T] {
	cfg := StreamConsumerConfig{
		Block:      0,
		Count:      100,
		BufferSize: 50,
	}

	if len(cfgs) > 0 {
		cfg = cfgs[0]
	}

	ctx, closeFunc := context.WithCancel(ctx)

	sc := &StreamConsumer[T]{
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

// Chan returns the main channel with new messages.
//
// This channel is closed when:
// - the consumer is closed
// - immediately on context cancel
// - in case of a ReadError
func (sc *StreamConsumer[T]) Chan() <-chan Message[T] {
	return sc.consumeChan
}

// Close returns a StreamIds that shows, up to which entry the streams were consumed.
//
// The StreamIds can be used to construct a new StreamConsumer that will
// pick up where this left off.
func (sc *StreamConsumer[T]) Close() StreamIDs {
	select {
	case <-sc.ctx.Done():
	default:
		sc.closeFunc()
	}

	// Await close.
	<-sc.consumeChan
	return sc.seenIds
}

// fetchLoop fills the fetchChan with new stream messages.
func (sc *StreamConsumer[T]) fetchLoop() {
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
			sendCheckCancel(sc.ctx, sc.fetchErrChan, err)
			// Don't close channels preemptively
			<-sc.ctx.Done()
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
func (sc *StreamConsumer[T]) consumeLoop() {
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
		if sendCheckCancel(sc.ctx, sc.consumeChan, toMessage[T](msg.message, msg.stream)) {
			sc.seenIds[msg.stream] = msg.message.ID
		}
	}
}

// read calls XREAD to read the next portion of messages from the streams.
func (sc *StreamConsumer[T]) read(fetchIds map[string]string, stBuf []string) ([]redis.XStream, error) {
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
