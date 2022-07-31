package main

import (
	"context"
	"runtime"
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

	errorChan   chan error
	fetchChan   chan fetchMessage
	consumeChan chan Message[T]
	seenIds     StreamIds
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

	sc := &SimpleConsumer[T]{
		ctx:         ctx,
		rdb:         rdb,
		cfg:         cfg,
		errorChan:   make(chan error),
		fetchChan:   make(chan fetchMessage),
		consumeChan: make(chan Message[T]),
		seenIds:     ids,
	}

	go sc.fetchLoop()
	go sc.consumeLoop()

	return sc
}

// Chan returns the channel for reading messages.
func (sc *SimpleConsumer[T]) Chan() <-chan Message[T] {
	return sc.consumeChan
}

// SeenIds returns a StreamIds that shows, up to which the streams were consumed.
//
// The StreamIds can be used to construct a new SimpleConsumer that will
// pick up where this left off.
func (sc *SimpleConsumer[T]) SeenIds() StreamIds {
	runtime.Gosched() // TODO: invent better fix to sync ids on time
	//e.g. accept last seen id
	return sc.seenIds
}

// fetchMessage is used for sending a redis.XMessage together with its corresponding stream.
type fetchMessage struct {
	stream  string
	message redis.XMessage
}

// fetchLoop fills the fetchChan with new stream messages.
func (sc *SimpleConsumer[T]) fetchLoop() {
	fetchedIds := copyMap(sc.seenIds)
	stBuf := make([]string, 2*len(fetchedIds))

	for {
		res, err := sc.read(fetchedIds, stBuf)

		if err != nil {
			sc.errorChan <- err
			return
		}

		for _, stream := range res {
			for _, rawMsg := range stream.Messages {
				msg := fetchMessage{stream: stream.Stream, message: rawMsg}
				select {
				case <-sc.ctx.Done():
					return
				case sc.fetchChan <- msg:
					fetchedIds[stream.Stream] = rawMsg.ID
				}
			}
		}
	}
}

// consumeLoop forwards messages from fetchChan and errorChan to consumeChan.
func (sc *SimpleConsumer[T]) consumeLoop() {
	defer close(sc.errorChan)
	defer close(sc.fetchChan)
	defer close(sc.consumeChan)

	for {
		var msg fetchMessage

		// Listen for fetch message, error or context cancellation.
		select {
		case <-sc.ctx.Done():
			return
		case err := <-sc.errorChan:
			sc.consumeChan <- Message[T]{Err: ClientError{clientError: err}}
			return
		case msg = <-sc.fetchChan:
		}

		// Send message.
		select {
		case <-sc.ctx.Done():
			return
		case sc.consumeChan <- toMessage[T](msg.message, msg.stream):
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
	res := sc.rdb.XRead(sc.ctx, &redis.XReadArgs{
		Streams: stBuf,
		Block:   sc.cfg.Block,
		Count:   sc.cfg.Count,
	})
	return res.Result()
}
