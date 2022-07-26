package main

import (
	"context"

	"github.com/go-redis/redis/v8"
)

// Stream represents a redis stream with messages of type T.
type Stream[T any] struct {
	client redis.Cmdable
	key    string
}

// Create a new stream with messages of type T.
func NewStream[T any](client redis.Cmdable, key string) Stream[T] {
	return Stream[T]{
		client: client,
		key:    key,
	}
}

// Add a message to the stream. Calls XADD.
func (s Stream[T]) Add(ctx context.Context, v T, idarg ...string) (string, error) {
	id := ""
	if len(idarg) > 0 {
		id = idarg[0]
	}

	id, err := s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: s.key,
		Values: structToMap(v),
		ID:     id,
	}).Result()

	if err != nil {
		err = ClientError{clientError: err}
	}

	return id, err
}

// Read reads a portion of the stream. Calls XREAD.
func (s Stream[T]) Read(ctx context.Context, from, to string, count ...int64) ([]Message[T], error) {
	var redisSlice []redis.XMessage
	var err error
	if len(count) == 0 {
		redisSlice, err = s.client.XRange(ctx, s.key, from, to).Result()
	} else {
		redisSlice, err = s.client.XRangeN(ctx, s.key, from, to, count[0]).Result()
	}
	if err != nil {
		return nil, ClientError{clientError: err}
	}

	msgs := make([]Message[T], len(redisSlice))
	for i, msg := range redisSlice {
		msgs[i] = toMessage[T](msg)
	}
	return msgs, nil
}

// Len returns the current stream length. Calls XLEN.
func (s Stream[T]) Len(ctx context.Context) (int64, error) {
	len, err := s.client.XLen(ctx, s.key).Result()
	if err != nil {
		err = ClientError{clientError: err}
	}
	return len, err
}
