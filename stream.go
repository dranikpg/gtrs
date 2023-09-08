package gtrs

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var NoExpiration = time.Duration(0)

// now is defined here so it can be overridden in unit tests
var now = time.Now

// Stream represents a redis stream with messages of type T.
type Stream[T any] struct {
	client redis.Cmdable
	stream string
	ttl    time.Duration
}

type Options struct {
	// TTL is an optional parameter to specify how long entries stay in the stream before expiring,
	// it only only works as expected when a non-custom id is used to Add a message.
	// The default is No Expiration.
	// Note that TTL is performed when messages are Added, so Range requests won't clean up old messages.
	TTL time.Duration
}

// Create a new stream with messages of type T.
// Options are optional (the parameter can be nil to use defaults).
func NewStream[T any](client redis.Cmdable, stream string, opt *Options) Stream[T] {
	ttl := NoExpiration
	if opt != nil {
		ttl = opt.TTL
	}
	return Stream[T]{client: client, stream: stream, ttl: ttl}
}

// Key returns the redis stream key.
func (s Stream[T]) Key() string {
	return s.stream
}

// Add a message to the stream. Calls XADD.
func (s Stream[T]) Add(ctx context.Context, v T, idarg ...string) (string, error) {
	id := ""
	if len(idarg) > 0 {
		id = idarg[0]
	}
	minID := ""
	if s.ttl > NoExpiration {
		minID = strconv.Itoa(int(now().Add(-s.ttl).UnixMilli()))
	}

	vals, err := structToMap(v)
	if err != nil {
		return "", err
	}

	id, err = s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: s.stream,
		Values: vals,
		ID:     id,
		MinID:  minID,
	}).Result()

	if err != nil {
		err = ReadError{Err: err}
	}

	return id, err
}

// Range returns a portion of the stream. Calls XRANGE.
func (s Stream[T]) Range(ctx context.Context, from, to string, count ...int64) ([]Message[T], error) {
	var redisSlice []redis.XMessage
	var err error
	if len(count) == 0 {
		redisSlice, err = s.client.XRange(ctx, s.stream, from, to).Result()
	} else {
		redisSlice, err = s.client.XRangeN(ctx, s.stream, from, to, count[0]).Result()
	}

	if err != nil {
		return nil, ReadError{Err: err}
	}

	msgs := make([]Message[T], len(redisSlice))
	for i, msg := range redisSlice {
		msgs[i] = toMessage[T](msg, s.stream)
	}
	return msgs, nil
}

// RevRange returns a portion of the stream in reverse order compared to Range. Calls XREVRANGE.
func (s Stream[T]) RevRange(ctx context.Context, from, to string, count ...int64) ([]Message[T], error) {
	var redisSlice []redis.XMessage
	var err error
	if len(count) == 0 {
		redisSlice, err = s.client.XRevRange(ctx, s.stream, from, to).Result()
	} else {
		redisSlice, err = s.client.XRevRangeN(ctx, s.stream, from, to, count[0]).Result()
	}

	if err != nil {
		return nil, ReadError{Err: err}
	}

	msgs := make([]Message[T], len(redisSlice))
	for i, msg := range redisSlice {
		msgs[i] = toMessage[T](msg, s.stream)
	}
	return msgs, nil
}

// Len returns the current stream length. Calls XLEN.
func (s Stream[T]) Len(ctx context.Context) (int64, error) {
	len, err := s.client.XLen(ctx, s.stream).Result()
	if err != nil {
		err = ReadError{Err: err}
	}
	return len, err
}
