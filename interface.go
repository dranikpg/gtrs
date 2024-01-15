package gtrs

import "context"

// Stream represents a redis stream with messages of type T.
type Stream[T any] interface {
	Add(ctx context.Context, v T, idarg ...string) (string, error)
	Key() string
	Len(ctx context.Context) (int64, error)
	Range(ctx context.Context, from, to string, count ...int64) ([]Message[T], error)
	RevRange(ctx context.Context, from, to string, count ...int64) ([]Message[T], error)
}

// Consumer is a generic consumer interface
type Consumer[T any] interface {
	Chan() <-chan Message[T]
	Close() any
}
