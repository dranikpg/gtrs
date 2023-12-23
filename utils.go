package gtrs

import (
	"context"

	"github.com/dranikpg/gtrs/gtrsconvert"
	"github.com/redis/go-redis/v9"
)

// Metadata is a type that allows serialization of generic structured
// metadata within the stream entries. Any value that can be serialized
// to JSON can be inserted here.
type Metadata = gtrsconvert.Metadata

// AsciiTime is a type that wraps time.Time, however the (Un)[M/m]arshalbinary functions are overridden to marshal in the same format as Text
type AsciiTime = gtrsconvert.AsciiTime

// ConvertibleTo is implemented by types that can convert themselves to a map.
type ConvertibleTo = gtrsconvert.ConvertibleTo

// ConvertibleFrom is implemented by types that can load themselves from a map.
type ConvertibleFrom = gtrsconvert.ConvertibleFrom

// FieldParseError is returned with a field fails to be parsed
type FieldParseError = gtrsconvert.FieldParseError

// SerializeError is returned with a field fails to be serialized
type SerializeError = gtrsconvert.SerializeError

func copyMap[K comparable, V any](in map[K]V) map[K]V {
	out := make(map[K]V, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func ackErrToMessage[T any](err innerAckError) Message[T] {
	return Message[T]{
		ID: err.ID, Stream: err.Stream,
		Err: AckError{Err: err.cause},
	}
}

// Convert a redis.XMessage to a Message[T]
func toMessage[T any](rm redis.XMessage, stream string) Message[T] {
	var data T
	var err error

	if err = gtrsconvert.MapToStruct(&data, rm.Values); err != nil {
		err = ParseError{
			Data: rm.Values,
			Err:  err,
		}
	}

	return Message[T]{
		ID:     rm.ID,
		Stream: stream,
		Err:    err,
		Data:   data,
	}
}

// sendCheckCancel sends a generic message without blocking cancellation.
// returns false if message was not delivered.
func sendCheckCancel[M any](ctx context.Context, ch chan M, m M) bool {
	// We really NEVER want to sent after we're closed
	select {
	case <-ctx.Done():
		return false
	default:
		select {
		case <-ctx.Done():
			return false
		case ch <- m:
			return true
		}
	}
}

func checkCancel(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
