// GTRS (Go Typed Redis Streams) is a library for easily reading Redis streams with or without consumer groups.
// See https://github.com/dranikpg/gtrs for a quick tutorial.
package gtrs

import (
	"fmt"

	"github.com/redis/go-redis/v9"
)

// A generic message with an ID, stream, data and an optional error.
type Message[T any] struct {
	ID     string
	Stream string
	Err    error
	Data   T
}

// fetchMessage is used for sending a redis.XMessage together with its corresponding stream.
// Used in consumer fetchloops.
type fetchMessage struct {
	stream  string
	message redis.XMessage
}

// innerAckError is sent by ackLoop and carries the id of the failed ack request and its cause.
type innerAckError struct {
	id    string
	cause error
}

// ReadError indicates an erorr with the redis client.
//
// After a ReadError was returned from a consumer, it'll close its main channel.
type ReadError struct {
	Err error
}

func (ce ReadError) Unwrap() error {
	return ce.Err
}

func (ce ReadError) Error() string {
	return fmt.Sprintf("read error: %v", ce.Err)
}

// AckError indicates that an acknowledgement request failed.
type AckError struct {
	Err error
}

func (ae AckError) Unwrap() error {
	return ae.Err
}

func (ae AckError) Error() string {
	return fmt.Sprintf("acknowledge error: cause: %v", ae.Err)
}

// ParseError indicates an error during parsing.
type ParseError struct {
	Data map[string]interface{} // raw data returned by the redis client
	Err  error
}

func (pe ParseError) Unwrap() error {
	return pe.Err
}

func (pe ParseError) Error() string {
	return fmt.Sprintf("parsing error: cause: %v", pe.Err)
}
