package gtrs

import (
	"fmt"

	"github.com/go-redis/redis/v8"
)

// A generic message with an ID, stream, data and an optional error.
type Message[T any] struct {
	ID     string
	Stream string
	Err    error
	Data   T
}

// IsLast returns true if the consumer closed after this message.
func (msg Message[T]) IsLast() bool {
	_, ok := msg.Err.(ReadError)
	return ok
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

// ParsingError indicates an error during parsing.
type ParsingError struct {
	Data map[string]interface{}
	Err  error
}

func (pe ParsingError) Unwrap() error {
	return pe.Err
}

func (pe ParsingError) Error() string {
	return fmt.Sprintf("parsing error: cause: %v", pe.Err)
}
