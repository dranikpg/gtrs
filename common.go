package main

import (
	"fmt"
)

// A generic message with an ID, stream, data and an optional error.
type Message[T any] struct {
	ID     string
	Stream string
	Err    error
	Data   T
}

// IsLast returns true if the consumer closed after this message
func (msg Message[T]) IsLast() bool {
	_, ok := msg.Err.(ClientError)
	return ok
}

// ClientError indicates an erorr with the redis client
type ClientError struct {
	clientError error
}

func (ce ClientError) Unwrap() error {
	return ce.clientError
}

func (ce ClientError) Error() string {
	return fmt.Sprintf("client error: %v", ce.clientError)
}

// ParsingError indicates an error during parsing
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
