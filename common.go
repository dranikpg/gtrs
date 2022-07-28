package main

import "fmt"

// A generic message with an ID, stream, data and an optional error.
type Message[T any] struct {
	ID     string
	Stream string
	Error  error
	Data   T
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
	ID    string
	Data  map[string]interface{}
	Inner error
}

func (pe ParsingError) Unwrap() error {
	return pe.Inner
}

func (pe ParsingError) Error() string {
	return fmt.Sprintf("parsing error: id %v, cause: %v", pe.ID, pe.Inner)
}
