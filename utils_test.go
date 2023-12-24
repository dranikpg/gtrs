package gtrs

import (
	"errors"
)

// Common types for tests

type person struct {
	Name   string
	Age    int
	Height float32
}

type taggedPerson struct {
	Name   string
	Age    int
	Height float32 `gtrs:"cm"`
}

type city struct {
	Name string
	Size int
}

// No-op parsing type
type empty struct {
}

// Always returns parsing error
type nonParsable struct {
}

var errNotParsable = errors.New("not parsable")

func (nc *nonParsable) FromMap(map[string]any) error {
	return errNotParsable
}
