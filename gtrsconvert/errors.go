package gtrsconvert

import (
	"errors"
	"fmt"
)

var ErrUnsupportedFieldType = errors.New("unsupported field type")

// FieldParseError is returned by the default parser
// if data for a field is present:
// - but its not assignable
// - or the field is of an unsupported type
type FieldParseError struct {
	Field string
	Value any
	Err   error
}

func (fpe FieldParseError) Error() string {
	return fmt.Sprintf("failed to parse field %v, got %v, because %v", fpe.Field, fpe.Value, fpe.Err)
}

func (fpe FieldParseError) Unwrap() error {
	return fpe.Err
}

type SerializeError struct {
	Field string
	Value any
	Err   error
}

func (se SerializeError) Error() string {
	return fmt.Sprintf("failed to serialize field %v with val %v, because %v", se.Field, se.Value, se.Err)
}

func (se SerializeError) Unwrap() error {
	return se.Err
}
