package gtrs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Metadata is a type that allows serialization of generic structured
// metadata within the stream entries. Any value that can be serialized
// to JSON can be inserted here.
type Metadata map[string]any

// ConvertibleTo is implemented by types that can convert themselves to a map.
type ConvertibleTo interface {
	ToMap() (map[string]any, error)
}

// ConvertibleFrom is implemented by types that can load themselves from a map.
type ConvertibleFrom interface {
	FromMap(map[string]any) error
}

func ackErrToMessage[T any](err innerAckError, stream string) Message[T] {
	return Message[T]{
		ID: err.id, Stream: stream,
		Err: AckError{Err: err.cause},
	}
}

// Convert a redis.XMessage to a Message[T]
func toMessage[T any](rm redis.XMessage, stream string) Message[T] {
	var data T
	var err error

	if err = mapToStruct(&data, rm.Values); err != nil {
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

// structToMap convert a struct to a map.
func structToMap(st any) (map[string]any, error) {
	if c, ok := st.(ConvertibleTo); ok {
		return c.ToMap()
	}

	rv := reflect.ValueOf(st)
	rt := reflect.TypeOf(st)
	out := make(map[string]interface{}, rv.NumField())

	for i := 0; i < rv.NumField(); i++ {
		fieldValue := rv.Field(i)
		fieldType := rt.Field(i)
		switch v := fieldValue.Interface().(type) {
		case time.Time:
			out[toSnakeCase(fieldType.Name)] = v.Format(time.RFC3339Nano)
		case time.Duration:
			out[toSnakeCase(fieldType.Name)] = v.String()
		case Metadata:
			js, err := json.Marshal(v)
			if err != nil {
				return nil, SerializeError{
					Field: fieldType.Name,
					Value: fieldValue.Interface(),
					Err:   err,
				}
			}
			out[toSnakeCase(fieldType.Name)] = string(js)
		default:
			out[toSnakeCase(fieldType.Name)] = fieldValue.Interface()
		}
	}
	return out, nil
}

// mapToStruct tries to convert a map to a struct.
func mapToStruct(st any, data map[string]any) error {
	rv := reflect.ValueOf(st).Elem()
	rt := reflect.TypeOf(st)

	if rt.Implements(typeOf[ConvertibleFrom]()) {
		if c, ok := st.(ConvertibleFrom); ok {
			return c.FromMap(data)
		}
	}

	rt = rt.Elem()

	for i := 0; i < rt.NumField(); i += 1 {
		fieldRv := rv.Field(i)
		fieldRt := rt.Field(i)

		v, ok := data[toSnakeCase(fieldRt.Name)]
		if !ok {
			continue
		}

		// The redis client always sends strings.
		stval, ok := v.(string)
		if !ok {
			continue
		}

		val, err := valueFromString(fieldRv, stval)
		if err != nil {
			return FieldParseError{Field: fieldRt.Name, Value: v, Err: err}
		} else {
			fieldRv.Set(reflect.ValueOf(val))
		}
	}
	return nil
}

// Parse value from string
// TODO: find a better solution. Maybe there is a library for this.
func valueFromString(val reflect.Value, st string) (any, error) {
	switch val.Interface().(type) {
	case string:
		return st, nil
	case bool:
		return strconv.ParseBool(st)
	case int:
		v, err := strconv.ParseInt(st, 10, 0)
		return int(v), err
	case uint:
		v, err := strconv.ParseUint(st, 10, 0)
		return uint(v), err
	case int32:
		v, err := strconv.ParseInt(st, 10, 32)
		return int32(v), err
	case uint32:
		v, err := strconv.ParseUint(st, 10, 32)
		return uint32(v), err
	case int64:
		return strconv.ParseInt(st, 10, 64)
	case uint64:
		return strconv.ParseUint(st, 10, 64)
	case float32:
		v, err := strconv.ParseFloat(st, 32)
		return float32(v), err
	case float64:
		return strconv.ParseFloat(st, 64)
	case time.Time:
		return time.Parse(time.RFC3339Nano, st)
	case time.Duration:
		return time.ParseDuration(st)
	case Metadata:
		m := Metadata{}
		err := json.Unmarshal([]byte(st), &m)
		return m, err
	}
	return nil, errors.New("unsupported field type")
}

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func toSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

// Get reflect type by generic type.
// see https://github.com/golang/go/issues/50741 for a better solution in the future
func typeOf[T any]() reflect.Type {
	return reflect.TypeOf((*T)(nil)).Elem()
}

func copyMap[K comparable, V any](in map[K]V) map[K]V {
	out := make(map[K]V, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
