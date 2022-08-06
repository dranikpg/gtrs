package gtrs

import (
	"context"
	"fmt"
	"reflect"
	"strconv"

	"github.com/go-redis/redis/v8"
)

// ConvertibleTo is implemented by types that can convert themselves to a map.
type ConvertibleTo interface {
	ToMap() map[string]any
}

// ConvertibleFrom is implemented by types that can load themselves from a map.
type ConvertibleFrom interface {
	FromMap(map[string]any) error
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
	select {
	case <-ctx.Done():
		return false
	case ch <- m:
		return true
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

type FieldParseError struct {
	Field string
	Value any
}

func (fpe FieldParseError) Error() string {
	return fmt.Sprintf("failed to parse field %v, got %v", fpe.Field, fpe.Value)
}

// structToMap convert a struct to a map.
func structToMap(st any) map[string]any {
	if c, ok := st.(ConvertibleTo); ok {
		return c.ToMap()
	}

	rv := reflect.ValueOf(st)
	rt := reflect.TypeOf(st)
	out := make(map[string]interface{}, rv.NumField())

	for i := 0; i < rv.NumField(); i++ {
		fieldValue := rv.Field(i)
		fieldType := rt.Field(i)
		out[fieldType.Name] = fieldValue.Interface()
	}
	return out
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

	for k, v := range data {
		field := rv.FieldByName(k)
		if !field.IsValid() {
			continue
		}
		stval, ok := v.(string)
		if !ok {
			continue
		}

		if val := valueFromString(field.Type().Kind(), stval); val != nil {
			field.Set(reflect.ValueOf(val))
		} else {
			return FieldParseError{Field: k, Value: v}
		}
	}

	return nil
}

// Parse value from string
// TODO: find a better solution. Maybe there is a library for this.
func valueFromString(kd reflect.Kind, st string) any {
	switch kd {
	case reflect.String:
		return st
	case reflect.Int:
		i, err := strconv.Atoi(st)
		if err != nil {
			return nil
		}
		return i
	case reflect.Float32:
		i, err := strconv.ParseFloat(st, 32)
		if err != nil {
			return nil
		}
		return float32(i)
	}
	return nil
}

// Get reflect type by generic type.
// see https://github.com/golang/go/issues/50741
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
