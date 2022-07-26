package main

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/go-redis/redis/v8"
)

type Convertible interface {
	ToMap() map[string]any
	FromMap(map[string]any) error
}

// Convert a redis.XMessage to a Message[T]
func toMessage[T any](rm redis.XMessage) Message[T] {
	var data T
	var err error

	if err = mapToStruct(&data, rm.Values); err != nil {
		err = ParsingError{
			ID:    rm.ID,
			Data:  rm.Values,
			Inner: err,
		}
	}

	return Message[T]{
		ID:    rm.ID,
		Data:  data,
		Error: err,
	}
}

func structToMap(st any) map[string]any {
	if c, ok := st.(Convertible); ok {
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

func mapToStruct(st any, data map[string]any) error {
	rv := reflect.ValueOf(st).Elem()
	rt := reflect.TypeOf(st).Elem()

	if rt.Implements(typeOf[Convertible]()) {
		if c, ok := st.(Convertible); ok {
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
			return fmt.Errorf("failed to parse %v as %v", k, field.Type().Kind())
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
			return int(0)
		}
		return i
	case reflect.Float32:
		i, err := strconv.ParseFloat(st, 32)
		if err != nil {
			return float32(0)
		}
		return float32(i)
	}
	return nil
}
