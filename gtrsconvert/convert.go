package gtrsconvert

import (
	"encoding"
	"reflect"
	"strconv"
	"time"
)

// ConvertibleTo is implemented by types that can convert themselves to a map.
type ConvertibleTo interface {
	ToMap() (map[string]any, error)
}

// ConvertibleFrom is implemented by types that can load themselves from a map.
type ConvertibleFrom interface {
	FromMap(map[string]any) error
}

// structToMap convert a struct to a map.
func StructToMap(st any) (map[string]any, error) {
	if c, ok := st.(ConvertibleTo); ok {
		return c.ToMap()
	}

	rv := reflect.ValueOf(st)
	rt := reflect.TypeOf(st)
	out := make(map[string]interface{}, rv.NumField())

	for i := 0; i < rv.NumField(); i++ {
		fieldValue := rv.Field(i)
		fieldType := rt.Field(i)
		fieldName := getFieldNameFromType(fieldType)
		switch v := fieldValue.Interface().(type) {
		case time.Duration:
			out[fieldName] = v.String()
		case encoding.BinaryMarshaler:
			txt, err := v.MarshalBinary()
			if err != nil {
				return nil, SerializeError{
					Field: fieldType.Name,
					Value: fieldValue.Interface(),
					Err:   err,
				}
			}
			out[fieldName] = string(txt)
		default:
			out[fieldName] = fieldValue.Interface()
		}
	}
	return out, nil
}

// mapToStruct tries to convert a map to a struct.
func MapToStruct(st any, data map[string]any) error {
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

		v, ok := data[getFieldNameFromType(fieldRt)]
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
	iface := val.Interface()

	switch cast := iface.(type) {
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
	case time.Duration:
		return time.ParseDuration(st)
	case encoding.BinaryUnmarshaler:
		return cast, cast.UnmarshalBinary([]byte(st))
	default:
		ifaceptr := val.Addr().Interface()
		unMarshaler, ok := ifaceptr.(encoding.BinaryUnmarshaler)
		if ok {
			err := unMarshaler.UnmarshalBinary([]byte(st))
			if err != nil {
				return nil, err
			}
			res := reflect.ValueOf(unMarshaler).Elem().Interface()
			return res, nil
		}
	}

	return nil, ErrUnsupportedFieldType
}
