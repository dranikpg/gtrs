package gtrs

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Common types for tests

type Person struct {
	Name   string
	Age    int
	Height float32
}

type City struct {
	Name string
	Size int
}

// No-op parsing type
type Empty struct {
}

// Always returns parsing error
type NonParsable struct {
}

var errNotParsable = errors.New("not parsable")

func (nc *NonParsable) FromMap(map[string]any) error {
	return errNotParsable
}

func TestUtils_convertStructToMap_Simple(t *testing.T) {
	p1 := Person{Name: "Vlad", Age: 19, Height: 172.0}
	m1 := structToMap(p1)

	assert.Equal(t, map[string]any{
		"name":   "Vlad",
		"age":    int(19),
		"height": float32(172),
	}, m1)
}

func TestUtils_convertMapToStruct_Simple(t *testing.T) {
	m1 := map[string]any{
		"name":   "Vlad",
		"age":    "19",
		"height": "172",
	}

	var p1 Person
	err := mapToStruct(&p1, m1)

	assert.Nil(t, err)
	assert.Equal(t, Person{Name: "Vlad", Age: 19, Height: 172.0}, p1)
}

func TestUtils_convertMapToStruct_AllTypes(t *testing.T) {
	type AllTypes struct {
		S   string
		I   int
		U   uint
		B   bool
		I32 int32
		U32 uint32
		I64 int64
		U64 uint64
		F32 float32
		F64 float64
		N   int
	}

	m1 := map[string]any{
		"s":   "s",
		"i":   "1",
		"u":   "1",
		"b":   "true",
		"i32": "1",
		"u32": "1",
		"i64": "1",
		"u64": "1",
		"f32": "1",
		"f64": "1",
		"n":   123, // non string value
	}

	var s1 AllTypes
	err := mapToStruct(&s1, m1)
	assert.Nil(t, err)
	assert.Equal(t, AllTypes{
		S:   "s",
		I:   1,
		U:   1,
		B:   true,
		I32: 1,
		U32: 1,
		I64: 1,
		U64: 1,
		F32: 1.0,
		F64: 1.0,
	}, s1)
}

func TestUtils_convertMapToStruct_UnsupportedType(t *testing.T) {
	type Unsupported struct {
		V any
	}
	var u Unsupported
	err := mapToStruct(&u, map[string]any{
		"v": "123",
	})
	assert.NotNil(t, err)
	assert.NotNil(t, errors.Unwrap(err))

	var fpe FieldParseError
	assert.ErrorAs(t, err, &fpe)
	assert.Contains(t, err.Error(), "failed to parse field V")
}

func TestUtils_convertMapToStruct_SnakeCase(t *testing.T) {
	type LongNames struct {
		FirstName         int
		OneMoreSecondName int
	}
	var v LongNames
	err := mapToStruct(&v, map[string]any{
		"first_name":           "1",
		"one_more_second_name": "2",
	})
	assert.Nil(t, err)
	assert.Equal(t, LongNames{
		FirstName:         1,
		OneMoreSecondName: 2,
	}, v)
}
