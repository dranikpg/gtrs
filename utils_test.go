package gtrs

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Person struct {
	Name   string
	Age    int
	Height float32
}

type City struct {
	Name string
	Size int
}

type Empty struct {
}

func (e *Empty) FromMap(map[string]any) error {
	return nil
}

type NonParsable struct {
}

var errNotParsable = errors.New("not parsable")

func (nc *NonParsable) FromMap(map[string]any) error {
	return errNotParsable
}

func TestUtils_convertStructToMap(t *testing.T) {
	p1 := Person{Name: "Vlad", Age: 19, Height: 172.0}
	m1 := structToMap(p1)

	assert.Equal(t, map[string]any{
		"name":   "Vlad",
		"age":    int(19),
		"height": float32(172),
	}, m1)
}

func TestUtils_convertMapToStruct(t *testing.T) {
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
