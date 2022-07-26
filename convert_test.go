package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type Person struct {
	Name   string
	Age    int
	Height float32
}

func TestConvert_StructToMap(t *testing.T) {
	p1 := Person{Name: "Vlad", Age: 19, Height: 172.0}
	m1 := structToMap(p1)

	assert.Equal(t, map[string]any{
		"Name":   "Vlad",
		"Age":    int(19),
		"Height": float32(172),
	}, m1)
}

func TestConvert_MapToStruct(t *testing.T) {
	m1 := map[string]any{
		"Name":   "Vlad",
		"Age":    "19",
		"Height": "172",
	}

	var p1 Person
	err := mapToStruct(&p1, m1)

	assert.Nil(t, err)
	assert.Equal(t, Person{Name: "Vlad", Age: 19, Height: 172.0}, p1)
}
