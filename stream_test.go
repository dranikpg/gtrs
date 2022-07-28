package main

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// startMiniredis starts a new miniredis instance and returns it with a fresh client.
func startMiniredis(t *testing.T) (*miniredis.Miniredis, redis.Cmdable) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{
		Addr:     s.Addr(),
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	return s, rdb
}

func TestStream_RangeLenSimple(t *testing.T) {
	ms, rdb := startMiniredis(t)
	ctx := context.TODO()

	stream := NewStream[Person](rdb, "s1")

	// Add first entry.
	ms.XAdd("s1", "0-1", []string{"Name", "First"})

	values, err := stream.Range(ctx, "-", "+")
	assert.Nil(t, err)
	assert.Equal(t, []Message[Person]{
		{ID: "0-1", Stream: "s1", Data: Person{Name: "First"}},
	}, values)
	len, err := stream.Len(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), len)

	// Add second entry.
	ms.XAdd("s1", "0-2", []string{"Name", "Second"})

	values, err = stream.Range(ctx, "-", "+")
	assert.Nil(t, err)
	assert.Equal(t, []Message[Person]{
		{ID: "0-1", Stream: "s1", Data: Person{Name: "First"}},
		{ID: "0-2", Stream: "s1", Data: Person{Name: "Second"}},
	}, values)
	len, err = stream.Len(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), len)
}

func TestStream_RangeInterval(t *testing.T) {
	ms, rdb := startMiniredis(t)
	ctx := context.TODO()

	stream := NewStream[Person](rdb, "s1")

	ms.XAdd("s1", "0-1", []string{"Name", "First"})
	ms.XAdd("s1", "0-2", []string{"Name", "Second"})
	ms.XAdd("s1", "0-3", []string{"Name", "Third"})
	ms.XAdd("s1", "0-4", []string{"Name", "Fourth"})

	vals, err := stream.Range(ctx, "0-3", "+")
	assert.Nil(t, err)
	assert.Equal(t, []Message[Person]{
		{ID: "0-3", Stream: "s1", Data: Person{Name: "Third"}},
		{ID: "0-4", Stream: "s1", Data: Person{Name: "Fourth"}},
	}, vals)

	vals, err = stream.Range(ctx, "0-1", "0-2")
	assert.Nil(t, err)
	assert.Equal(t, []Message[Person]{
		{ID: "0-1", Stream: "s1", Data: Person{Name: "First"}},
		{ID: "0-2", Stream: "s1", Data: Person{Name: "Second"}},
	}, vals)

	vals, err = stream.Range(ctx, "-", "0-1")
	assert.Nil(t, err)
	assert.Equal(t, []Message[Person]{
		{ID: "0-1", Stream: "s1", Data: Person{Name: "First"}},
	}, vals)

	_, err = stream.Range(ctx, "??", "??")
	assert.NotNil(t, err)
	assert.IsType(t, ClientError{}, err)
}

func TestStream_Add(t *testing.T) {
	_, rdb := startMiniredis(t)
	ctx := context.TODO()

	stream := NewStream[Person](rdb, "s1")

	// Add first entry.
	stream.Add(ctx, Person{Name: "First"}, "0-1")

	len, err := stream.Len(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), len)

	vals, err := stream.Range(ctx, "-", "+")
	assert.Nil(t, err)
	assert.Equal(t, []Message[Person]{
		{ID: "0-1", Stream: "s1", Data: Person{Name: "First"}},
	}, vals)

	// Add second entry.
	stream.Add(ctx, Person{Name: "Second"})

	len, err = stream.Len(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), len)
}
