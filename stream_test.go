package gtrs

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// startMiniredis starts a new miniredis instance and returns it with a fresh client.
func startMiniredis(t *testing.T) (*miniredis.Miniredis, redis.Cmdable) {
	var s *miniredis.Miniredis
	if t != nil {
		s = miniredis.RunT(t)
	} else {
		var err error
		if s, err = miniredis.Run(); err != nil {
			panic(err)
		}
	}
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

	// Just a check for codecov :)
	assert.Equal(t, "s1", stream.Key())

	// Add first entry.
	ms.XAdd("s1", "0-1", []string{"name", "First"})

	values, err := stream.Range(ctx, "-", "+")
	assert.Nil(t, err)
	assert.Equal(t, []Message[Person]{
		{ID: "0-1", Stream: "s1", Data: Person{Name: "First"}},
	}, values)
	len, err := stream.Len(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), len)

	// Add second entry.
	ms.XAdd("s1", "0-2", []string{"name", "Second"})

	values, err = stream.Range(ctx, "-", "+")
	assert.Nil(t, err)
	assert.Equal(t, []Message[Person]{
		{ID: "0-1", Stream: "s1", Data: Person{Name: "First"}},
		{ID: "0-2", Stream: "s1", Data: Person{Name: "Second"}},
	}, values)
	len, err = stream.Len(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), len)

	// Add third entry.
	ms.XAdd("s1", "0-3", []string{"name", "Third"})

	values, err = stream.Range(ctx, "-", "+", 2)
	assert.Nil(t, err)
	assert.Equal(t, []Message[Person]{
		{ID: "0-1", Stream: "s1", Data: Person{Name: "First"}},
		{ID: "0-2", Stream: "s1", Data: Person{Name: "Second"}},
	}, values)
}

func TestStream_RangeInterval(t *testing.T) {
	ms, rdb := startMiniredis(t)
	ctx := context.TODO()

	stream := NewStream[Person](rdb, "s1")

	ms.XAdd("s1", "0-1", []string{"name", "First"})
	ms.XAdd("s1", "0-2", []string{"name", "Second"})
	ms.XAdd("s1", "0-3", []string{"name", "Third"})
	ms.XAdd("s1", "0-4", []string{"name", "Fourth"})

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
	assert.IsType(t, ReadError{}, err)
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

func TestStream_Error(t *testing.T) {
	ms, rdb := startMiniredis(t)
	ctx := context.TODO()

	ms.Close()

	stream := NewStream[Person](rdb, "s1")

	_, err := stream.Range(ctx, "-", "+")
	assert.NotNil(t, err)
	assert.IsType(t, ReadError{}, err)

	_, err = stream.Len(ctx)
	assert.NotNil(t, err)
	assert.IsType(t, ReadError{}, err)

	_, err = stream.Add(ctx, Person{})
	assert.NotNil(t, err)
	assert.IsType(t, ReadError{}, err)
}
