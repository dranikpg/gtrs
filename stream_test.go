package main

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func startMiniredis(t *testing.T) (*miniredis.Miniredis, redis.Cmdable) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{
		Addr:     s.Addr(),
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	return s, rdb
}

func TestStream_Add(t *testing.T) {
	_, rdb := startMiniredis(t)
	ctx := context.TODO()

	stream := Stream[Person]{
		client: rdb,
		key:    "opa",
	}

	stream.Add(ctx, Person{Name: "First. One."})

	len, err := stream.Len(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), len)
}
