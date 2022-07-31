package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConsumer_SimpleSync(t *testing.T) {
	ms, rdb := startMiniredis(t)
	cs := NewConsumer[City](context.TODO(), rdb, StreamIds{"s1": "0-0"}, SimpleConsumerConfig{
		Block:      0,
		Count:      0,
		BufferSize: 0,
	})
	var sent int32 = 0   // number of items sent
	var confim int32 = 0 // confirm status of receiver

	cities := make([]City, 100)
	for i := 0; i < len(cities); i++ {
		cities[i] = City{Name: fmt.Sprintf("City %v", i), Size: i}
	}

	go func() {
		// Send cities one by one and wait for confirm.
		for _, city := range cities {
			atomic.StoreInt32(&confim, 0) // clear confirm flag

			atomic.AddInt32(&sent, 1)
			ms.XAdd("s1", "*", []string{"Name", city.Name, "Size", fmt.Sprint(city.Size)})

			for atomic.LoadInt32(&confim) == 0 {
				continue // wait for confirm
			}
		}
	}()

	i := 0
	for msg := range cs.Chan() {
		assert.Nil(t, msg.Err)
		assert.False(t, msg.IsLast())
		assert.Equal(t, msg.Data, cities[i])
		assert.Equal(t, int32(i+1), atomic.LoadInt32(&sent))

		atomic.StoreInt32(&confim, 1)
		i += 1
		if i == len(cities) {
			break
		}
	}
}

func TestConsumer_ClientError(t *testing.T) {
	ms, rdb := startMiniredis(t)
	cs := NewConsumer[City](context.TODO(), rdb, StreamIds{"s1": "0-0"}, SimpleConsumerConfig{
		Block:      0,
		Count:      0,
		BufferSize: 0,
	})

	ms.Close()

	msg := <-cs.Chan()
	assert.NotNil(t, msg.Err)
	assert.True(t, msg.IsLast())
	assert.IsType(t, ClientError{}, msg.Err)
}

func TestConsumer_ParserError(t *testing.T) {
	ms, rdb := startMiniredis(t)
	cs := NewConsumer[NonParsable](context.TODO(), rdb, StreamIds{"s1": "0-0"}, SimpleConsumerConfig{
		Block:      0,
		Count:      0,
		BufferSize: 0,
	})

	ms.XAdd("s1", "*", []string{"Test", "Yes"})

	msg := <-cs.Chan()
	assert.NotNil(t, msg.Err)
	assert.False(t, msg.IsLast())
	assert.IsType(t, ParsingError{}, msg.Err)
	assert.Equal(t, errNotParsable, errors.Unwrap(msg.Err))
}
