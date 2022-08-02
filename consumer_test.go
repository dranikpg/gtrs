package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/go-redis/redis/v8"
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
	assert.IsType(t, ReadError{}, msg.Err)
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

func TestConsumer_Close(t *testing.T) {
	_, rdb := startMiniredis(t)
	cs := NewConsumer[NonParsable](context.TODO(), rdb, StreamIds{"s1": "0-0"}, SimpleConsumerConfig{
		Block:      0,
		Count:      0,
		BufferSize: 0,
	})

	cs.Close()

	_, ok := <-cs.Chan()
	assert.False(t, ok)
}

type benchmarkClientMock struct {
	*redis.Client
	msgs []redis.XMessage
}

func (bmc benchmarkClientMock) XRead(ctx context.Context, a *redis.XReadArgs) *redis.XStreamSliceCmd {
	return redis.NewXStreamSliceCmdResult([]redis.XStream{
		{Stream: "s1", Messages: bmc.msgs},
	}, nil)
}

// BenchmarkConsumer measures the throughput of the whole pipeline.
//
// An unbuffered channel read takes about 100-150 ns, a buffered channel read takes up to 100ns.
// A goroutine wakeup takes less than 100ns.
// So the theoretical target should be about 1000 ns (1 ms), giving throughput of 1 M/s entries.
// However currently results are in range 1200-1800 ns
func BenchmarkConsumer(b *testing.B) {
	b.SetParallelism(1)

	msgbuf := make([]redis.XMessage, 5000)
	for i := 0; i < len(msgbuf); i++ {
		msgbuf[i] = redis.XMessage{
			ID:     fmt.Sprint(i),
			Values: map[string]any{"A": "B"},
		}
	}

	for _, size := range []uint{0, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("s-%v", size), func(b *testing.B) {
			mock := benchmarkClientMock{msgs: msgbuf}
			cs := NewConsumer[Empty](context.TODO(), mock, StreamIds{"s1": "0-0"}, SimpleConsumerConfig{
				Block:      0,
				Count:      int64(len(msgbuf)),
				BufferSize: size,
			})

			b.StartTimer()
			var read int
			for range cs.Chan() {
				if read += 1; read == b.N {
					break
				}
			}
			b.StopTimer()
		})
	}
}
