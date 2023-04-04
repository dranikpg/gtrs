package gtrs

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestConsumer_SimpleSync(t *testing.T) {
	ms, rdb := startMiniredis(t)
	cs := NewConsumer[City](context.TODO(), rdb, StreamIDs{"s1": "0-0"}, StreamConsumerConfig{
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
			ms.XAdd("s1", "*", []string{"name", city.Name, "size", fmt.Sprint(city.Size)})

			for atomic.LoadInt32(&confim) == 0 {
				continue // wait for confirm
			}
		}
	}()

	i := 0
	for msg := range cs.Chan() {
		assert.Nil(t, msg.Err)
		assert.Equal(t, cities[i], msg.Data)
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
	cs := NewConsumer[City](context.TODO(), rdb, StreamIDs{"s1": "0-0"}, StreamConsumerConfig{
		Block:      0,
		Count:      0,
		BufferSize: 0,
	})

	ms.Close()

	msg := <-cs.Chan()
	assert.NotNil(t, msg.Err)
	assert.IsType(t, ReadError{}, msg.Err)
}

func TestConsumer_ParserError(t *testing.T) {
	ms, rdb := startMiniredis(t)
	cs := NewConsumer[NonParsable](context.TODO(), rdb, StreamIDs{"s1": "0-0"}, StreamConsumerConfig{
		Block:      0,
		Count:      0,
		BufferSize: 0,
	})

	ms.XAdd("s1", "*", []string{"Test", "Yes"})

	msg := <-cs.Chan()
	assert.NotNil(t, msg.Err)
	assert.IsType(t, ParseError{}, msg.Err)
	assert.Equal(t, errNotParsable, errors.Unwrap(msg.Err))
	assert.ErrorIs(t, msg.Err, errNotParsable)
}

func TestConsuer_FieldParseError(t *testing.T) {
	type FPE struct {
		Name float64
	}

	ms, rdb := startMiniredis(t)
	cs := NewConsumer[FPE](context.TODO(), rdb, StreamIDs{"s1": "0-0"}, StreamConsumerConfig{
		Block:      0,
		Count:      0,
		BufferSize: 0,
	})

	ms.XAdd("s1", "*", []string{"name", "FAILFAIL"})

	msg := <-cs.Chan()
	assert.NotNil(t, msg.Err)

	var fpe FieldParseError
	assert.ErrorAs(t, msg.Err, &fpe)
	assert.Equal(t, "Name", fpe.Field)
	assert.Equal(t, "FAILFAIL", fpe.Value)
	assert.NotNil(t, fpe.Err)
}

func TestConsumer_Close(t *testing.T) {
	_, rdb := startMiniredis(t)
	cs := NewConsumer[NonParsable](context.TODO(), rdb, StreamIDs{"s1": "0-0"}, StreamConsumerConfig{
		Block:      0,
		Count:      0,
		BufferSize: 0,
	})

	cs.Close()

	_, ok := <-cs.Chan()
	assert.False(t, ok)
}

func TestConsumer_CloseGetSeenIDs(t *testing.T) {
	var readCount = 100
	var consumeCount = 75

	ms, rdb := startMiniredis(t)
	cs := NewConsumer[City](context.TODO(), rdb, StreamIDs{"s1": "0-0"}, StreamConsumerConfig{
		Block:      0,
		Count:      0,
		BufferSize: 0,
	})

	for i := 1; i <= readCount; i++ {
		ms.XAdd("s1", fmt.Sprintf("0-%v", i), []string{"name", "Town"})
	}

	for i := 1; i <= consumeCount; i++ {
		<-cs.Chan()
	}

	seen := cs.Close()
	assert.Equal(t, fmt.Sprintf("0-%v", consumeCount), seen["s1"])
}

func TestConsumer_CancelContext(t *testing.T) {
	var readCount = 100
	var consumeCount = 75

	ms, rdb := startMiniredis(t)
	ctx, cancelFunc := context.WithCancel(context.TODO())
	defer cancelFunc()
	cs := NewConsumer[City](ctx, rdb, StreamIDs{"s1": "0-0"}, StreamConsumerConfig{
		Block:      0,
		Count:      0,
		BufferSize: 0,
	})

	for i := 1; i <= readCount; i++ {
		ms.XAdd("s1", fmt.Sprintf("0-%v", i), []string{"name", "Town"})
	}

	stopid := fmt.Sprintf("0-%v", consumeCount)
	for msg := range cs.Chan() {
		assert.Nil(t, msg.Err)
		if msg.ID == stopid {
			cancelFunc()
		}
	}

	seen := cs.Close()
	assert.Equal(t, fmt.Sprintf("0-%v", consumeCount), seen["s1"])
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
//
// The current implementation takes about 400-600 ns for a single loop iteration. That is not optimal,
// but gives nonetheless an acceptable thoughput of 2 M/s, which is likely to be faster than
// any ordinary users needs.
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
			cs := NewConsumer[Empty](context.TODO(), mock, StreamIDs{"s1": "0-0"}, StreamConsumerConfig{
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
