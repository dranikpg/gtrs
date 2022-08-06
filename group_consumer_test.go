package gtrs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// Consumer groups are not supported in miniredis yet, we'll have to mock clients

type groupCreateMock struct{}

func (gcm groupCreateMock) XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd {
	return redis.NewStatusResult("OK", nil)
}

type simpleSyncMock struct {
	*redis.Client
	groupCreateMock
	acks int
}

func (sc simpleSyncMock) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	return redis.NewXStreamSliceCmdResult([]redis.XStream{{
		Stream: "s1",
		Messages: []redis.XMessage{
			{ID: "0-1", Values: map[string]interface{}{"name": "TestTown"}},
		},
	}}, nil)
}

func (sc *simpleSyncMock) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	sc.acks += 1
	return redis.NewIntResult(1, nil)
}

func TestGroupConsumer_SimpleSync(t *testing.T) {
	rdb := simpleSyncMock{}
	cs := NewGroupConsumer[City](context.TODO(), &rdb, "g1", "c1", "s1", ">")

	var i = 0
	var readCount = 100
	for msg := range cs.Chan() {
		assert.Nil(t, msg.Err)

		cs.Ack(msg)
		cs.AwaitAcks()
		assert.Equal(t, i+1, rdb.acks)

		if i += 1; i >= readCount {
			break
		}
	}

	assert.Equal(t, readCount, rdb.acks)
}

type switchToNewMock struct {
	*redis.Client
	groupCreateMock
	maxHandout int
}

func (sc switchToNewMock) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	if a.Streams[1] == ">" {
		return redis.NewXStreamSliceCmdResult([]redis.XStream{{
			Stream: "s1",
			Messages: []redis.XMessage{
				{ID: "1-1", Values: map[string]interface{}{"name": "NewTown"}},
			},
		}}, nil)
	} else if a.Streams[1] == fmt.Sprintf("0-%v", sc.maxHandout) {
		return redis.NewXStreamSliceCmdResult([]redis.XStream{{
			Stream:   "s1",
			Messages: []redis.XMessage{},
		}}, nil)
	} else {
		id, _ := strconv.ParseInt(strings.Split(a.Streams[1], "-")[1], 10, 64)
		return redis.NewXStreamSliceCmdResult([]redis.XStream{{
			Stream: "s1",
			Messages: []redis.XMessage{
				{ID: fmt.Sprintf("0-%v", id+1), Values: map[string]interface{}{"name": "OldTown"}},
			},
		}}, nil)
	}
}

func (sc *switchToNewMock) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	return redis.NewIntResult(1, nil)
}

func TestGroupConsumer_SwitchToNew(t *testing.T) {
	var readCount = 10
	var maxHistory = 5
	rdb := switchToNewMock{maxHandout: maxHistory}
	cs := NewGroupConsumer[City](context.TODO(), &rdb, "g1", "c1", "s1", "0-0")

	var i = 0
	for msg := range cs.Chan() {
		assert.Nil(t, msg.Err)
		if i < maxHistory {
			assert.Equal(t, "OldTown", msg.Data.Name)
		} else {
			assert.Equal(t, "NewTown", msg.Data.Name)
		}

		if i += 1; i >= readCount {
			break
		}
	}
}

type remainingAckMock struct {
	*redis.Client
	groupCreateMock
}

func (sc *remainingAckMock) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	if ids[0] == "0-0" || ids[0] == "0-1" {
		return redis.NewIntResult(0, errors.New("must fail"))
	} else {
		select {
		case <-ctx.Done():
		case <-time.After(1000 * time.Second):
		}
		return redis.NewIntResult(1, errors.New("must cancel"))
	}
}

func (sc *remainingAckMock) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	return redis.NewXStreamSliceCmdResult([]redis.XStream{{
		Stream: "s1",
		Messages: []redis.XMessage{
			{ID: "0-1", Values: map[string]interface{}{"name": "TestTown"}},
		},
	}}, nil)
}

func TestGroupConsumer_RemainingAck(t *testing.T) {
	var ackCount = 100

	rdb := remainingAckMock{}
	cs := NewGroupConsumer[City](context.TODO(), &rdb, "g1", "c1", "s1", "0-0", GroupConsumerConfig{
		AckBufferSize: 101,
	})

	for i := 0; i < ackCount; i += 1 {
		assert.True(t, cs.Ack(Message[City]{ID: fmt.Sprintf("0-%v", i)}))
	}

	time.Sleep(50 * time.Millisecond)

	rm := cs.CloseGetRemainingAcks()

	// Neither ack request finished, and neither error was processed - expect as many as sent
	assert.Len(t, rm, ackCount)
}
