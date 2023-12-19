package gtrs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// Consumer groups are not supported in miniredis yet, we'll have to mock clients

type groupCreateMock struct{}

func (gcm groupCreateMock) XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd {
	return redis.NewStatusResult("OK", nil)
}

// TestGroupConsumer_SimpleSync

type simpleSyncMock struct {
	*redis.Client
	groupCreateMock
	acks int64
}

func (sc *simpleSyncMock) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	return redis.NewXStreamSliceCmdResult([]redis.XStream{{
		Stream: "s1",
		Messages: []redis.XMessage{
			{ID: "0-1", Values: map[string]interface{}{"name": "TestTown"}},
		},
	}}, nil)
}

func (sc *simpleSyncMock) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	atomic.AddInt64(&sc.acks, 1)
	return redis.NewIntResult(1, nil)
}

func TestGroupConsumer_SimpleSync(t *testing.T) {
	rdb := simpleSyncMock{}
	cs := NewGroupMultiStreamConsumer[city](context.TODO(), &rdb, "g1", "c1", map[string]string{"s1": ">"})

	var i int64 = 0
	var readCount int64 = 100
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

// TestGroupConsumer_SwitchToNew

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
	var readCount = 100
	var maxHistory = 50
	rdb := switchToNewMock{maxHandout: maxHistory}
	cs := NewGroupMultiStreamConsumer[city](context.TODO(), &rdb, "g1", "c1", map[string]string{"s1": "0-0"})

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

// TestGroupConsumer_RemainingAck
type remainingAckMock struct {
	*redis.Client
	groupCreateMock
}

func (sc *remainingAckMock) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	if ids[0] == "0-0" || ids[0] == "0-1" {
		// fail not zero ack
		return redis.NewIntResult(0, nil)
	} else {
		select {
		case <-ctx.Done():
		case <-time.After(1000 * time.Second):
		}
		return redis.NewIntResult(0, errors.New("must cancel"))
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
	cs := NewGroupMultiStreamConsumer[city](context.TODO(), &rdb, "g1", "c1", map[string]string{"s1": "0-0"}, GroupConsumerConfig{
		AckBufferSize: uint(ackCount) + 1,
	})

	for i := 1; i <= ackCount; i += 1 {
		cs.Ack(Message[city]{ID: fmt.Sprintf("0-%v", i)})
	}

	time.Sleep(50 * time.Millisecond)

	rm := cs.Close()

	assert.Len(t, rm, ackCount)
}

// TestGroupConsumer_AckErrors

type ackErrorMock struct {
	*redis.Client
	groupCreateMock
	i int
}

func (ae *ackErrorMock) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	return redis.NewIntResult(0, errors.New("must fail"))
}

func (ae *ackErrorMock) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	defer func() { ae.i += 1 }()
	return redis.NewXStreamSliceCmdResult([]redis.XStream{{
		Stream: "s1",
		Messages: []redis.XMessage{
			{ID: fmt.Sprintf("0-%v", ae.i), Values: map[string]interface{}{"name": "TestTown"}},
		},
	}}, nil)
}

func TestGroupConsumer_AckErrors(t *testing.T) {
	var readCount = 5_000

	rdb := ackErrorMock{}
	cs := NewGroupMultiStreamConsumer[city](context.TODO(), &rdb, "g1", "c1", map[string]string{"s1": "0-0"})

	var ackErrors = 0
	var seen = 0
	for msg := range cs.Chan() {
		if _, ok := msg.Err.(AckError); ok {
			ackErrors += 1
		} else {
			seen += 1
			cs.Ack(msg)
		}
		if seen == readCount {
			break
		}
	}

	lastErrs := cs.AwaitAcks()
	unseen := cs.Close()
	assert.NotZero(t, len(lastErrs)+len(unseen))
	assert.Equal(t, readCount, ackErrors+len(unseen)+len(lastErrs))
}

// TestGroupConsumer_AckErrorCancel

func TestGroupConsumer_AckErrorCancel(t *testing.T) {
	var readCount = 100

	rdb := ackErrorMock{}
	ctx, cancelFunc := context.WithCancel(context.TODO())
	cs := NewGroupMultiStreamConsumer[city](ctx, &rdb, "g1", "c1", map[string]string{"s1": "0-0"})

	var msgs []Message[city]
	for msg := range cs.Chan() {
		assert.Nil(t, msg.Err)
		msgs = append(msgs, msg)
		if len(msgs) == readCount {
			break
		}
	}

	cancelFunc()

	for _, msg := range msgs {
		cs.Ack(msg)
	}

	remaining := cs.Close()
	assert.Len(t, remaining, readCount)
}

// TestGroupConsumer_FailCreate

type failCreateMock struct {
	*redis.Client
}

func (fcm failCreateMock) XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd {
	return redis.NewStatusResult("", errors.New("test error"))
}

func TestGroupConsumer_CreateError(t *testing.T) {
	rdb := failCreateMock{}
	cs := NewGroupMultiStreamConsumer[city](context.TODO(), &rdb, "g1", "c1", map[string]string{"s1": "0-0"})

	msg := <-cs.Chan()
	assert.NotNil(t, msg.Err)
	assert.IsType(t, ReadError{}, msg.Err)
	assert.ErrorContains(t, msg.Err, "read error: test error")
}

// TestGroupConsumer_ReadError

type readErrorMock struct {
	groupCreateMock
	*redis.Client
}

func (rem readErrorMock) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	return redis.NewXStreamSliceCmdResult(nil, errors.New("test error"))
}

func TestGroupConsumer_ReadError(t *testing.T) {
	rdb := readErrorMock{}
	cs := NewGroupMultiStreamConsumer[city](context.TODO(), &rdb, "g1", "c1", map[string]string{"s1": "0-0"})

	msg := <-cs.Chan()
	assert.NotNil(t, msg.Err)
	assert.NotNil(t, errors.Unwrap(msg.Err))
	assert.IsType(t, ReadError{}, msg.Err)
	assert.ErrorContains(t, msg.Err, "read error: test error")
}

// TestGroupConsumer_ConcurrentRead
type readConcurentErrorMock struct {
	groupCreateMock
	*redis.Client
	i int
}

func (rem *readConcurentErrorMock) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	defer func() { rem.i += 5 }()

	var val = []redis.XStream{}

	if rem.i <= 35 {
		val = []redis.XStream{{
			Stream: "s1",
			Messages: []redis.XMessage{
				{ID: fmt.Sprintf("0-%d", rem.i), Values: map[string]any{"name": "TestTown", "size": 1000}},
				{ID: fmt.Sprintf("0-%d", rem.i+1), Values: map[string]any{"name": "TestTown", "size": 1000}},
				{ID: fmt.Sprintf("0-%d", rem.i+2), Values: map[string]any{"name": "TestTown", "size": 1000}},
				{ID: fmt.Sprintf("0-%d", rem.i+3), Values: map[string]any{"name": "TestTown", "size": 1000}},
			},
		}, {
			Stream: "s2",
			Messages: []redis.XMessage{
				{ID: fmt.Sprintf("0-%d", rem.i), Values: map[string]any{"name": "TestTown", "size": 1000}},
			},
		}, {
			Stream: "s3",
			Messages: []redis.XMessage{
				{ID: fmt.Sprintf("0-%d", rem.i), Values: map[string]any{"name": "TestTown", "size": 1000}},
				{ID: fmt.Sprintf("0-%d", rem.i+1), Values: map[string]any{"name": "TestTown", "size": 1000}},
			},
		}}
	}

	if rem.i >= 40 && rem.i < 70 {
		val = []redis.XStream{{
			Stream: "s1",
			Messages: []redis.XMessage{
				{ID: fmt.Sprintf("0-%d", rem.i), Values: map[string]any{"name": "TestTown", "size": 1000}},
			},
		}, {
			Stream: "s2",
			Messages: []redis.XMessage{
				{ID: fmt.Sprintf("0-%d", rem.i), Values: map[string]any{"name": "TestTown", "size": 1000}},
				{ID: fmt.Sprintf("0-%d", rem.i+1), Values: map[string]any{"name": "TestTown", "size": 1000}},
				{ID: fmt.Sprintf("0-%d", rem.i+2), Values: map[string]any{"name": "TestTown", "size": 1000}},
			},
		}, {
			Stream: "s3",
			Messages: []redis.XMessage{
				{ID: fmt.Sprintf("0-%d", rem.i), Values: map[string]any{"name": "TestTown", "size": 1000}},
			},
		}}
	}

	return redis.NewXStreamSliceCmdResult(val, nil)
}

func (rem *readConcurentErrorMock) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	if len(ids) > 0 {
		i := -1

		fmt.Sscanf(ids[0], "0-%d", &i)

		if i != -1 && i%15 == 0 {
			return redis.NewIntResult(0, errors.New("must fail by modulo"))
		}
	}
	return redis.NewIntResult(1, nil)
}

func TestGroupConsumer_ConcurrentRead(t *testing.T) {
	rdb := readConcurentErrorMock{}

	cs := NewGroupMultiStreamConsumer[city](context.TODO(), &rdb, "g1", "c1", map[string]string{"s1": "0-0", "s2": "0-0", "s3": "0-0"})

	msg := make([]Message[city], 0, 21)
	msgError := make([]Message[city], 0, 21)
	msgList := make([]Message[city], 0, 121)

	for end := true; end; {
		select {
		case tmp := <-cs.Chan():
			if tmp.Err == nil {
				cs.Ack(tmp)
			} else if _, ok := tmp.Err.(AckError); ok {
				msgError = append(msgError, tmp)
				continue
			} else {
				t.Errorf("error not found: %s", tmp.Err.Error())
			}

			msgList = append(msgList, tmp)
			if len(msgList)%2 == 0 {
				ml := cs.AwaitAcks()
				if len(ml) > 0 {
					msg = append(msg, ml...)
				}
			}

		case <-time.Tick(750 * time.Millisecond):
			msg = append(msg, cs.AwaitAcks()...)
			end = false
		}
	}

	assert.Greater(t, len(msgError), 1)
	assert.Greater(t, len(msg), 1)
	assert.Equal(t, len(msg)+len(msgError), 15)
	assert.Equal(t, len(cs.Close())+len(msgList)+len(msg)+len(msgError), 101)
}
