package gtrs

import (
	"context"
	"errors"

	"github.com/go-redis/redis/v8"
)

type TestMessage struct {
	Content string
}

type Event struct {
	Kind     string
	Priority int
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	ctx := context.TODO()

	cs := NewGroupConsumer[Event](ctx, rdb, "group", "consumer", "stream", ">")
	defer cs.Close()

	for msg := range cs.Chan() {
		if _, ok := msg.Err.(AckError); ok {
			_ = errors.Unwrap(msg.Err) // get the redis error
		}

		// handle
		cs.Ack(msg)
	}

	cs.CloseGetRemainingAcks()

}
