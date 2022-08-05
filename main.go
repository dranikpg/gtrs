package gtrs

import (
	"context"
	"fmt"

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

	cs := NewConsumer[Event](ctx, rdb, StreamIds{"my-stream": "$"})
	defer cs.Close()

	var msg Message[Event]
	for msg = range cs.Chan() {
		if msg.Err != nil {
			continue
		}
		fmt.Println(msg.Stream) // source stream
		fmt.Println(msg.ID)     // entry id
		fmt.Println(msg.Data)   // your data (the Event)

		if pe, ok := msg.Err.(ParseError); ok {
			fmt.Println(pe)
		}
	}

	cs.SeenIds()
}
