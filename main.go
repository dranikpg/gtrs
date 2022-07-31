package main

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

type TestMessage struct {
	Content string
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	cs := NewConsumer[TestMessage](context.TODO(), rdb, StreamIds{"s1": "0"})
	defer cs.Close()

	var msg Message[TestMessage]
	for msg = range cs.Chan() {
		if msg.Err != nil {
			switch t := msg.Err.(type) {
			case ClientError:
				panic(t)
			case ParsingError:
				fmt.Println("failed to parse", t.Data, "reason", t.Err)
			}
		}
	}
	if msg.Err != nil {
		fmt.Println(msg)
	}
}
