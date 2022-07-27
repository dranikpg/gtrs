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

	for msg := range cs.Chan() {
		fmt.Println(msg)
	}
}
