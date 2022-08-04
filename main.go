package gtrs

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

	cs := NewGroupConsumer[TestMessage](context.TODO(), rdb, "g1", "c1", "s1", "0-0")
	defer cs.Close()
	defer cs.AwaitAcks()

	for msg := range cs.Chan() {
		if msg.Err != nil {
			break
		}
		fmt.Println(msg)
		fmt.Scan(new(int))
		cs.Ack(msg)
		cs.AwaitAcks()
	}
	fmt.Println("---")
	fmt.Println(cs.RemainingAcks())
}
