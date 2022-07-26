package main

import "github.com/go-redis/redis/v8"

func main() {
	redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}
