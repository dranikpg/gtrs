# Go Typed Redis Streams

<a href="https://pkg.go.dev/github.com/dranikpg/gtrs"><img src="https://godoc.org/github.com/dranikpg/gtrs?status.svg" /></a>
[![Go Report Card](https://goreportcard.com/badge/github.com/dranikpg/gtrs)](https://goreportcard.com/report/github.com/dranikpg/gtrs)

Effectively reading [Redis streams](https://redis.io/docs/manual/data-types/streams/) requires some work: counting ids, prefetching and buffering, asynchronously sending acknowledgements and parsing entries. What if it was just the following?

```go
consumer := NewGroupConsumer[MyType](...)
for msg := range consumer.Chan() {
  // Handle mssage
  consumer.Ack(msg)
}
```

Wait...it is! 🔥

### Quickstart

Define a type that represents your stream data. It'll be parsed automatically with all field names converted to snake case. Missing fields will be skipped silently. You can also use the `ConvertibleFrom` and `ConvertibleTo` interfaces to do custom parsing.

```go
// maps to {"name": , "priority": }
type Event struct {
  Name     string
  Priority int
}
```

#### Consumers

Consumers allow reading redis streams through Go channels. Specify context, a [redis client](https://github.com/go-redis/redis) and where to start reading. Make sure to specify `StreamConsumerConfig`, if you don't like the default ones or want optimal performance. New entries are fetched asynchronously to provide a fast flow 🚂

```go
consumer := NewConsumer[Event](ctx, rdb, StreamIDs{"my-stream": "$"})

for msg := range cs.Chan() {
  if msg.Err != nil {
    continue
  }
  var event Event = msg.Data
}
```

Don't forget to `Close()` the consumer. If you want to start reading again where you left off, you can save the last StreamIDs.
```go
ids := cs.Close()
```

#### Group Consumers

They work just like regular consumers and allow sending acknowledgements asynchronously. Beware to use `Ack` only if you keep processing new messages - that is inside a consuming loop or from another goroutine. Even though this introduces a two-sided depdendecy, the consumer is avoids deadlocks.

```go
cs := NewGroupConsumer[Event](ctx, rdb, "group", "consumer", "stream", ">")

for msg := range cs.Chan() {
  cs.Ack(msg)
}
```

Stopped processing? Check your errors 🔎
```go
// Wait for all acknowledgements to complete
errors := cs.AwaitAcks()

// Acknowledgements that were not sent yet or their errors were not consumed
remaining := cs.Close()
```

#### Error handling

This is where the simplicity fades a litte, but only a little :) The channel provides not just values, but also errors. Those can be only of three types:
- `ReadError` reports a failed XRead/XReadGroup request. Consumer will close the channel after this error
- `AckError` reports a failed XAck request
- `ParseError` speaks for itself

Consumers don't send errors on cancellation and immediately close the channel.

```go
switch errv := msg.Err.(type) {
case nil: // This interface-nil comparison in safe
  fmt.Println("Got", msg.Data)
case ReadError:
  fmt.Println("ReadError caused by", errv.Err)
  return // last message in channel
case AckError:
  fmt.Printf("Ack failed %v-%v caused by %v\n", msg.Stream, msg.ID, errv.Err)
case ParseError:
  fmt.Println("Failed to parse", errv.Data)
}
```

All those types are wrapping errors. For example, `ParseError` can be unwrapped to:
- Find out why the default parser failed via `FieldParseError` (e.g. assigning string to int field)
- Catch custom errors from `ConvertibleFrom`

```go
var fpe FieldParseError
if errors.As(msg.Err, &fpe) {
  fmt.Printf("Failed to parse field %v because %v", fpe.Field, fpe.Err)
}

errors.Is(msg.Err, errMyTypeFailedToParse)
```

#### Streams

Streams are simple wrappers for basic redis commands on a stream.

```go
stream := NewStream[Event](rdb, "my-stream", &Options{TTL: time.Hour})
stream.Add(ctx, Event{
  Kind:     "Example event",
  Priority: 1,
})
```
The Options.TTL parameter will evict stream entries after the specified duration has elapsed (or it can be set to `NoExpiration`).

### Installation

```
go get github.com/dranikpg/gtrs
```

Gtrs is still in its early stages and might change in further releases.

### Examples

* [This is a small example](https://github.com/dranikpg/gtrs-test) for reading from three consumers in parallel and handling all types of errors.

### Performance

```
go test -run ^$ -bench BenchmarkConsumer -cpu=1
```

The iteration cost on a _mocked client_ is about 500-700 ns depending on buffer sizes, which gives it a **throughput close to 2 million entries a second** 🚀. Getting bad results? Make sure to set large buffer sizes in the options.
