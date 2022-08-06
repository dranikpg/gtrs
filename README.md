# Go Typed Redis Streams

Effectively reading [Redis streams](https://redis.io/docs/manual/data-types/streams/) requires some boilerplate: counting ids, prefetching and buffering, asynchronously sending acknowledgements and parsing entries. What if it was just the following?

```go
consumer := NewConsumer[MyType](...)
for msg := range consumer.Chan() {
    // handle message
}
```

Wait...it is! 🔥

### Quickstart

Define a type that represents your stream data. It'll be parsed automatically with all field names converted to snake case. Missing fields will be skipped silently. You can also use [ConvertibleFrom]() and [ConvertibleTo]() to do custom parsing.

```go
// maps to {"name": , "priority": }
type Event struct {
  Name     string
  Priority int
}
```

#### Consumers

Consumers allow reading redis streams through Go channels. Specify context, a [redis client](https://github.com/go-redis/redis) and where to start reading. Make sure to specify [custom options](), if you don't like the default ones (buffer sizes and blocking time).

```go
consumer := NewConsumer[Event](ctx, rdb, StreamIDs{"my-stream": "$"})

for msg := range cs.Chan() {
  if msg.Err != nil {
    continue
  }
  var event Event = msg.Data
}
```

Don't forget to `Close()` the consumer. If you want to start reading again where you left off, you can save the last  StreamIDs.
```go
ids := cs.CloseGetSeenIds()
```

#### Group Consumers

They work just like regular consumers and allow sending acknowledgements asynchronously.

```go
cs := NewGroupConsumer[Event](ctx, rdb, "group", "consumer", "stream", ">")

for msg := range cs.Chan() {
  if !cs.Ack(msg) { // blocks only if buffer is full
    // oh no, the context was cancelled
  }
}
```

Of course we care about correctness. Not a single error will be lost 🔎
```go
// Wait for all acknowledgements to complete
cs.AwaitAcks()

// Not sent yet or their errors were not consumed
remaining := cs.CloseGetRemainingAcks()
```

#### Error handling

This is where the simplicity fades a litte, but only a little :) The channel provides not just values, but also errors. Those can be only of three types:
- ReadError reports a failed XRead/XReadGroup request. Consumer will close the channel after this error
- AckError reports a failed XAck request
- ParseError speaks for itself

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
- Find out why the default parser failed (e.g. assigning string to int field)
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
stream := NewStream[Event](rdb, "my-stream")
stream.Add(ctx, Event{
  Kind:     "Example event",
  Priority: 1,
}
```

### Installation

```
go get github.com/dranikpg/gtrs
```

Gtrs is still in its early stages and might change in further releases.

### Examples

* [This is a small example]() for reading from three consumers in parallel and handling all types of errors.

### Performance

```
go test -run ^$ -bench BenchmarkConsumer -cpu=1
```

The iteration cost on a _mocked client_ is about 500-700 ns depending on buffer sizes, which gives it a **throughput close to 2 million entries a second** 🚀.
