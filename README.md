# Go Typed Redis Streams

Effectively reading [Redis streams](https://redis.io/docs/manual/data-types/streams/) requires some boilerplate: counting ids, prefetching and buffering entries, asynchronously sending acknowledgements and parsing entries. What if it was just the following?

```go
consumer := NewConsumer[MyType](...)
for msg := range consumer.Chan() {
    // handle message
}
```

Wait...it is! 🔥

### Quickstart

Define a type that represents your stream data. It'll be parsed automatically with all field names converted to snake case. You can also use [ConvertibleFrom]() and [ConvertibleTo]() to do custom parsing.

```go
type Event struct {
  Name     string
  Priority int
}
// maps to {"name": , "priority": }
```

#### Consumers

Create a new consumer. Specify context, a [redis client](https://github.com/go-redis/redis) and where to start reading. Make sure to specify [custom options](), if you don't like the default ones (buffer sizes and blocking time).

```go
consumer := NewConsumer[Event](ctx, rdb, StreamIds{"my-stream": "$"})
```

Then you can start reading.

```go
for msg := range cs.Chan() {
  if msg.Err != nil {
    continue
  }
  fmt.Println(msg.Stream) // source stream
  fmt.Println(msg.ID)     // entry id
  fmt.Println(msg.Data)   // Event
}
```

But what about error handling? This is where the simplicty fades a little, but there's no way round it :)


The channel is used not only for new messages, but also for errors. There are only two possible errors: [ReadError]() and [ParseError](). The consumer stops on a ReadError, but continues in case of a ParseError.

```go
// Always caused by the redis client. Consumer closes after this error.
if _, ok := msg.Err.(ReadError); ok {
  _ = errors.Unwrap(readErr) // get the redis error
  // msg.ID is empty, no real entry associated with it
}

// Check for your custom parsing errors if you use ConvertibleFrom. 
// They're always wrapped inside a ParseError.
if errors.Is(msg.Err, errMyCustom) {
}

// Check for a FieldParseError. It's wrapped inside a ParseError.
var fpe FieldParseError
if errors.As(msg.Err, &fpe) {
  fmt.Println("failed on", fpe.Field, "got", fpe.Value)
}

```

If you want to start reading from where you left off, then you can save the last seen ids.
```go
seen := cs.CloseGetSeenIds()
```

#### Group Consumers

Group consumers work like regular consumers. They also allow sending acknowledgements, which are processed asynchronously. Make sure, the consumer group exists - the consumer won't create it for you.

```go
rdb.XGroupCreateMkStream(ctx, "stream", "group", "0-0")

cs := NewGroupConsumer[Event](ctx, rdb, "group", "consumer", "stream", ">")

for msg := range cs.Chan() {
  // request an acknowledgement
  cs.Ack(msg)
}
```

Of course we care about correctness:
```go
// wait until all acknowledgements are sent
// or the consumer stops (e.g. context close)
cs.AwaitAcks()

// Stop consumer and get list of unsuccessful
// or unprocessed acknowledgements
failed := cs.CloseGetRemainingAcks()
```

Along with [ReadError]() and [ParseError](), there is one more in this case - [AckError]()

```go
if _, ok := msg.Err.(AckError); ok {
  _ = errors.Unwrap(msg.Err) // get the redis error
  fmt.Println("Failed to acknowledge", msg.ID)
}
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

### Examples

* [This is a small example]() for reading from three consumers in parallel and handling all types of errors.

### Performance

```
go test -run ^$ -bench BenchmarkConsumer -cpu=1
```

The iteration cost on a _mocked client_ is about 500-700 ns depending on buffer sizes, which gives it a **throughput of about 2 million entries a second** 🚀.
