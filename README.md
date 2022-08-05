### Go Typed Redis Streams

Effectively reading redis streams requires some boilerplate: counting ids, prefetching and buffering entries, asynchronously sending acknowledgements and parsing entries. What if it was just:

```go
consumer := NewConsumer[MyType](...)
for msg := range consumer.Chan() {
    // handle message
}
```

Wait...it is!

### Quickstart

You'll need [go-redis/redis](https://github.com/go-redis/redis)

#### Types

Define a type that represents your stream data:

```go
type Event struct {
	Name     string
	Priority int
}
```

It'll be parsed automatically. You can also use [ConvertibleFrom]() and [ConvertibleTo]() to do custom parsing.

#### Streams

#### Consumers

Create a new consumer. Specify context, client and where to start reading. Make sure to specify [custom options](), if you don't like the default ones.

```go
consumer := NewConsumer[Event](ctx, rdb, StreamIds{"my-stream": "$"})
defer consumer.Close()
```

Then you can start reading with a channel.

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

But what about error handling? This is where the simplicty fades a little, but there's no way round them.

There are only two possible errors: ReadError and ParseError. The consumer stops on a ReadError, but continues in case of a ParseError.

```go
// Always caused by the client. Consumer closes after this error.
if _, ok := msg.Err.(ReadError); ok {
	fmt.Println(errors.Unwrap(readErr)) // get the redis error
}

// Check for your custom parsing errors if you use ConvertibleFrom. They're always wrapped inside a ParseError.
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
seen := cs.SeenIds()
```

#### Group Consumers

### Installation

```
go get github.com/dranikpg/gtrs
```

### Performance

```
go test -run ^$ -bench BenchmarkConsumer -cpu=1
```

The iteration cost on a _mocked client_ is about 500-700 ns depending on buffer sizes, which gives it a **throughput of about 2 million entries a second**.