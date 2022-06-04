# Getting Started with Barco

Barco supports producing and consuming events using [HTTP/2][http-2] APIs. HTTP/2 provides solid low level features
that are required in a client-server protocol like framing, request pipelining, ping, compression, etc. without
the performance pitfalls of HTTP 1.

## Producing

Sending events to Barco is as simple as sending a HTTP request. Use your favorite HTTP client in your technology stack
to produce events.

Barco Producer Server supports both HTTP 1.1 and HTTP/2 to support clients of all technology stacks but we recommend
using HTTP/2 whenever possible.

### Producing an event using curl

```shell
TOPIC="my-topic"
curl -X POST -i -d '{"hello":"world"}' \
    -H "Content-Type: application/json" --http2-prior-knowledge \
    "http://barco.streams:9251/v1/topic/${TOPIC}/messages"
```

Note that `9251` is the default producing port. The Barco service will route to a broker in a round-robin way when no
partition key is specified.

If you want to specify the partition key, you can set it in the querystring.

```shell
PARTITION_KEY="key1"
TOPIC="my-topic"
curl -X POST -i -d '{"hello":"world"}' \
    -H "Content-Type: application/json" --http2-prior-knowledge \
    "http://barco.streams:9251/v1/topic/${TOPIC}/messages?partitionKey=${PARTITION_KEY}"
```

### Producing an event in Go

You can use the [official Go Client][go-client]. There's no need to target a port in the service url, the client
will discover the cluster and send the request to the broker that is the partition leader for a key.

```go
import (
	"strings"

	"github.com/barcostreams/go-client"
)

// ...

producer, err := barco.NewProducer("barco://barco.streams")
if err != nil {
	panic(err)
}

topic := "my-topic"
message := strings.NewReader(`{"hello": "world"}`)
partitionKey := "" // Empty to use a random partition

if err := producer.Send(topic, message, partitionKey); err != nil {
	panic(err)
}
```

Read more in the [Go Client's Getting Started Guide][go-client-start].

## Consuming

TODO: HTTP/2 only

[http-2]: https://en.wikipedia.org/wiki/HTTP/2
[go-client]: https://github.com/barcostreams/go-client
[go-client-start]: https://github.com/barcostreams/go-client#getting-started
