# Getting Started with PolarStreams on Docker

PolarStreams is distributed by default with a minimal size of 3 brokers for production use. You can run a
single-broker using Docker / Podman with developer mode enabled to get started quickly.

```shell
docker run --rm --env POLAR_DEV_MODE=true -p 9250-9254:9250-9254 polarstreams/polar:latest
```

You can start producing messages using a [client library][go-client] or directly invoking the PolarStreams's
[REST API][rest-api]. You can use your technology of choice to send requests to the REST API, there are some examples
in [Rust][example-rust], [Node.js][example-nodejs] and others in the [Client Examples][client-examples]
repository.

For the purpose of this guide, we will use `curl` as an HTTP client.

```shell
curl -X POST -i -d '{"hello":"world"}' \
    -H "Content-Type: application/json" \
    "http://localhost:9251/v1/topic/my-topic/messages"
```

Consuming messages is also supported via client libraries and using the REST API. Consuming requires a certain
request flow to support stateless HTTP clients and still provide ordering and delivery guarantees.

First, register a consumer in the cluster by setting your consumer identifier, the consumer group and
the topics to subscribed to:

```shell
curl -X PUT \
    "http://localhost:9252/v1/consumer/register?consumerId=1&group=my-app&topic=my-topic"
```

Note that `consumerId` and `group` parameter values can be chosen freely by you, you only have to make sure
those values are uniform across the different instances of your application. In most cases the application name is a
good choice for consumer `group` name and the application instance id or a random uuid are suited for the `consumerId`
value.

After registering, you can start polling from the brokers:

```shell
curl -i -X POST -H "Accept: application/json" \
    "http://localhost:9252/v1/consumer/poll?consumerId=1"
```

You can continue polling the brokers multiple times to consume data.

PolarStreams internally tracks the reader position of each consumer group (offset) in relationship to the topic and partition.
The broker will automatically commit the previously read data when a new poll request is made from the same consumer.

If at any point in time you want to manually save the reader offset without having to retrieve more data, you
can optionally send a commit request:

```shell
curl -i -X POST "http://localhost:9252/v1/consumer/commit?consumerId=1"
```

Once you finished consuming records from a topic, you can optionally send a goodbye request noting that the consumer
instance will not continue reading. The broker will also attempt to manually commit the offset.

```shell
curl -X POST "http://localhost:9252/v1/consumer/goodbye?consumerId=1"
```

If a consumer does not send requests over a span of 2 minutes to a broker, the brokers will consider the consumer
instance as inactive and it will not be included in the data assignment.

Read more about the API flow and guarantees on the [REST API Documentation][rest-api].

[rest-api]: ../../rest_api/
[go-client]: https://github.com/polarstreams/go-client
[example-rust]: https://github.com/polarstreams/client-examples/tree/main/rust
[example-nodejs]: https://github.com/polarstreams/client-examples/tree/main/nodejs
[client-examples]: https://github.com/polarstreams/client-examples
