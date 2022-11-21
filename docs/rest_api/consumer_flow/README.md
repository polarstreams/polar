# Consumer API Flow

Consuming messages from Barco Streams requires a certain request flow to support stateless HTTP clients and still
provide delivery guarantees.

You should make sure that you have at least one connection to each broker address. You can either use the DNS AAAA
records from the Kubernetes headless service exposed by Barco or use the discovery service to get the individual broker
addresses.

This is a summary of the request/response flow of a typical application that consumes data from a Barco cluster:

* Discover the broker addresses
* Register as consumer in the cluster
* In loop
  * Poll each broker
* Unregister consumer

There's an [implementation example in Node.js][nodejs-example] of the request-response flow in the
[Client Examples][client-examples] repository.

## Inactivity timeouts

A broker considers a consumer as active when it has received a request (poll/status/...) within the past 2m. After that,
it will automatically unregister the consumer.

When a consumer is not considered as registered by a broker, [poll requests](../README.md#post-v1consumerpoll) will
return `409 Conflict` HTTP status. If the consumer is still active and wants to continue polling, it should send
a [register request](../README.md#put-v1consumerregister) and retry.

## Offset Commit

Barco Streams brokers keep track the position of the consumers. After fetching a group of messages, when the consumer
polls again on the same broker, the broker will automatically commit the consumer position before returning the
next group of messages.

If a consumer wants to manually commit the reader position without requesting more data, it can send a [commit
request](../README.md#post-v1consumercommit). Additionally, a [goodbye request](../README.md#post-v1consumergoodbye)
will also commit the position as well as unregistering the consumer.

[nodejs-example]: https://github.com/barcostreams/client-examples/tree/main/nodejs/consuming
[client-examples]: https://github.com/barcostreams/client-examples/
