# Getting Started with Barco on Kubernetes

You can install Barco Streams on Kubernetes using `kubectl` by using [our kustomize base][kustomize-base]. You can
follow [our step by step guide to install on K8s](../install/KUBERNETES.md).

After deploying Barco on your Kubernetes cluster, you can start producing and consuming messages using a
[client library][go-client] or directly invoking the Barco's [REST API][rest-api]. The following code snippets
assume that the `barco` statefulset is deployed in a namespace with the name `streams`.

For the purpose of this guide, let's create a [pod] to produce and consume messages from Barco that will get deleted
after we exit the shell.

```shell
kubectl run sample-barco-client -it --image=alpine/curl --rm -- sh
```

From the sample pod in your K8s cluster, you can send a POST request to the REST API to produce a message:

```shell
curl -X POST -i -d '{"hello":"world"}' \
    -H "Content-Type: application/json" \
    "http://barco.streams:9251/v1/topic/my_topic/messages"
```

The Barco service will route to a broker in a round-robin way when no partition key is specified. If you want to
specify the partition key you can set it in the querystring, for example:
`http://barco.streams:9251/v1/topic/my_topic/messages?partitionKey=my_key`

Consuming messages is also supported via client libraries and using the REST API. Consuming requires a certain
request flow to support stateless HTTP clients and still provide ordering and delivery guarantees.

First, register a consumer in the cluster by setting your consumer identifier, the consumer group and
the topics to subscribed to:

```shell
curl -X PUT \
    "http://barco.streams:9252/v1/consumer/register?consumer_id=1&group=my_app&topic=my_topic"
```

Note that `consumer_id` and `group` parameter values can be chosen freely by you, you only have to make sure
those values are uniform across the different instances of your application. In most cases the application name is a
good choice for consumer `group` name and the application instance id or a random uuid are suited for the `consumer_id`
value.

After registering, you can start polling from the brokers:

```shell
curl -i -X POST -H "Accept: application/json" \
    "http://barco.streams:9252/v1/consumer/poll?consumer_id=1"
```

You can continue polling the brokers multiple times to consume data.

Barco internally tracks the reader position of each consumer group (offset) in relationship to the topic and partition.
The broker will automatically commit the previously read data when a new poll request is made from the same consumer.

If at any point in time you want to manually save the reader offset without having to retrieve more data, you
can optionally send a commit request:

```shell
curl -i -X POST "http://barco.streams:9252/v1/consumer/commit?consumer_id=1"
```

Once you finished consuming records from a topic, you can optionally send a goodbye request noting that the consumer
instance will not continue reading. The broker will also attempt to manually commit the offset.

```shell
curl -X POST "http://barco.streams:9252/v1/consumer/goodbye?consumer_id=1"
```

If a consumer does not send requests over a span of 2 minutes to a broker, the brokers will consider the consumer
instance as inactive and it will not be included in the data assignment.

Read more about the API flow and guarantees on the [REST API Documentation][rest-api].

[go-client]: https://github.com/barcostreams/go-client
[kustomize-base]: https://github.com/barcostreams/barco/tree/main/deploy/kubernetes/
[rest-api]: ../REST_API.md
[pod]: https://kubernetes.io/docs/concepts/workloads/pods/
