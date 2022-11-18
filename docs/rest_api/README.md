# Barco Streams REST API

Barco Streams provide HTTP APIs to produce and consume messages from the cluster, and to introspect the
status of the brokers in the cluster.

## Table of Contents

- [Discovery API](#discovery-api)
- [Producer API](#producer-api)
- [Consumer API](#consumer-api)

## Discovery API

The Discovery API is exposed in port `9250` by default and provides information about the topology and status of the
cluster. It is used by client libraries to support targeting specific brokers.

### `GET /v1/brokers`

Retrieves the cluster topology.

#### Response

A JSON Object containing the following properties:

| Property | Type | Description |
| -------- | ---- | ----------- |
| length | `number` | Current amount of broker instances in the cluster. |
| producerPort | `number` | The port number exposing the [Producer API](#producer-api). |
| consumerPort | `number` | The port number exposing the [Consumer API](#consumer-api). |
| names | `string[]` | The host names of the cluster, on K8s it can be empty for large clusters, providing `baseName` and `serviceName` instead. |
| baseName | `string` | In K8S, the host base name that composed with the pod ordinal and `serviceName`, compose the host name of a broker. |
| serviceName | `string` | In K8s, the name of the Barco service with the namespace. |


#### Examples

Getting the topology of a 3-broker cluster.

```bash
$ curl -i "http://barco.streams:9250/v1/brokers"
HTTP/1.1 200 OK
Content-Type: application/json

{"length":3,"names":["barco-0.barco.streams","barco-1.barco.streams","barco-2.barco.streams"],"producerPort":9251,"consumerPort":9252}
```

Getting the topology of a 12-broker cluster.

```shell
$ curl -i "http://barco.streams:9250/v1/brokers"
HTTP/1.1 200 OK
Content-Type: application/json

{"baseName":"barco-","serviceName":"barco.streams","length":12,"producerPort":9251,"consumerPort":9252}
```

### `GET /status`

Responds HTTP status `200 OK` when the discovery API is ready on the broker.

-----

## Producer API

The Producer API, exposed in port `9251` by default, is used to send events to a topic.

### `POST /v1/topic/{topic}/messages`

Stores one or more events. When a `partitionKey` is provided in the query string, Barco will route the request to the
leader of the partition to provide the following ordering guarantee: events with the same partition key are stored
(and retrieved) in order.

The endpoint supports 2 content types: `application/json` (default) and `application/x-ndjson`. [NDJSON][ndjson] is
well suited for sending multiple topic events into a single request.

#### Query String

| Key | Type | Description |
| --- | ---- | ----------- |
| `partitionKey` | `string` | Determines the placement of the data in the cluster, events with the same partition key are guaranteed to be stored (and retrieved) in order. |

#### Response

Responds HTTP status `200 OK` when the data has been stored and replicated.

#### Examples:

Sending an event with the partition key set.

```shell
$ curl -X POST -i -d '{"productId": 123, "units": -5}' \
    -H "Content-Type: application/json" \
    "http://barco.streams:9251/v1/topic/product-stock/messages?partitionKey=123"
```

### `GET /status`

Responds HTTP status `200 OK` when the Producer API is ready on the broker.

-----

## Consumer API

The Consumer API, exposed in port `9252` by default, is used to retrieve events from a topic.

Consuming requires a certain request flow to support stateless HTTP clients and still provide delivery guarantees:
you need to register the consumer first in order to read records from a topic.

### `PUT /v1/consumer/register`

Registers a consumer in all the brokers, it's the first step in the read flow and it's idempotent.

#### Query String

| Key | Type | Description |
| --- | ---- | ----------- |
| `consumerId` | `string` | A text value chosen by you to identify the consumer in the read flow. In general The application instance id or a random uuid are suited for the `consumerId` value, as long as you reuse the id across the following requests of the read flow. |
| `group` | `string` (optional)| The name of the consumer group, In most cases the application name is a good choice for consumer `group` name. Defaults to `"default"`. |
| `topic` | `string[]` | The topics to subscribe to. In case it is more than one, you can send repeating the parameter key and value, for example: `?topic=a&topic=b`. |
| `onNewGroup` | `string` | Determines the start offset when there's no information for a given consumer group. Possible values are `startFromLatest` (default) and `startFromEarliest`.|

#### Example

Register a consumer in the cluster subscribing to the topic `"product-stock"`.

```shell
$ curl -X PUT \
    "http://barco.streams:9252/v1/consumer/register?consumerId=1&group=product-stock-updater&topic=product-stock"
```

#### Response

Responds HTTP status `200 OK` when the consumer is registered on all brokers.

### `POST /v1/consumer/poll`

#### Query String

| Key | Type | Description |
| --- | ---- | ----------- |
| `consumerId` | `string` (required) | The consumer identifier used to register the consumer. |

#### Response

Responds HTTP status `200 OK` with data in the response body. The data is a JSON Array containing objects with the
following properties:

| Property | Type | Description |
| -------- | ---- | ----------- |
| topic | `string` | Name of the topic. |
| token | `string` | Token that determines the placement of the data. |
| rangeIndex | `number` | Range index that determines the placement. |
| version | `number` | Generation version. |
| startOffset | `string` | An int64 value (represented as string containing a decimal value) that details the numerical offset of the first event. The offset of the following events can be calculated as `startOffset+{value_index}`. |
| values | `array` | An array of events as produced. |

Responds HTTP status `204 No Content` when there's no data available to read.

Responds HTTP status `409 Conflict` when the consumer is not considered to be register. The caller should invoke the
Register endpoint from above and retry.

#### Examples

```
$ curl -i -X POST -H "Accept: application/json"\
    "http://barco.streams:9252/v1/consumer/poll?consumerId=1"
HTTP/1.1 200 OK
Content-Type: application/json

[{"topic":"product-stock","token":"-9223372036854775808","rangeIndex":0,"version":1,"startOffset":"6","values":[{"productId": 123, "units": -1}, {"productId": 123, "units": 20}]}]
```

### `POST /v1/consumer/commit`

Manually commits the position of the reader. This is not required as part of the normal consuming flow, as the
brokers will automatically commit the previous position when new data is requested. Goodbye endpoint will also commit
the position of the reader.

#### Query String

| Key | Type | Description |
| --- | ---- | ----------- |
| `consumerId` | `string` | The consumer identifier used to register the consumer. |

#### Response

Responds HTTP status `204 No Content` with data in the response body.

Responds HTTP status `409 Conflict` when the consumer is not considered to be register.

### Examples

Manually commit the position of the reader.

```shell
$ curl -i -X POST "http://barco.streams:9252/v1/consumer/commit?consumerId=1"
```

### `POST /v1/consumer/goodbye`

Commits the position of the reader and unregisters the consumer. It should normally be called when exiting the consuming
loop.

#### Query String

| Key | Type | Description |
| --- | ---- | ----------- |
| `consumerId` | `string` | The consumer identifier used to register the consumer. |

#### Response

Responds HTTP status `200 No Content` when it was successfully unregistered.

Responds HTTP status `409 Conflict` when the consumer is not considered to be register.

### `GET /status`

Responds HTTP status `200 OK` when the Consumer API is ready on the broker.

[ndjson]: http://ndjson.org/

<!--TODO: Talk about the following topics-->
<!--Flow: loop logic: 409, ... -->
<!--Read / idle timeout-->
<!--Ordering (producing)-->
<!--Consumer flow Flow-->
