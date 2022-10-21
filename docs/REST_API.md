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
| serviceName | `string` | In K8s, the name of the Barco service. |


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

{"baseName":"barco-","serviceName":"barco","length":12,"producerPort":9251,"consumerPort":9252}
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

According to the event partition key and the hashing algorithm, the event will be placed in a broker and replicated to
the following two brokers in the cluster.

#### Query String

| Key | Type | Description |
| --- | ---- | ----------- |
| `partitionKey` | `string` | Determines the placement of the data in the cluster, events with the same partition key are guaranteed to be stored (and retrieved) in order. |

#### Response

Responds HTTP status `200 OK` when the data has been stored and replicated.

Responds HTTP status `200 OK` when the data has been stored and replicated.

### `GET /status`

Responds HTTP status `200 OK` when the Producer API is ready on the broker.

## Consumer API



### `GET /status`

Responds HTTP status `200 OK` when the Consumer API is ready on the broker.


Registers automatically on all brokers
Flow: loop logic
Autocommit and manual commit
Read / idle timeout
Ordering (producing)
Connection pooling (recommended but not required)
Clients (recommended but not required)
Consumer flow Flow
