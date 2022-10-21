# Barco Streams

Barco is a lightweight, elastic, Kubernetes-native event streaming system. It acts as a persistent buffer between
services providing at-least-once delivery guarantees.

Barco Streams is optimized to be developer friendly, resource efficient, have minimal operational overhead, and be a
good K8s neighbor.

![go build](https://github.com/barcostreams/barco/actions/workflows/go.yml/badge.svg)

## Features

### Lightweight & Fast

- Limited memory footprint
- Production workloads using just 0.5GiB of memory per pod
- [1+ million durable writes per second on commodity hardware](./docs/BENCHMARKS.md)
- Small binary size, arm64 as first-class citizen
- No additional dependencies, i.e., no Zookeeper

### Elastic

- New brokers can be started up, join the cluster, and receive new data in seconds
- No need to operate it manually for scaling up/down cluster
- After a period of low usage, it scales down automatically
- Elastic in both computing and storage

### Kubernetes Native

- Basic setup using StatefulSets
- Data distribution is K8s-aware: data placement based on the StatefulSet's pod ordinal
- No need for an operator
- Good K8s neighbor: Direct I/O, no OS page cache for log segments

-----

<details>
<summary>⚠️ <strong>The project is still in early development</strong> ⚠️</summary>
Barco Streams is not production ready yet, expect bugs and things that don't work.

We honestly value your contribution to make this project ready for general availability. If you want to contribute,
check out the [Contributing Guide](./CONTRIBUTING.md).
</details>

-----

## Resources

- [Documentation Index](./docs/README.md)
- [Why Barco?](#why-barco-streams)
- [How does Barco work?](./docs/TECHNICAL_INTRO.md)
- [Benchmarks](./docs/BENCHMARKS.md)
- [REST API docs][rest-api]
- [Installing](./docs/install/README.md)
- [FAQ](./docs/FAQ.md)

## Getting Started

Producing and consuming messages from Barco is as simple as sending a HTTP request. Use your favorite HTTP client in
your technology stack to send and retrieve events. Additionally, we also provide a [Go Client Library][go-client].

### Getting Started on Docker

<!-- Start excerpt from docs/getting_started/WITH_DOCKER.md -->

Barco Streams is distributed by default with a minimal size of 3 brokers for production use. You can run a
single-broker using Docker/Podman with developer mode enabled to get started quickly.

```shell
docker run --rm --env BARCO_DEV_MODE=true -p 9250-9252:9250-9252 barcostreams/barco:latest
```

You can start producing messages using a [client library][go-client] or directly invoking the Barco's
[REST API][rest-api], for example:

```shell
curl -X POST -i -d '{"hello":"world"}' \
    -H "Content-Type: application/json" \
    "http://localhost:9251/v1/topic/my_topic/messages"
```

Consuming messages is also supported via the REST API or with client libraries. Consuming requires a certain
request flow to support stateless HTTP clients and still provide ordering and delivery guarantees.

First, register a consumer in the cluster by setting your consumer identifier, the consumer group and
the topics to subscribed to:

```shell
curl -X PUT \
    "http://localhost:9252/v1/consumer/register?consumer_id=1&group=my_app&topic=my_topic"
```

Note that `consumer_id` and `group` parameter values can be chosen freely by you, you only have to make sure
those values are uniform across the different instances of your application. In most cases the application name is a
good choice for consumer `group` name and the application instance id or a random uuid are suited for the `consumer_id`
value.

After registering, you can start polling from the brokers:

```shell
curl -i -X POST -H "Accept: application/json" \
    "http://localhost:9252/v1/consumer/poll?consumer_id=1"
```

You can continue polling the brokers multiple times to consume data.

<!-- End excerpt -->

Read more about the [Barco REST API in the documentation][rest-api]. You can also check out our official
[Client Library for Go][go-client].

### Getting started on Kubernetes

Get started on Kubernetes using [this guide](./docs/getting_started/ON_KUBERNETES.md).

-----

## Why Barco Streams?

Barco Streams was created to provide a developer friendly and resource efficient event streaming solution for
Kubernetes that is easy to use and operate.

We believe that deploying and managing a persistent event streaming for a microservice architecture should be as easy as
deploying a stateless service. Users should be able to start with small pods (512MiB of memory!) and elastically scale
to support larger workloads with no operational overhead.

Ease of use and resource efficiency is what the Cloud is all about. Pay for the resources that you need and avoid
overprovisioning in advance.

## Build

```bash
go build .

go test -v ./...
```

The [Contributing Guide](./CONTRIBUTING.md#environment-setup) has more information about environment setup and other
topics that can help you to build Barco from source.

## Design Principles

### Act as safe buffer between services

Persistent event streaming should act as a buffer between services, supporting peaks by seamlessly scaling,
allowing events to be consumed at a later time from the peak. Once a certain amount of events have been produced
and not consumed, the following events can be stored in object storage to be pulled once the consumers catch up,
reducing costs for storage.

### Always-on with cost control

Message streaming should be highly available by default and only consume the minimum required resources to run
effectively.

### Operational simplicity

Regular maintenance operations like upgrading the cluster or decommissioning Kubernetes nodes are common and should
be handled smoothly. For example, when a consumer is being restarted due to an upgrade, there's no need for re-balance
the data assignment among consumers as it's very likely that the consumer will be ready in a few seconds. Querying the
Kubernetes API can provide valuable insights to understand what is occurring and what to expect.

### Linearly horizontally scalable

The system should elastically scale intelligently, to support the future web scale and without affecting running
services.

## Contribute

We are always happy to have contributions to the project whether it is source code, documentation, bug reports,
feature requests or feedback. To get started with contributing:

- Have a look through GitHub issues labeled ["Good first issue"][good-first-issue].
- Read the [contribution guide](./CONTRIBUTING.md).
- See the [build instructions](#build), for details on building Barco.
- [Create a fork][create-fork] of Barco and submit a pull
request with your proposed changes.

## License

Copyright (C) 2022 Jorge Bay

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

https://www.gnu.org/licenses/agpl-3.0.html

[good-first-issue]: https://github.com/barcostreams/barco/labels/good%20first%20issue
[create-fork]: https://docs.github.com/en/github/getting-started-with-github/fork-a-repo
[go-client]: https://github.com/barcostreams/go-client
[rest-api]: ./docs/REST_API.md