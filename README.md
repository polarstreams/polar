# Barco Streams

Barco is a lightweight, elastic, Kubernetes-native event streaming system. It acts as a persistent buffer between
services providing at least once delivery guarantees.

Barco Streams is optimized to be resource efficient, have minimal operational overhead and be a good K8s neighbor.

![go build](https://github.com/barcostreams/barco/actions/workflows/go.yml/badge.svg)

## Features

### Lightweight & Fast

- Limited memory footprint: Memory usage can be capped at a few hundreds of Mb
- Production workloads with 0.5GiB of memory per pod
- 20K durable writes per second with 3 [`t3.nano`][t3-nano] brokers
- Small binary size, arm64 as first class citizen
- No additional dependencies, i.e., no Zookeeper

### Elastic

- New brokers can be started up in seconds, join the cluster and receive new data
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
Barco Streams is not production ready, expect bugs and things that don't work.

We honestly value your contribution to make this project ready for general availability. If you want to contribute,
check out the [Contributing Guide](./CONTRIBUTING.md).
</details>

-----

## How does Barco work?

Events are organized in topics. Topics in Barco are always multi-producer and multi-consumer. To achieve high
availability and durability, topic events are persisted on disk on multiple Barco brokers.

Data is automatically distributed across brokers using consistent hashing in a similar way as [Amazon
DynamoDB](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) and [Apache
Cassandra](https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html#dataset-partitioning-consistent-hashing). Each broker is assigned a token based on the [ordinal
index](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#ordinal-index) within the cluster,
which will be used to determine the data that naturally belongs to that broker.

Read the [technical introduction](./docs/TECHNICAL_INTRO.md) in our documentation for more information.

## Installing

### Installing on Kubernetes

You can install Barco on Kubernetes using `kubectl` by using [our kustomize base](./deploy/kubernetes/).

Follow [our guide to install on K8s](./docs/install/KUBERNETES.md).

### Installing with Docker Compose for Application Development

You can use docker / docker compose to run Barco for application development and CI.

Read [this guide to run it using docker compose](./docs/install/DOCKER_COMPOSE.md).

### Installing locally for Application Development

Barco expects the cluster to be composed by multiple brokers. To simplify application development, we introduced
"dev mode" setting to run a single-broker cluster for application development.

After [building](#build) Barco, set `BARCO_DEV_MODE` environment variable before running:

```bash
BARCO_DEV_MODE=true BARCO_HOME=./barco-data go run .
```

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

- Have a look through GitHub issues labeled ["Good first issue"](https://github.com/barcostreams/barco/labels/good%20first%20issue).
- Read the [contribution guide](./CONTRIBUTING.md).
- See the [build instructions](#build), for details on building Barco.
- [Create a fork](https://docs.github.com/en/github/getting-started-with-github/fork-a-repo) of Barco and submit a pull
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

[t3-nano]: https://aws.amazon.com/ec2/instance-types/t3/#Product_Details
