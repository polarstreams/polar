# PolarStreams

PolarStreams is a lightweight, elastic, Kubernetes-native event streaming system. It acts as a persistent buffer between
services providing at-least-once delivery guarantees.

PolarStreams is optimized to be developer friendly, resource efficient, have minimal operational overhead, and be a
good K8s neighbor.

![go build](https://github.com/polarstreams/polar/actions/workflows/go.yml/badge.svg)

## Features

### Lightweight & Fast

- Limited memory footprint
- Production workloads using just 0.5GiB of memory per pod
- [1+ million durable writes per second on commodity hardware][benchmarks]
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
PolarStreams is not production ready yet, expect bugs and things that don't work.

We honestly value your contribution to make this project ready for general availability. If you want to contribute,
check out the [Contributing Guide](./CONTRIBUTING.md).
</details>

-----

## Resources

- [Documentation Index](./docs/)
- [Why PolarStreams?](#why-polarstreams)
- [How does PolarStreams work?](./docs/technical_intro/)
- [Benchmarks][benchmarks]
- [REST API docs][rest-api]
- [Installing](./docs/install/)
- [FAQ](./docs/faq/)

## Getting Started

Producing and consuming messages from PolarStreams is as simple as sending a HTTP request. Use your favorite HTTP client in
your technology stack to send and receive events. Additionally, we also provide a [Go Client Library][go-client].

### Getting Started on Kubernetes

Get started with PolarStreams on Kubernetes using [this guide](./docs/getting_started/on_kubernetes/).

### Getting Started on Docker

PolarStreams is distributed by default with a minimal size of 3 brokers for production use. You can run a
single-broker using Docker / Podman with developer mode. Read more about [how to get started with PolarStreams on
Docker](./docs/getting_started/on_docker/).

-----

## Why PolarStreams?

PolarStreams was created to provide a developer friendly and resource efficient event streaming solution for
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
topics that can help you to build PolarStreams from source.

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
- See the [build instructions](#build), for details on building PolarStreams.
- [Create a fork][create-fork] of PolarStreams and submit a pull
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

[good-first-issue]: https://github.com/polarstreams/polar/labels/good%20first%20issue
[create-fork]: https://docs.github.com/en/github/getting-started-with-github/fork-a-repo
[go-client]: https://github.com/polarstreams/go-client
[rest-api]: ./docs/rest_api/
[benchmarks]: ./docs/benchmarks/
