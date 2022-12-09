# Frequently Asked Questions

## How PolarStreams compares to Apache Kafka

When Apache Kafka was introduced, it solved a previously unresolved problem: how to safely pass messages between
applications at scale. Kafka's design contained a set of original ideas around message offset management and
partitioning that were groundbreaking at the time.

With PolarStreams, we introduced a new design that attempts to solve the remaining pain points when using and deploying a
event streaming solution like Apache Kafka and Apache Pulsar.

Both Kafka and PolarStreams share the following characteristics:

- Persistent and replicated log
- At least once delivery guarantees
- multi-producer and multi-consumer
- Consumer groups to tolerate consumer failure

PolarStreams differentiators:

- Kubernetes native: no operator, no page cache use.
- No need for overprovisioning to handle usage peaks.
- Lightweight: production workloads with less than 1 dedicated vCPU and 0.5 GiB of memory.

## What are the hardware requirements

PolarStreams supports production workloads starting at 0.5GiB of memory and less than a dedicated vCPU. PolarStreams is fast even
when running on the lower-end of commodity hardware, [check out the benchmarks for more information][benchmarks].

## What settings should I tune?

We strive for having no settings to adapt or watch out for when managing a PolarStreams deployment. In practice, we only
recommend looking for the default number of [consumer ranges per broker (fan-out)][partitioning].

## Why the project name change?

PolarStreams project used to be called Barco Streams. We decided to change it because there was an existing large
company named Barco with a product named SecureStream (unrelated to event streaming). The name changed
 but the goal of the project is the same: to provide a persistent, distributed, durable and elastic
event streaming solution that's easy to use and operate.

[benchmarks]: ../benchmarks/
[partitioning]: ../features/partitioning/
