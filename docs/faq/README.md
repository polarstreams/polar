# Frequently Asked Questions

## How Barco compares to Apache Kafka

When Apache Kafka was introduced, it solved a previously unresolved problem: how to safely pass messages between
applications at scale. Kafka's design contained a set of original ideas around message offset management and
partitioning that were groundbreaking at the time.

With Barco, we introduced a new design that attempts to solve the remaining pain points when using and deploying a
event streaming solution like Apache Kafka and Apache Pulsar.

Both Kafka and Barco share the following characteristics:

- Persistent and replicated log
- At least once delivery guarantees
- multi-producer and multi-consumer
- Consumer groups to tolerate consumer failure

Barco differentiators:

- Kubernetes native: no operator, no page cache use.
- No need for overprovisioning to handle usage peaks.
- Lightweight: production workloads with less than 1 dedicated vCPU and 0.5 GiB of memory.

## What are the hardware requirements

Barco supports production workloads starting at 0.5GiB of memory and less than a dedicated vCPU. Barco is fast even
when running on the lower end of commodity hardware, [check out the benchmarks for more information][benchmarks].

## What settings should I tune?

We strive for having no settings to adapt or watch out for when managing a Barco deployment. In practice, we only
recommend looking for the default number of consumer ranges per broker (fanout).


[benchmarks]: ../benchmarks/