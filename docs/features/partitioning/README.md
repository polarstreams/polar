# Partitioning

PolarStreams stores data in partitions across a cluster, with each partition representing a subset of topic records.
Partitioning, performed through a hashing function over the partition key, enables horizontal scalability
and determines broker-to-consumer fan-out.

Each [PolarStreams broker is assigned a token][how-it-works] based on the ordinal index within the cluster, which will
be used to determine the data that naturally belongs to that broker. A token range is subdivided into
"consumer ranges". **The amount of total partitions in a cluster is determined by the number of brokers,
times the consumer ranges**.

For example: a 3-broker cluster with 4 consumer ranges has 12 partitions and can have up to 12 consumers
per group receiving data in parallel.

The design of partitions determined by `tokens * consumer_ranges` is well suited for scaling clusters: As the number of
brokers increases, tokens also increase, supporting a larger number of consumers receiving data in parallel.

Going back to the previous example, when the cluster scales the number of brokers (e.g. via a [HPA][hpa]) from 3 to 6
due to high usage, the number of possible consumers automatically increases from 12 to 24.

## Consumer Ranges

The consumer ranges determines the amount of partitions per broker. It's designed to answer the question of how many
consumers will be able to read data in parallel per broker (broker-to-consumer fan-out).

Currently, it can be changed using `POLAR_CONSUMER_RANGES` environment variable (defaults to `4`).

Hopefully in the future this setting could be defined at [topic level][topic-issue].

[hpa]: https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/
[how-it-works]: ../../technical_intro/
[topic-issue]: https://github.com/polarstreams/polar/issues/1
