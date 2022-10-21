# Barco Streams - Technical Introduction

## Seminal papers

- [Amazon Dynamo Paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
- [Scalable Distributed Transactions across Heterogeneous Stores](https://www.researchgate.net/profile/Akon-Dey/publication/282156834_Scalable_Distributed_Transactions_across_Heterogeneous_Stores/links/56058b9608ae5e8e3f32b98d/Scalable-Distributed-Transactions-across-Heterogeneous-Stores.pdf)
- [Raft Consensus Algorithm](https://raft.github.io/raft.pdf)

## How does Barco work?

Events are organized in topics. In Barco, topics are always multi-producer and multi-consumer. To achieve high
availability and durability, topic events are persisted on disk on multiple Barco brokers.

Data is automatically distributed across brokers using consistent hashing (Murmur3 tokens) in a similar way as [Amazon
DynamoDB](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) and [Apache
Cassandra](https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html#dataset-partitioning-consistent-hashing). Each broker is assigned a token based on the [ordinal
index](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#ordinal-index) within the cluster,
which will be used to determine the data that naturally belongs to that broker.

According to the event partition key and the hashing algorithm, the event will be placed in a broker and replicated to
the following two brokers in the cluster.

<div>
<img alt="1 ring with 6 nodes" src="https://user-images.githubusercontent.com/2931196/174292608-e7c08749-cbc9-4311-b151-400185f586bf.png" style="max-width:400px;">
</div>

In the preceding diagram, Broker A is the natural leader of token A. When Broker A is considered unavailable by other brokers, after a series of strong consistent operations broker B will take ownership of range (A, B). In case both A and B are considered down, C will not try to take ownership of range (A, B), as it won't be able guarantee the minimum amount of replicas of the data.

Barco uses a deterministic way to assign tokens to brokers (i.e. broker with ordinal 2 will always have the same token). New brokers added to an existing cluster will be placed in the middle of the previous token range, splitting it in half. In the same way, removing brokers causes ranges to be twice the size.

![2 rings with 3 and 6 nodes respectively](https://user-images.githubusercontent.com/2931196/174292614-4124eddc-01f1-4495-8391-93796f32083e.png)

This technique provides a simple way to add/remove brokers without the need to rebalance existing data. New brokers can take ownership of their natural ranges when ready, with the help of previous brokers, without disrupting availability [additional info needed related to how the ownership decisions are taken / transactions for election of a token leader].

A Producer doesn't necessarily have to understand this placement scheme to publish an event. It can target any broker
or the Kubernetes Service and the event message will be routed automatically. From the client's perspective producing
a message is just calling an HTTP endpoint.

To consume events, a client should poll for new data to all live brokers. The brokers will determine when that consumer
should be served with topic events of a given partition depending on the consumer placement.

Barco guarantees that any consumer of a given topic and partition key will always read that events in the same order as
they were written.

## I/O

Internally, Barco uses [Direct I/O][direct-io] and bypasses the Linux page cache to read and write the log segments.

Bypassing the kernel page cache has the following benefits:

- The page cache is a shared resource in K8s. When a container is accessing data in the page cache it has to compete
for resources with unrelated applications on the K8s node.
- No copies exist between the kernel cache and the internal read/write buffers.
- We can use read ahead strategies tuned to the workload when reading.
- We can use buffering and flush strategies tuned to the workload when writing.
- We can control exactly the amount of memory dedicated for buffers and caching (K8s working set).

Additionally, when writing, Barco brokers [compresses and checksums chunks][file-formats] of data
once and replicates the compressed payload, reducing network and CPU usage for replication.

These techniques translates into better overall performance compared to traditional I/O.

We are looking forward to use the new [io_uring] interfaces once its adoption increases.
This will allow us to simplify the buffer logic within Barco, but we don't expect a performance gain from it.

## Interbroker communication

Barco uses a series of TCP connections to communicate between brokers for different purposes:

- **Gossip**: Barco uses a protocol to exchange state information about brokers participating in the cluster, called Gossip.
Each broker uses the Gossip protocol to agree on token range ownership and consumers view/topology with neighboring brokers.
- **Data replication**: All data is automatically replicated to the following brokers in the ring. Compressed groups of events of a certain partition are sent periodically to the followers of the partition leader.
- **Producer routing**: When a producer client sends a new event message to a broker that is not the leader of the partition, the broker will route the message to the correct leader in the foreground.

[direct-io]: https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/5/html/global_file_system/s1-manage-direct-io
[io_uring]: https://en.wikipedia.org/wiki/Io_uring
[file-formats]: https://github.com/barcostreams/barco/blob/docs-reorg/docs/developer/FILE_FORMATS.md
