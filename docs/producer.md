# Producing events

Data is partitioned using Murmur3 tokens. Each broker gets a token assigned in its configuration and acts as leader by default of the partitions which key fall in the range of the token.

A producer client will send one or more messages in a batch to broker acting as leader of a partition. The broker will assign each message an offset id, coalesce the batch of messages with any other outstanding batch from a different producer, compress the resulting group of messages and store it in the local storage and send it to replicas.

After one of the followers acknowledged the message, the leader sends a successful response to the producer client.

## Replication

The leader sends compressed groups of messages to two follower brokers (fixed for now). For a given token, follower brokers are the brokers next to the default leader of that token, ordered by token.

Consider the following figure with four brokers:

```
   (T1)
   /  \
  /    \
(T4)  (T2)
  \    /
   \  /
   (T3)
```

Brokers with tokens 2 & 3 act as default followers for token 1.

Brokers with tokens 4 & 1 act as default followers for token 3.

Notation: `B1` is the broker assigned `T1` token, `Bn` is the broker assigned `Tn` token.

## Generations

Each message is assigned a numerical offset that acts as identifier and denotes the position of the message in the log _generation_.

When a broker starts, a _generation_ is created. A generation marks the leader for a given token at a certain time.

For example, after B1 starts, it will communicate to broker instances B2 and B3 that a new generation for T1 is starting. After brokers for T2 and T3 agree, messages for T1 can be assigned offset identifiers.

In case B1 goes offline, once B2 receives a message for T1 range, B2 will communicate with B3 the intention of starting a new generation for T1. If B3 agrees, a new generation for T1 will start. This generation will be valid until B1 becomes back online, at which time, B1 will try to start a new generation setting itself as leader of this new generation for T1.

This _generational_ approach provides resiliency to failure without the need to hold elections, as there will be a deterministic way to set the owner of the a generation at any given time.

Creating generations and persisting them across multiple brokers in a strongly consistent manner requires additional metadata/state to be sent (i.e. see [distributed transactions across heterogenous stores](https://www.researchgate.net/publication/282156834_Scalable_Distributed_Transactions_across_Heterogeneous_Stores)).

Luckily, there are some guarantees that limit the problem scope:

- B3 will never try to start a generation for T1. Only B1 and B2 can do that. B3 will always be a follower for T1.
- Generation creation will only occur when a broker goes offline/online or joins, so it can be a chatty algorithm without risk of overloading the system.
- When B1 detects B2 and B3 offline, it will not attempt to continue acting as generation leader and will reject producer requests for T1. Once B1 regains connectivity to B2 or B3, it will communicate the intention of starting a new generation for T1. Once that is agreed upon, B1 will accept new T1 messages.

## Broker routing

When a broker receives messages for which is not an active leader, it can route to the correct broker instance.
