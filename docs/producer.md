# Producing events

Data is partitioned using Mumur3 tokens. Each broker gets a token assigned in its configuration and acts as leader by default of the partitions which key fall in the range of the token.

A producer client will send one or more messages in a batch to broker acting as leader of a partition. The broker will assign each message an offset id, coalesce the batch of messages with any other outstanding batch from a different producer, compress the resulting group of messages and store it in the local storage and send it to replicas.

After one of the followers acknowledged the message, the leader sends a successful response to the producer client.

## Replication

The leader sends compressed groups of messages to two follower brokers (fixed for now). For a given token, follower brokers are the brokers next to the default leader of that token, ordered by token.

Consider the following figure with four brokers:

```
   (T1)
   /  \
  /    \
 (T4) (T2)
  \    /
   \  /
   (T3)
```

Brokers with tokens 2 & 3 act as default followers for token 1.
Brokers with tokens 4 & 1 act as default followers for token 3.

## Generations

## Broker routing

When a broker recieves messages for which is not an active leader, it can route to the correct broker instance.
