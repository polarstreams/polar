# Producing events

Data is partitioned using Mumur3 tokens. Each broker gets a token assigned in its configuration and acts as leader by default of the partitions which key fall in the range of the token.

A producer client will send one or more messages in a batch to broker acting as leader of a partition. The broker will assign each message an offset id, coalesce the batch of messages with any other outstanding batch from a different producer, compress the resulting group of messages and store it in the local storage and send it to replicas.

## Replication



## Generations

## Broker routing

When a broker recieves messages for which is not an active leader, it can route to the correct broker instance.
