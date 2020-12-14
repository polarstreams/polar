# Soda Streams

Soda is a lightweight, elastic, kubernetes friendly messaging system.

## Features

### Lightweight

- Small binary size.
- Limited memory footprint: Memory usage is capped at few hundreds of Mb.
- No additional dependencies, i.e., no Zookeeper.

### Elastic

- New nodes can be started up in seconds, join the cluster and receive new
data.
- No need to assign data partitions
- Scale down easily.

### Kubernetes friendly

- Basic setup using StatefulSets.
- Volume setup, scaling, rolling upgrades is managed by Kubernetes.

## Limitations

- No log compaction: data is kept after is consumed for a fixed period of time or
when the log reaches some predetermined size.

## TODO / Investigate
- Multi region, Multicloud
- Consumer replay
