# Soda Streams

Soda is a lightweight, elastic, kubernetes friendly messaging system.

## Features

### Lightweight

- Small binary size.
- Limited memory footprint: Memory usage is capped at few hundreds of Mb.
- Two nodes for a minimal highly available deployment.
- No additional dependencies, i.e., no Zookeeper.

### Elastic

- New nodes can be started up in seconds, join the cluster and receive new
data.
- No need to assign data partitions
- Scale down easily.

### Kubernetes friendly

- Basic setup using StatefulSets.
- Volume setup, scaling, rolling upgrades is managed by Kubernetes.


## TODO
- Multi region, Multicloud
- Instant failover
