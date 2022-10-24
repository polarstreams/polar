# Modern I/O Techniques

Internally, Barco uses a series of I/O-related techniques that make it both fast and lightweight, supporting high
throughput and consistently low latency while keeping compute and memory resource utilization low.

## Direct I/O

Barco uses [Direct I/O][direct-io]. Direct I/O is a feature of the file system in which file reads and writes go
directly from an application to the storage device, bypassing the Kernel page cache.

Bypassing the page cache in our case allow us to use buffering and flush strategies tuned to the workload when writing
and use read-ahead strategies when retrieving data.

Another benefit of using Direct I/O is process isolation. The Linux page cache is a shared resource, a process has to
compete with other unrelated processes to access it even when running in a containerized environment. In Kubernetes,
this translates into a stable performance and being able to control exactly the amount of memory dedicated for
buffers and caching (K8s working set).

## Write batching and parallel processing

When writing to disk, Barco coalesces multiple events into compressed and [checksummed][checksum] data fragments,
called chunks. A chunk gets built in memory as new events are arranged to be written in a topic. When a chunk is
ready to be written to disk we schedule a write to disk and, instead of waiting idle for the storage device to
acknowledge the write, the coalescer continues processing the following chunk.

<p align="center">
    <img src="https://user-images.githubusercontent.com/2931196/197501755-51767a3b-34c3-43df-89e0-22626377853b.png" alt="Coalescer writing">
    <br>
    <em>The coalescer keeps processing the following chunk, while the previous chunk is being written to disk.</em>
</p>

## Compressed chunks for replication and consumers

The Barco broker acting as a leader of a partition is the one responsible for compressing and checksumming the
data in the log segment. A chunk is effectively only compressed once and sent to the replicas as a compressed payload.
The replica is only responsible for appending the chunk and acknowledge it. Minimizing CPU utilization on a replica and
significantly reducing network traffic.

Additionally, when consuming these chunks can be sent straight to the client without processing it on the broker side.

[checksum]: https://en.wikipedia.org/wiki/Checksum
[direct-io]: https://man7.org/linux/man-pages/man2/open.2.html#:~:text=O_DIRECT

## Benchmarks

Learn more about how applying these techniques leads to high throughput and consistently low latencies in [our
benchmarks](../../benchmarks/).
