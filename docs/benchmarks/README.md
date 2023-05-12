# Benchmarking PolarStreams

We tested PolarStreams performance on different AWS instance types to provide
detailed information on what to expect in terms of throughput and latency for given hardware specs. The goal is not
to push the upper limits of PolarStreams as it's still in early development but to define the direction it's
heading in terms of performance.

The workload is designed to post 1 KiB messages containing JSON data. The message is composed by a large portion of
random data alongside dictionary values, numbers and UUIDs to try to represent real world data. PolarStreams uses
**compression by default** so the shape of the data matters as it will affect the amount of data flushed to disk.
PolarStreams **writes are durable** by sending data to two replicas and waiting for a replica acknowledgement before
responding to the client.

The tool used to benchmark PolarStreams is [available on GitHub][tool-repo] and it can be used to reproduce these results.

The [AWS instance types][instance-types] selected are both arm64 and amd64 instances with EBS-only storage. Using EBS
storage is common pattern and, even though it can be slower than attached NVMe SSDs, it's the most sensible option
when deploying a stateful service in the Cloud due to costs and durability guarantees:

- **t4g.micro**: One of the cheapest instances on AWS costing less than a cent per hour. 2 vCPU (burstable
with 10% baseline performance) and 1 GiB of memory. On demand cost is $0.0084 per hour.
- **c7g.medium**: 1 vCPU (Arm-based AWS Graviton3 processors) and 1 GiB of memory. On demand cost is $0.0361 per hour.
- **c6i.large**: 2 vCPU (Up to 3.5 GHz 3rd generation Intel Xeon Scalable processors) and 4 GiB of memory.
On demand cost is $0.085 per hour.
- **c6i.xlarge**: 4 vCPU (Up to 3.5 GHz 3rd generation Intel Xeon Scalable processors) and 8 GiB of memory.
On demand cost is $0.17 per hour.

<p align="center">
    <img src="https://user-images.githubusercontent.com/2931196/232050558-8843aba2-5b4f-41b9-b593-daf000f3fd0d.png" alt="Throughput by instance type">
    <br>
    <em>Messages per second by instance</em>
</p>

The results show that PolarStreams can process more than one million messages per second (1 GiB/s) when writing on a
cluster composed of commodity instances with a single vCPU each. The max latency on all runs was under 20ms.

What we find specially interesting is that **PolarStreams can support writes of more than 84K msgs/s with baseline CPU
performance of `t4g.micro`, bursting up to 890K msgs/s**. This is also an example of what resource sharing might
look like when running PolarStreams on Kubernetes with a wide [requests-limits resource range][k8s-resource-mgmt].

The benchmarks benefit from [message packing made by the client][message-packing] when sending data without partition
key defined. To understand what should be the expected results when using partition keys (guaranteed order by key),
we ran a separate benchmark:

<p align="center">
    <img src="https://user-images.githubusercontent.com/2931196/232051794-0322e272-1dfe-480f-83cb-fb535312c3cc.png" alt="Throughput by instance type">
    <br>
    <em>Messages per second by instance (with partition key)</em>
</p>

With key defined results show that PolarStreams can process more than 250K messages per second on a
cluster composed of commodity instances with a single vCPU each. The max latency on these runs was under 50ms.

## Capacity planning compared to Apache Kafka

Using the benchmark results from above can help us do capacity planning for a production deployment to answer the
question what hardware will I need to support a certain volume of data.

To provide a good baseline, we compare the computing cost of running PolarStreams to Apache Kafka. We propose two scenarios:
1. Deploy Apache Kafka according to [Confluent system requirements][confluent-system] (only brokers, no costs for
Zookeeper, Connect, ...)
2. Deploy Apache Kafka with minimal hardware requirements.

For the first scenario, we use 3 `m5.4xlarge` brokers (64 GiB memory) and we state that Kafka can support
500K msgs/s with those instances with EBS storage, while keeping max latency under 100ms.
For the latter, we use `m5.xlarge` instances with 16 GiB of memory and define target throughput to 250K msgs/s with
predictable latency.

Rates are in USD and represent the yearly computing costs of running PolarStreams brokers compared to
Apache Kafka brokers.

<p align="center">
    <img src="https://user-images.githubusercontent.com/2931196/197513895-5b03fdde-2906-4c27-b90c-c7359ca2b786.png" style="margin: 0 auto">
    <br>
    <em>Cost of running PolarStreams and Kafka for a target throughput based on Confluent system requirements</em>
</p>

<p>&nbsp;</p>

<p align="center">
    <img src="https://user-images.githubusercontent.com/2931196/197513893-6bb866ef-c4e1-4568-a9e5-b277ee419265.png" style="margin: 0 auto">
    <br>
    <em>Cost of running PolarStreams and Kafka for a target throughput (minimal H/W)</em>
</p>

## Reproducibility

The tool used to benchmark PolarStreams is [available on GitHub][tool-repo] and it can be used to reproduce these results with
the following parameters:

- PolarStreams Commit Hash: [9c3687b](https://github.com/polarstreams/polar/commit/9c3687b0f83cb416ee84f23504df6ea63cb14b5f)
(`v0.7.0`).
- Tool parameters: `-w binary -c 6 -n 2000000 -m 1024 -ch 1`

There are also [terraform files available in the repository][terraform-files] to easily deploy the necessary resources
on AWS.

## Further reading

If you are interested in learning more about how PolarStreams achieves these throughput rates with consistently
low latencies, you can read our [I/O Documentation][io-docs].

[instance-types]: https://aws.amazon.com/ec2/instance-types/
[tool-repo]: https://github.com/jorgebay/polar-benchmark-tool/
[terraform-files]: https://github.com/jorgebay/polar-benchmark-tool/tree/main/terraform
[confluent-system]: https://docs.confluent.io/platform/current/installation/system-requirements.html#confluent-system-requirements
[k8s-resource-mgmt]: https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
[io-docs]: ../features/io/
[message-packing]: https://github.com/polarstreams/go-client/pull/28
