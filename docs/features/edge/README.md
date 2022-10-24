# Built for Edge Computing

There's an increasing demand for running computing resources close to end user, whether is at the boundary of
operator networks, in an AWS Local Zone or an on-premise edge infrastructure. The edge has great potential for
running latency-sensitive applications with limited hardware resources.

Barco Streams is the perfect solution for event streaming on the Edge for acting as a safe buffer between edge
services and the public cloud, storing events that are produced on remote facilities that can be consumed by
components in the cloud.

These are the Barco features that make it a great choice for Edge Computing deployments:

- [High throughput and predictable latency][benchmarks] even on resource-constrained deployments.
- [REST API][rest-api]: Barco provides simple HTTP API endpoints that expose all the functionality to produce and
consume events without requiring additional drivers or client libraries.
- Support for Production workloads with less than vCPU and a limited memory footprint starting at 0.5 GiB.
- Kubernetes native: Whether it's with [KubeEdge][kubeedge], Kubernetes on bare metal or EKS on AWS Outposts,
Kubernetes is rising to be the default control plane for new iterations of Edge deployments. Barco is specifically
designed to run on Kubernetes and support K8s deployment lifecycle seamlessly.


[benchmarks]: ../../benchmarks/
[rest-api]: ../../rest_api/
[kubeedge]: https://kubeedge.io/en/
