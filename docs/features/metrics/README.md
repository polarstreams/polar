# Metrics

PolarStreams records and exposes internal metrics that can be relevant for monitoring and alerting, such as the amount
of bytes produced or the number of active consumers.

A built-in [Prometheus][prometheus] endpoint is exposed on port `9253` by default and metrics can be collected using any
Prometheus server of your choice, without the need of an exporter.

## Prometheus target discovery on Kubernetes

Prometheus implements service discovery within Kubernetes, automatically scraping Kubernetes resources that define
a set of annotations.

[PolarStreams's kustomize base][kustomize-base] already defines those annotations for the pods created:

```yaml
  annotations:
    prometheus.io/port: "9253"
    prometheus.io/scrape: "true"
```

There's no need to modify those settings on your deployment.

[prometheus]: https://prometheus.io/
[kustomize-base]: https://github.com/polarstreams/polar/tree/main/deploy/kubernetes/
