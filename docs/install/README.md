# Installing PolarStreams

## Installing on Kubernetes

You can install PolarStreams on Kubernetes using `kubectl` by using [our kustomize base](../deploy/kubernetes/) or using our [Helm Chart](../../charts/polar).

Follow [our guide to install on K8s](./kubernetes/).

## Installing on Docker Compose for Application Development

You can use docker / docker compose to run PolarStreams for application development and CI.

Read [this guide to run it using docker compose](./docker_compose/).

## Installing locally for Application Development

PolarStreams expects the cluster to be composed by multiple brokers. To simplify application development, we introduced
"dev mode" setting to run a single-broker cluster for application development.

After [building](../../#build) PolarStreams, set the `POLAR_DEV_MODE` environment variable before running:

```bash
POLAR_DEV_MODE=true POLAR_HOME=./polar-data go run .
```
