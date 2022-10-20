# Installing Barco Streams

## Installing on Kubernetes

You can install Barco on Kubernetes using `kubectl` by using [our kustomize base](./deploy/kubernetes/).

Follow [our guide to install on K8s](./KUBERNETES.md).

## Installing with Docker Compose for Application Development

You can use docker / docker compose to run Barco for application development and CI.

Read [this guide to run it using docker compose](./DOCKER_COMPOSE.md).

## Installing locally for Application Development

Barco expects the cluster to be composed by multiple brokers. To simplify application development, we introduced
"dev mode" setting to run a single-broker cluster for application development.

After [building](../../#build) Barco, set the `BARCO_DEV_MODE` environment variable before running:

```bash
BARCO_DEV_MODE=true BARCO_HOME=./barco-data go run .
```