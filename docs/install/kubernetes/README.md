# Installing PolarStreams on Kubernetes

There are multiple methods that can be used to install PolarStreams on Kubernetes:

* [Kustomize](https://kustomize.io)
* [Helm](https://helm.sh)

## Kustomize

Use the following steps to deploy PolarStreams on Kubernetes using Kustomize.

### Define PolarStreams's namespace

We recommend running PolarStreams in its own Kubernetes namespace. In the instructions here we’ll use `streams` as a namespace
but you’re free to choose your own.

```shell
kubectl create namespace --dry-run=client -o yaml streams > namespace.yaml
```

#### Prepare your kustomization file

This example configuration file deploys PolarStreams as a cluster with 3 replicas.

```shell
cat <<-'KUSTOMIZATION' > kustomization.yaml
---
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

# Override the defautl namespace.
namespace: streams

bases:
  # Include PolarStreams recommended base.
  - github.com/polarstreams/polar/deploy/kubernetes

images:
  # Override the image tag to pin the version used.
  - name: polarstreams/polar
    newTag: latest

resources:
  # The namespace previously created to keep the resources in.
  - namespace.yaml
KUSTOMIZATION
```

#### Verify your kustomization file

```shell
kubectl kustomize
```

#### Install PolarStreams

```shell
kubectl apply -k .
```

Kubernetes command line tool should create the Namespace, StatefulSet and other resources. You can checkout PolarStreams
logs of a broker by using `kubectl logs -n streams statefulset/polar`.

## Helm

Use the following steps to deploy PolarStreams on Kubernetes using Helm.

### Installing the Helm CLI

Follow the [Helm documentation](https://helm.sh/docs/intro/install) to install the Helm CLI to your machine.

### Add the Polar Chart Repository

Add the Polar chart repository

```shell
helm repo add polar https://polarstreams.github.io/polar/
```

### Install the Polar Helm Chart

Install the Polar Helm Chart to deploy Polar to a namespace called `streams`:

```shell
helm upgrade -i polar polar/polar -n streams --create-namespace
```

Once the chart has been installed successfully, check the logs of a broker by using `kubectl logs -n streams statefulset/polar`.

### Install the Polar Helm Chart (OCI Registry)

The Polar Helm chart is also available from an OCI registry. Execute the command to install the chart to a namespace called `streams`.

```shell
helm upgrade -i <release_name> oci://ghcr.io/polarstreams/polar/polar --version=<version>
```
