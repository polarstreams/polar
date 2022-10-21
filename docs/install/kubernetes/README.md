### Installing Barco Streams on Kubernetes

You can use `kubectl` to install Barco on Kubernetes.

#### Define Barco's namespace

We recommend running Barco in its own Kubernetes namespace. In the instructions here we’ll use `streams` as a namespace
but you’re free to choose your own.

```shell
kubectl create namespace --dry-run=client -o yaml streams > namespace.yaml
```

#### Prepare your kustomization file

This example configuration file deploys Barco as a cluster with 3 replicas.

```shell
cat <<-'KUSTOMIZATION' > kustomization.yaml
---
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

# Override the defautl namespace.
namespace: streams

bases:
  # Include Barco Streams recommended base.
  - github.com/barcostreams/barco/deploy/kubernetes

images:
  # Override the image tag to pin the version used.
  - name: barcostreams/barco
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

#### Install Barco

```shell
kubectl apply -k .
```

Kubernetes command line tool should create the Namespace, StatefulSet and other resources. You can checkout Barco
logs of a broker by using `kubectl logs -n streams statefulset/barco`.
