---
title: Kubernetes
rank: 2
---

# Kubernetes

<!--@include: ./_common/support.md-->

Sail supports distributed data processing on Kubernetes clusters.
This guide demonstrates how to deploy Sail on a Kubernetes cluster and connect to it via Spark Connect.

## Building the Docker Image

We first need to build the Docker image for Sail. Please refer to
the [Docker Images Guide](/guide/deployment/docker-images/) for more information.

## Loading the Docker Image

You will need to make the Docker image available to your Kubernetes cluster.
The exact steps depend on your Kubernetes environment.
For example, you may want to push the Docker image to your private Docker registry accessible from the Kubernetes
cluster.

If you are using a local Kubernetes cluster, you may need to load the Docker image into the cluster. The command varies
depending on the Kubernetes distribution you are using.
Here are the examples for some well-known Kubernetes distributions.
You can refer to their documentation for more details.

::: code-group

```bash [kind]
kind load docker-image sail:latest
```

```bash [minikube]
minikube image load sail:latest
```

```bash [k3d]
k3d image import sail:latest
```

```bash [k3s]
docker save sail:latest | k3s ctr images import -
```

```bash [MicroK8s]
docker save sail:latest | microk8s images import
```

:::

The following sections use [kind](https://kind.sigs.k8s.io/) as an example, but you can run Sail in other Kubernetes
distributions of your choice.
Run the following command to create a local Kubernetes cluster.

```bash
kind create cluster
```

Alternatively, you can create a kind cluster with a custom configuration.
Here is an example.

```bash
kind create cluster --config k8s/kind-config.yaml
```

::: code-group

<<< ../../../k8s/kind-config.yaml [k8s/kind-config.yaml]

:::

Then load the Docker image into the cluster.

```bash
kind load docker-image sail:latest
```

## Running the Sail Server

::: info

The way to configure Sail applications is not stable yet.
Please refer to the [Changelog](/reference/changelog/) if the configuration ever changes in future releases.

:::

Create a file `k8s/sail.yaml` with the following content.

::: code-group

<<< ../../../k8s/sail.yaml [k8s/sail.yaml]

:::

Create the Kubernetes resources using the following command.
The Sail Spark Connect server runs as a Kubernetes deployment, and the gRPC port is exposed as a Kubernetes service.

```bash
kubectl apply -f k8s/sail.yaml
```

## Overriding the Default Pod Spec

By default, the worker pod spec is created programmatically. If the `kubernetes.worker_pod_template` configuration option is provided, it is merged into
the generated spec, allowing you to customize the worker pods.

### Using a Custom Pod Template

You can use Kustomize to declaratively define the Kubernetes resources. This approach makes it easier to manage
overrides and reuse manifests across environments. For example, you can specify custom volumes in your overlay
with the following content.

::: code-group

<<< ../../../k8s/kustomization.yaml [k8s/kustomization.yaml]

:::

::: code-group

<<< ../../../k8s/test-volume-patch.yaml [k8s/test-volume-patch.yaml]

:::

You can then apply the customized manifests using the following command.

```bash
kubectl apply -k k8s/
```

## Running Spark Jobs

To connect to the Sail Spark Connect server, you can forward the service port to your local machine.

```bash
kubectl -n sail port-forward service/sail-spark-server 50051:50051
```

Now you can run Spark jobs using the standalone Spark client.
Here is an example of running PySpark shell powered by Sail on Kubernetes.
Sail worker pods are launched on-demand to execute Spark jobs.
The worker pods are terminated after a certain period of inactivity.

```bash
env SPARK_CONNECT_MODE_ENABLED=1 SPARK_REMOTE="sc://localhost:50051" pyspark
```

## Running Python Tests

When running tests with pytest, the worker pods may need a writable volume for temporary files. To handle this, define a
custom pod template (such as the `k8s/test-volume-patch.yaml` file shown above) that mounts a temporary volume into the pod.

Once the pod template is in place, you can run pytest locally against the cluster with the following command.

```bash
env SPARK_REMOTE="sc://localhost:50051" PYTEST_DEBUG_TEMPROOT="/tmp/sail" pytest --pyargs pysail
```

## Cleaning Up

Run the following command to clean up the Kubernetes resources for the Sail server.
All Sail worker pods will be terminated automatically as well.

```bash
kubectl delete -f k8s/sail.yaml
```

If you used Kustomize to deploy the Sail server, you can clean up the resources with the following command.

```bash
kubectl delete -k k8s/
```

You can delete the kind cluster using the following command.

```bash
kind delete cluster
```
