---
title: Kubernetes
rank: 2
---

# Kubernetes

::: info

LakeSail offers flexible enterprise support options, including managing Sail on Kubernetes.

[Get in touch](https://lakesail.com/support/) to learn more.

:::

Sail supports distributed data processing on Kubernetes clusters.
This guide demonstrates how to deploy Sail on a Kubernetes cluster and connect to it via Spark Connect.

## Building the Docker Image

We first need to build the Docker image for Sail. Please refer to the [Docker Images Guide](/guide/deployment/docker-images/) for more information.

## Loading the Docker Image

You will need to make the Docker image available to your Kubernetes cluster.
The exact steps depend on your Kubernetes environment.
For example, you may want to push the Docker image to your private Docker registry accessible from the Kubernetes cluster.
If you are using a local Kubernetes cluster, you may want to load the Docker image into the cluster. The command varies depending on the Kubernetes distribution you are using.

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

```bash [OrbStack]
# The Kubernetes cluster in OrbStack can use host Docker images directly.
```

:::

The following sections use [kind](https://kind.sigs.k8s.io/) as an example, but you can run Sail in other Kubernetes distributions of your choice.
Run the following commands to create a local Kubernetes cluster using kind and load the Docker image into the cluster.

```bash
kind create cluster
kind load docker-image sail:latest
```

## Running the Sail Server

::: info

The way to configure Sail applications is not stable yet.
Please refer to the [Changelog](/reference/changelog/) if the configuration ever changes in future releases.

:::

Create a file named `sail.yaml` with the following content.

::: code-group

<<< ../../../k8s/sail.yaml

:::

Then create the Kubernetes resources using the following command.
The Sail Spark Connect server runs as a Kubernetes deployment, and the gRPC port is exposed as a Kubernetes service.

```bash
kubectl apply -f sail.yaml
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

## Cleaning Up

Run the following command to clean up the Kubernetes resources for the Sail server.
All Sail worker pods will be terminated automatically as well.

```bash
kubectl delete -f sail.yaml
```

You can delete the kind cluster using the following command.

```bash
kind delete cluster
```
