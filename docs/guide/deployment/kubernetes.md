---
title: Kubernetes
rank: 1
---

# Kubernetes

Sail supports distributed data processing on Kubernetes clusters.
This guide demonstrates how to deploy Sail on a Kubernetes cluster and connect to it via Spark Connect.

## Building the Docker Image

We first need to build the Docker image for Sail. In an empty directory, create a `Dockerfile` with the following content.

::: code-group

<<< ../../../docker/release/Dockerfile{docker}

:::

Then run the following command to build the Docker image.

```bash
docker build -t sail:latest .
```

::: details Building the Docker Image with the Standalone Binary

You can also create a Docker image containing the standalone Sail binary built from source. This is useful for development purposes.
To do this, checkout the Sail [repository](https://github.com/lakehq/sail) and run the following command from the root of the project directory.

```bash
docker build -t sail:latest -f docker/dev/Dockerfile .
```

:::

You will then need to make the Docker image available to your Kubernetes cluster.
The exact steps depend on your Kubernetes environment.
For example, you may want to push the Docker image to your private Docker registry accessible from the Kubernetes cluster.
If you are using a local Kubernetes cluster, you may want to load the Docker image into the cluster. The command varies depending on the Kubernetes distribution you are using.

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
