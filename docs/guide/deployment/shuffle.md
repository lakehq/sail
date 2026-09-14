---
title: Shuffle Backends
rank: 3
---

# Shuffle Backends

Sail supports various shuffle backends for distributed data processing.

The shuffle backend can be configured via the `cluster.shuffle_backend.*` options. The examples on this page show how to set these configuration options using environment variables.

## Arrow Flight

The default shuffle backend is `flight`, which uses the Arrow Flight gRPC protocol for data exchange between tasks. This is equivalent to setting the following environment variable explicitly.

```bash
export SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE=flight
```

The `flight` shuffle backend implies _pipelined shuffle_, which means that shuffle data is streamed directly from map tasks to reduce tasks without intermediate storage. This is recommended for most use cases because it does not incur shuffle data persistence overhead. However, pipelined shuffle requires sufficient resources to run all tasks simultaneously. It also means that the entire job must be retried if any task fails, which may be less desirable for large-scale jobs and environments with high failure rates.

## Storage

To improve the resilience of shuffle-heavy, large-scale jobs, you can use the `storage` shuffle backend. This implies _blocking shuffle_, where map tasks write shuffle data to a storage backend before reduce tasks are launched to read it. This allows a job to have multiple task regions, so stages in different regions can be retried independently. You also need fewer workers to run the job because not all tasks must run simultaneously.

```bash
export SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE=storage
export SAIL_CLUSTER__SHUFFLE_BACKEND__STORAGE__PATH="s3://sail/shuffle"
```

## Apache Celeborn

[Apache Celeborn](https://celeborn.apache.org/) is a distributed shuffle service that provides high-performance shuffle data management. Sail supports Celeborn as a shuffle backend, which also implies _blocking shuffle_.

```bash
export SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE=celeborn
export SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__MASTER_ENDPOINTS='["127.0.0.1:12097"]'
```

You can also specify multiple master endpoints in the array if your Celeborn cluster supports high availability.

Sail implements a native Celeborn client that communicates with the Celeborn master and workers.
The master advertises worker endpoints when the shuffle is registered. Worker hosts must be on the same network as Sail so that Sail workers can connect to Celeborn workers to read and write shuffle data.

You can refer to the [Configuration](/reference/configuration/) reference page for advanced options for shuffle backends.
