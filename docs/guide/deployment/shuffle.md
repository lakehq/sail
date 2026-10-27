---
title: Shuffle Backends
rank: 5
---

# Shuffle Backends

Sail supports various shuffle backends for distributed data processing.

The shuffle backend can be configured via the `cluster.shuffle_backend.*` options. The examples on this page show how to set these configuration options using environment variables.
The server reads these options when it starts, and every session on the server uses the same shuffle backend.

Sail runs the tasks of a job in **task regions**.
A task region contains the stages that exchange data through pipelined streams, and Sail assigns all tasks of a region at the same time.
If a task fails, Sail cancels the other tasks in its region and runs the whole region again, up to `cluster.task_max_attempts` attempts.
The job fails without running the region again if an Iceberg REST catalog commit has an unknown outcome, because the commit may have succeeded.
A commit has an unknown outcome when the catalog returns status 500, 502, 503, or 504, or when its response is lost or cannot be parsed.
If `cluster.worker_max_count` is positive and the maximum number of workers cannot provide enough task slots for a region, the job fails without waiting for workers.

## Arrow Flight

The default shuffle backend is `flight`, which uses the Arrow Flight gRPC protocol for data exchange between tasks. This is equivalent to setting the following environment variable explicitly.

```bash
export SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE=flight
```

The `flight` shuffle backend implies _pipelined shuffle_, which means that shuffle data is streamed directly from map tasks to reduce tasks without intermediate storage. This is recommended for most use cases because it does not incur shuffle data persistence overhead. However, pipelined shuffle requires sufficient resources to run all tasks simultaneously. It also means that the entire job must be retried if any task fails, which may be less desirable for large-scale jobs and environments with high failure rates.

`cluster.shuffle_backend.flight.compression` sets the codec that Sail uses to compress shuffle data in Flight streams.
It defaults to `zstd` and also accepts `lz4` and `none`.
Compression uses CPU to reduce shuffle network traffic.

The following options tune the HTTP/2 connections that each Flight client opens to a peer.

| Option                                                          | Default | Effect                                  |
| --------------------------------------------------------------- | ------- | --------------------------------------- |
| `cluster.shuffle_backend.flight.connection_count`               | `1`     | Connections opened to each peer         |
| `cluster.shuffle_backend.flight.initial_stream_window_size`     | `0`     | Receive window per stream, in bytes     |
| `cluster.shuffle_backend.flight.initial_connection_window_size` | `0`     | Receive window per connection, in bytes |

`connection_count` must be positive, and the client sends requests over the connections in round-robin order.
More connections can help when many streams share the flow-control window of one connection.
A window size of `0` keeps the transport default, and the maximum for either window is `2147483647`.
A larger window lets more data be in flight, which uses more worker memory.
Measure shuffle throughput before and after you change it.

The compression and connection options are experimental, and they apply only when the shuffle backend is `flight`.
The other backends also stream data between some stages without storage, and those streams are uncompressed.

## Storage

To improve the resilience of shuffle-heavy, large-scale jobs, you can use the `storage` shuffle backend. This implies _blocking shuffle_, where map tasks write shuffle data to a storage backend before reduce tasks are launched to read it. This allows a job to have multiple task regions, so stages in different regions can be retried independently. You also need fewer workers to run the job because not all tasks must run simultaneously.

```bash
export SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE=storage
export SAIL_CLUSTER__SHUFFLE_BACKEND__STORAGE__PATH="s3://sail/shuffle"
```

At each shuffle, Sail adds a stage that merges the shuffle data of each output partition.
The stage writes the data to the storage path as Arrow IPC files, which are compressed with `zstd` by default.
A failed task region can read the files of a completed region again.
If those files are lost, the job fails, because Sail does not run a completed region again.

The server and every worker that reads or writes shuffle data need access to the same storage path.
Sail removes shuffle data when stages and jobs finish.
Set a lifecycle rule for the storage path to remove the files that an interrupted cleanup leaves behind.

## Apache Celeborn

[Apache Celeborn](https://celeborn.apache.org/) is a distributed shuffle service that provides high-performance shuffle data management. Sail supports Celeborn as a shuffle backend, which also implies _blocking shuffle_.

```bash
export SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE=celeborn
export SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__MASTER_ENDPOINTS='["127.0.0.1:12097"]'
```

You can also specify multiple master endpoints in the array if your Celeborn cluster supports high availability.
Sail uses one endpoint at a time and moves to the next one after a connection, timeout, or protocol failure.
Celeborn master high availability is configured in Celeborn itself.

Sail implements a native Celeborn client that communicates with the Celeborn master and workers.
The master advertises worker endpoints when the shuffle is registered. Worker hosts must be on the same network as Sail so that Sail workers can connect to Celeborn workers to read and write shuffle data.
The Sail server registers shuffles with the Celeborn masters and sends slot reservation and commit requests to the Celeborn workers.
The server therefore also needs network access to them.
Refer to the [Security Baseline](./security-baseline.md#internal-traffic) page for how to restrict this traffic.

Each Spark Connect session registers with Celeborn as the application `sail-session-<session-id>`.
Sail uses its own `cluster.shuffle_backend.celeborn.*` options, and the Celeborn Spark plugin settings do not configure this client.

### Client Limits

The native client has the following limits.

- RPC, push, and fetch use plaintext connections.
  The client supports neither Celeborn TLS nor SASL, and `cluster.enable_tls` does not change this.
- The client does not request replicated shuffle data.
- Fetch has no retry or replica fallback.
  If a Celeborn worker that holds committed shuffle data is lost, the downstream tasks cannot read that data, and the job fails.
  To remove a Celeborn worker on purpose, use the Celeborn [decommissioning process](https://celeborn.apache.org/docs/0.6.3/decommissioning/) instead of deleting a worker that holds shuffle data.
- Each request to a master or worker has a 30-second I/O timeout.
- Sail registers every session with the fixed Celeborn user identifier `default`/`default`.
  Celeborn cannot tell Sail sessions apart by user.

Celeborn expires the registration of an application that stops sending heartbeats.
If Sail stops abruptly, it cannot unregister its shuffles.
Keep the expiry and cleanup settings of Celeborn enabled.

You can refer to the [Configuration](/reference/configuration/) reference page for advanced options for shuffle backends.
