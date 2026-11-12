---
title: Architecture
rank: 1
---

# Architecture

On Kubernetes, one Sail Spark Connect server pod hosts every Spark Connect session, and each session launches its own worker pods.
Refer to the [Architecture](/concepts/architecture/) page for the execution model, including the cluster job runner, driver, stages, and tasks.

## Processes

In Kubernetes cluster mode, each session has its own driver and worker manager inside the server process.
Configuration keys call the server the driver, as in `kubernetes.driver_pod_name` and `cluster.driver_*`, because the driver of every session runs in it.
Worker pods run `sail worker`.

```text
Spark Connect clients
  -> ClusterIP Service on port 50051
  -> Sail Spark Connect server pod
       -> session manager
            -> session A: driver, worker manager, worker pods
            -> session B: driver, worker manager, worker pods
```

The provided Deployment has one replica and uses the `Recreate` strategy.

## Workers

When a session starts, its worker manager launches `cluster.worker_initial_count` worker pods, up to `cluster.worker_max_count`.
The engine default of `cluster.worker_initial_count` is `4`.
With the base value of `0`, worker pods start only when a job needs them.
The worker manager launches more worker pods as jobs need task slots, and the session reuses them across jobs.
See [Cluster Configuration and Sizing](./configuration.md#workers) for worker limits and idle shutdown.
See [Worker Pod Template](./configuration.md#worker-pod-template) for how Sail builds each worker pod.

## Clients

Spark Connect clients send unresolved logical plans and receive results encoded in Arrow.
The client runs outside the server process.
It provides no driver JVM, `SparkContext`, or RDD APIs.
The Sail server and workers read and write the data.
They need access to the storage and catalogs that a query uses, and the client does not.

Sail does not support uploading artifacts from the client.
`AddArtifacts` and `ArtifactStatus` requests return an error.
The Python packages that your UDFs import must already be installed in the image of the server and the workers.

The server accepts gRPC messages of up to 128 MiB.
For reattachable operations, the server keeps the last 128 response messages so that the client can resume the result stream.

The server selects its Spark compatibility profile from the PySpark version installed in its own environment.
Any 3.x version selects the Spark 3.5 profile, and versions 4.0.x, 4.1.x, and 4.2.x have their own profiles.
For any other version, session creation fails.
If the server cannot import PySpark, it uses the Spark 3.5 profile.
The Sail Python package requires Python 3.10 through 3.14.
See [Checkpoint Storage](./checkpoint.md) for the supported checkpoint calls.

## Network

The `ClusterIP` Service exposes port `50051`.
See [Security Baseline](./security-baseline.md#network-policy) for the NetworkPolicy, authentication, and Kubernetes API permissions.

## Failures

| Failure      | What happens                                                                                                                                                            |
| ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Client loss  | The session stays until the client releases it or `spark.session_timeout_secs` passes without a client request                                                          |
| Worker loss  | Every task in the affected task region runs again, up to `cluster.task_max_attempts` attempts. The job can fail before Sail detects the loss                            |
| Shuffle loss | With `flight`, the region that produced the data runs again, as for worker loss. With `storage` or `celeborn`, Sail does not rerun a completed region, so the job fails |
| Server loss  | Every session and active query in that process ends                                                                                                                     |
| Node loss    | Depends on whether the server or only session workers ran there                                                                                                         |

Sail detects a lost worker when it receives no heartbeat from the worker for `cluster.worker_heartbeat_timeout_secs`, which defaults to `120`.
Until then, Sail can assign tasks to the lost worker, including the tasks of a region that runs again.
Sail runs a failed region again without delay.
If the attempts run out before Sail detects the loss, the job fails.

A replacement server pod starts with no sessions.
Checkpoint files and stored shuffle data do not restore a session.
Sail does not run the queries of a lost server again.
To run them again, the client or an external controller must submit them to the new server.

## One Server per Workload

Run a separate server when a workload needs its own configuration or restart schedule, or must fail independently of other workloads.
