---
title: Cluster Configuration and Sizing
rank: 4
---

# Cluster Configuration and Sizing

The server loads Sail configuration from its environment when it starts, and applies it to every session it hosts.
Refer to the [Configuration](/guide/configuration/) guide for how option names map to environment variables.
The [configuration reference](/reference/configuration/) lists every option with its default.
See [Performance Tuning](/guide/performance/) for settings that affect query performance in any mode, such as memory pools, joins, and Parquet options.

## Example Configuration

The values in the Kustomize base are sized for a small cluster.

| Setting                             | Engine default          | Server base | Worker template |
| ----------------------------------- | ----------------------- | ----------- | --------------- |
| `cluster.worker_max_count`          | `0`, no ceiling         | `4`         | Not set         |
| `cluster.worker_task_slots`         | `8`                     | `2`         | Not set         |
| `cluster.worker_initial_count`      | `4`                     | `0`         | Not set         |
| `execution.default_parallelism`     | `0`, derive from driver | `8`         | Not set         |
| `runtime.memory_pool.type`          | `unbounded`             | `fair`      | `fair`          |
| `runtime.memory_pool.fair.max_size` | 64 GiB                  | 1.5 GiB     | 3 GiB           |
| `runtime.temporary_files.paths`     | `[]`, OS temp directory | `/tmp/sail` | `/tmp/sail`     |
| `runtime.temporary_files.max_size`  | 64 GiB                  | 4 GiB       | 10 GiB          |

## Workers

| Setting                             | Effect                                     |
| ----------------------------------- | ------------------------------------------ |
| `cluster.worker_max_count`          | Maximum number of workers for each session |
| `cluster.worker_task_slots`         | Logical task slots on each worker          |
| `cluster.worker_max_idle_time_secs` | Idle time before Sail stops a worker       |
| `execution.default_parallelism`     | Default number of partitions               |

The default `cluster.worker_max_count` of `0` sets no limit.
Set a positive value, and use a Kubernetes resource quota to limit the total for the namespace.
A session fails to start when `cluster.worker_task_slots` is `0`.
Set `execution.default_parallelism` explicitly, because the default follows the CPU cores of the server.
See [Parallelism and Batching](/guide/performance/parallelism) for parallelism and task slots.

The session reuses workers across jobs.
Sail stops a worker that has no work for `cluster.worker_max_idle_time_secs`.
A stopping worker no longer counts toward `cluster.worker_max_count`, even before its process exits.
Sail deletes the pods of stopped workers when the session ends.
Until then, you can see more worker processes or pods than the maximum.

## Settings Passed to Workers

The server passes the following settings to each worker pod it creates.

- `cluster.enable_tls`
- `execution.batch_size`
- `cluster.worker_heartbeat_interval_secs`
- `cluster.task_stream_buffer`
- `cluster.task_stream_creation_timeout_secs`
- `cluster.rpc_retry_strategy`
- `cluster.shuffle_backend.type`, with the settings of the selected shuffle backend
- The identifiers and addresses that each worker needs to register with its driver
- The `RUST_LOG` environment variable, which is `info` if the server does not set it

Set every other worker setting in the container environment of the worker pod template.
This includes the memory pool, temporary files, telemetry, and storage credentials.

## Memory and Temporary Files

Set the maximum size of the worker memory pool below the memory limit of the container.
Leave room for the [memory outside the pool](/guide/performance/memory#memory-outside-the-pool).
Mount writable storage at every path in `runtime.temporary_files.paths`.
`runtime.temporary_files.max_size`, the `sizeLimit` of an `emptyDir` volume, and the ephemeral-storage limit of the pod are separate limits.
Check all three.

Size the server pod for the number of concurrent sessions.
Refer to the [Managing Memory](/guide/performance/memory#memory-pool) page for how the memory limits apply to each session.

## Pod Identity and Images

| Setting                                  | Default        | Purpose                              |
| ---------------------------------------- | -------------- | ------------------------------------ |
| `kubernetes.image`                       | `sail:latest`  | Image for worker pods                |
| `kubernetes.image_pull_policy`           | `IfNotPresent` | Pull policy for worker pods          |
| `kubernetes.namespace`                   | `default`      | Namespace for worker pods            |
| `kubernetes.driver_pod_name`             | empty          | Sets owner references on worker pods |
| `kubernetes.worker_pod_name_prefix`      | `sail-worker-` | Prefix for worker pod names          |
| `kubernetes.worker_service_account_name` | `default`      | ServiceAccount for worker pods       |

The base fills `kubernetes.namespace` and `kubernetes.driver_pod_name` from the downward API.
With a driver pod name, worker pods get an owner reference, and Kubernetes removes them when the server pod is deleted.
With an empty driver pod name, worker pods have no owner reference, and Kubernetes does not remove them with the server pod.
Sail looks up the driver pod in `kubernetes.namespace`.
If you set the driver pod name, `kubernetes.namespace` must be the namespace of the server, or worker pods fail to launch.

## Worker Pod Template

`kubernetes.worker_pod_template` is a JSON string holding a pod template.
The base supplies it through the ConfigMap in `deployments/production/base/worker-pod-template.yaml`.

The worker manager generates the image, command, environment, ServiceAccount, and `restartPolicy: Never` of each worker pod.
Sail then merges the `spec` of the template into that pod, field by field.
Template values can replace generated fields or add containers.
The container must be named `worker` to merge into the generated container.
Environment variables merge by name, and an entry with the same name replaces the field that it sets.
If the result has both `value` and `valueFrom` for one name, Kubernetes rejects the pod.
Do not set the image, command, generated `SAIL_CLUSTER__*` values, `SAIL_EXECUTION__BATCH_SIZE`, ServiceAccount, or restart policy in the template.

From the metadata of the template, Sail uses only the labels and annotations.
Generated labels win when the names collide, and Sail ignores the name, namespace, and owner references of the template.

## Shuffle and Checkpoints

See [Shuffle Backends](./shuffle.md) to set `SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE`.
See [Checkpoint Storage](./checkpoint.md) to set `SAIL_EXECUTION__CHECKPOINT__PATH` to a shared path.

## Storage and Catalogs

See [Security Baseline](./security-baseline.md#images-and-credentials) for how to deliver object store and catalog credentials to the server and each worker that reads or writes the data.

`catalog.list` defaults to a single in-memory catalog named `sail`, and the base keeps that default.
Each session gets its own copy of that catalog.
Its tables are visible only to the session that defines them, and they are lost when the session ends.
To share tables across sessions or keep them after a session ends, configure an external catalog.
See [Storage](/guide/storage/) and [Catalogs](/guide/catalog/) for the provider settings.

## Timeouts and Retries

| Setting                                     | Default | Meaning                                |
| ------------------------------------------- | ------- | -------------------------------------- |
| `cluster.worker_heartbeat_interval_secs`    | `10`    | How often a worker reports liveness    |
| `cluster.worker_heartbeat_timeout_secs`     | `120`   | Silence before a worker is lost        |
| `cluster.worker_launch_timeout_secs`        | `120`   | Waiting for a worker to register       |
| `cluster.task_launch_timeout_secs`          | `120`   | Waiting for a task to get a worker     |
| `cluster.task_stream_creation_timeout_secs` | `60`    | Setting up one distributed stream      |
| `cluster.task_max_attempts`                 | `3`     | Total attempts per task                |
| `spark.session_timeout_secs`                | `900`   | Spark Connect client inactivity        |
| `spark.execution_heartbeat_interval_secs`   | `15`    | Heartbeats on the client result stream |

Two retry schedules apply in cluster mode.

| Schedule                               | Default                                                 | Delays           |
| -------------------------------------- | ------------------------------------------------------- | ---------------- |
| `cluster.worker_launch_retry_strategy` | `exponential_backoff`, 5 retries, 1 s to 30 s, factor 2 | 1, 2, 4, 8, 16 s |
| `cluster.rpc_retry_strategy`           | `fixed`, 3 retries, 5 s                                 | 5, 5, 5 s        |

`cluster.task_max_attempts` counts all attempts.
The `max_count` of a retry schedule counts the retries after the first attempt.
Each worker launch attempt can also wait up to `cluster.worker_launch_timeout_secs`.
Workers use the RPC schedule to register with the driver and to report task status.
A worker that exhausts it shuts down.

See [Failures](./architecture.md#failures) for what happens when a worker or the server is lost.
See [Sessions and Timeouts](./operations.md#sessions-and-timeouts) for how the session timeout affects long result streams.
