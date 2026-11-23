---
title: Performance Tuning
rank: 12
---

# Performance Tuning

Sail is designed to provide high performance for data processing workloads. However, several factors can affect the performance of your Sail applications. In this guide, we discuss common performance tuning techniques and best practices to help you optimize your Sail applications.

::: info
Here, we refer to configuration options by their names, such as `execution.batch_size`, but you can typically set these options via environment variables, such as `SAIL_EXECUTION__BATCH_SIZE`.
Refer to the [Configuration Guide](../configuration/) for more information on how Sail configuration works in general.
:::

## Batch Size

`execution.batch_size` controls the number of rows processed in each batch during execution.
Increasing this value can improve performance by reducing the overhead of processing many small batches.
However, setting it too high may lead to potential out-of-memory errors.
Experiment with different batch sizes to find the optimal value for your workload.

## Task Regions and Slot Sharing

Sail divides execution into **stages**, with tasks processing the partitions of each stage.
A **task region** is a set of tasks across stages that are scheduled and retried together.
For example, stages connected by pipelined shuffle can form a task region.

A **task slot** is a logical representation of how tasks occupy worker resources.
Each worker has a fixed number of slots, controlled by `cluster.worker_task_slots`.
Slots track task assignments rather than isolate CPU or memory, so tasks on a worker share its resources.

Tasks from different stages in the same region can share a slot when they belong to the same **slot-sharing** group.
Tasks for different partitions of the same stage must use different slots.
For a pipelined shuffle region whose stages share a slot-sharing group, **the total number of slots required is therefore determined by the widest stage**, meaning the largest number of partitions in any of those stages, rather than the sum across stages.
The required slots must be available together to schedule the region.

## Parallelism and Task Slots

`execution.default_parallelism` determines the default number of partitions for physical operators.
The default value `0` uses the available CPU cores, which is a good default for local mode.
In cluster mode, physical planning happens on the driver, so this default uses the cores available to the driver and does not account for the size of the worker pool.
You should **set the parallelism explicitly for distributed execution**, and choose the number of task slots with the worker count in mind.

Too few slots can delay task scheduling or leave workers underutilized, while too many concurrent tasks can increase CPU and memory contention.
Fewer slots per worker may also require more workers for the same workload, increasing scheduling overhead.
If a task remains unassigned beyond `cluster.task_launch_timeout_secs` because sufficient slots are unavailable, it can fail with a "task scheduling timeout" error.

The **partition expansion factor** describes how the task count of the widest stage compares to the default parallelism.
For example, `UnionExec` concatenates the partitions of its inputs.
For a query whose widest stage contains a union of $k$ inputs with $p$ partitions each, the union produces $k \cdot p$ partitions, so the partition expansion factor is $k$ for the query.
A query without a union typically has a factor of $1$.

As a starting point, for default parallelism $p$, the number of workers $w$, and partition expansion factor $k$, provision at least $\lceil p \cdot k / w \rceil$ task slots per worker.
The appropriate number depends on the physical plan and slot sharing, so adjust it for your workload and available resources.

For example, the following settings use one partition per worker vCPU and keep a fixed number of workers.
Set `worker_count`, `worker_vcpu_count`, `factor`, and `max_idle_time_secs` for your deployment before applying these settings.

```bash
export SAIL_EXECUTION__DEFAULT_PARALLELISM="$((worker_count * worker_vcpu_count))"
export SAIL_CLUSTER__WORKER_INITIAL_COUNT="$worker_count"
export SAIL_CLUSTER__WORKER_MAX_COUNT="$worker_count"
export SAIL_CLUSTER__WORKER_TASK_SLOTS="$((worker_vcpu_count * factor))"
export SAIL_CLUSTER__WORKER_MAX_IDLE_TIME_SECS="$max_idle_time_secs"
```

With this choice of parallelism, **the estimate for task slots per worker depends on both the number of vCPUs per worker and the partition expansion factor**.
For a fixed number of workers, you can size the slots using the partition expansion factor of the widest query in your workload.
Partitions are distributed across workers, and each slot is a logical assignment that does not reserve a fixed share of CPU or memory.
Narrower stages can still distribute their partitions evenly across available workers.
Increasing the slot count alone therefore does not further restrict the worker resources available to each partition.

## Kubernetes Worker Resources

For Kubernetes deployments, use `kubernetes.worker_pod_template` to configure the resources available to each worker pod.
This option accepts a JSON string representing a Kubernetes `PodTemplateSpec`, which is merged into the generated worker pod spec.
Use it to set CPU and memory requests and limits and control node placement, so that workers receive the resources assumed by the parallelism settings.
See [Overriding the Default Pod Spec](../deployment/kubernetes.md#overriding-the-default-pod-spec) for more information.
