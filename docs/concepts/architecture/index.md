---
title: Architecture
rank: 1
---

# Architecture

Sail is designed to be a high-performance, user-friendly compute engine that supports both local and cluster modes.
The **local mode** is designed for ad-hoc data analysis and development, while the **cluster mode** supports distributed data processing at scale.
The architecture of Sail allows for smooth transitions between these modes, enabling users to scale their workloads from laptops to clusters with minimal friction.

Sail serves as a Spark Connect server that maintains a bidirectional gRPC connection with the PySpark client.
The PySpark client submits execution requests and receives execution results through this connection.

The Sail server performs semantic analysis on the submitted execution requests and generates an optimized physical execution plan, which is discussed in more detail on the [Query Planning](../query-planning) page.
The physical plan is then executed in different ways depending on the mode.

## Local Mode

<SvgDiagram :svg="data['architecture-local.dot']" />

In local mode, Sail runs as a single process.
Each Spark session is powered by a **local job runner** responsible for executing the optimized physical plan.
The local job runner can use multiple threads to process data partitions in parallel, leveraging the available CPU cores on the host.

## Cluster Mode

<SvgDiagram :svg="data['architecture-cluster.dot']" />

In cluster mode, Sail forms a distributed system involving the Sail server and multiple Sail **workers**.
The Sail server in cluster mode can support multiple Spark sessions, each powered by a **cluster job runner**. Each cluster job runner owns a **driver** that schedules the distributed physical plan containing multiple **stages**. Each stage can have multiple **tasks**, each processing a partition of data. The tasks are sent to the workers for execution.

Sail operates with a separation of concerns between the **control plane** and the **data plane**.

For the control plane, the internal Sail gRPC protocol is used for communication between the driver and workers.
The driver and workers act as both gRPC servers and clients.
Note that the Sail workers do not communicate with each other in the control plane.
The **actor model** forms the backbone of the control plane, offering a concurrency model that ensures state is safely managed without locks.

For the data plane, the Arrow Flight gRPC protocol is used for exchanging shuffle data among the workers and returning results from the workers to the driver.

Here is a high-level overview of the execution flow in cluster mode.

1. The PySpark client submits an execution request to the Sail server.
2. The driver actor launches worker containers using the **worker manager**. The worker container creates the worker actor.
3. The worker actor registers the worker with the driver actor.
4. The driver actor sends tasks to the worker for execution.
5. The worker actor executes the tasks, where data shuffling occurs across stages.
6. The worker actor reports the task status to the driver actor.
7. The driver actor collects results from tasks belonging to the final stage.
8. The driver actor returns results to the PySpark client.

<script setup lang="ts">
import SvgDiagram from "@theme/components/SvgDiagram.vue";
import { data } from "./index.data.ts";
</script>
