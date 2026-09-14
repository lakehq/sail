---
title: Checkpoint Storage
rank: 4
---

# Checkpoint Storage

The Spark `DataFrame.checkpoint()` API allows you to store intermediate data for reuse. This is important for iterative processing, which is typically used in graph algorithms. Without checkpointing, the query plan can grow exponentially during iterations, and all computations are performed from scratch at every step.

Sail supports checkpointing in both local and cluster modes. Sail relies on external storage for checkpoint data.
For example, the storage can be a local file system for local query execution. In cluster mode, it can be a network file system accessible by all Sail workers or a cloud storage service such as Amazon S3.

The following example shows how to configure the checkpoint storage path via environment variables.

```bash
export SAIL_EXECUTION__CHECKPOINT__PATH="s3://sail/checkpoint"
```
