---
title: Checkpoint Storage
rank: 6
---

# Checkpoint Storage

The Spark `DataFrame.checkpoint()` API allows you to store intermediate data for reuse. This is important for iterative processing, which is typically used in graph algorithms. Without checkpointing, the query plan can grow exponentially during iterations, and all computations are performed from scratch at every step.

Sail supports checkpointing in both local and cluster modes. Sail relies on external storage for checkpoint data.
For example, the storage can be a local file system for local query execution. In cluster mode, it can be a network file system accessible by all Sail workers or a cloud storage service such as Amazon S3.
In cluster mode, the workers write and read the checkpoint files, and the server deletes them during cleanup.
Both need access to the storage.

The following example shows how to configure the checkpoint storage path via environment variables.

```bash
export SAIL_EXECUTION__CHECKPOINT__PATH="s3://sail/checkpoint"
```

With the default empty path, Sail rejects checkpoint operations instead of keeping the data in memory or on the local disk of a worker.
If you also use the `storage` shuffle backend, use a path for checkpoints that is separate from the shuffle storage path.

Sail supports only eager checkpoints, which is the default for `df.checkpoint()`.
Sail rejects lazy checkpoints and the `storageLevel` argument of `df.localCheckpoint()`.
`df.localCheckpoint()` writes to the same path as `df.checkpoint()`.

Checkpoints belong to the session that creates them.
Sail removes the checkpoint data of a session when the client releases the session, or when the session expires after `spark.session_timeout_secs` without client requests.
Sail does not remove checkpoint data when the server shuts down.
Use a lifecycle rule for the checkpoint path to remove the files that an interrupted cleanup leaves behind.
