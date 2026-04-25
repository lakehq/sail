---
name: sail-distributed
description: Expert guidance for Sail's distributed execution architecture. Use when designing or debugging distributed systems, implementing shuffle data exchange, or working with fault tolerance. Triggers include understanding driver-worker model, stage scheduling, Arrow Flight shuffle, task failure recovery, or optimizing distributed query performance.
---

# Distributed Systems Architecture

Sail supports local and cluster execution modes with Arrow Flight for data shuffling.

## Architecture

```
PySpark Client → Sail Server (Driver) → Workers
                    ↓
              Stage Scheduling
                    ↓
              Task Assignment
                    ↓
              Arrow Flight Shuffle
```

## Components

| Component | Location | Purpose |
|-----------|----------|---------|
| Driver | `sail-execution/src/driver/` | Schedules distributed plans |
| Workers | `sail-execution/src/worker/` | Execute task partitions |
| Control Plane | gRPC | Orchestration (Tonic) |
| Data Plane | Arrow Flight | Efficient columnar shuffle |

## Arrow Flight Shuffle

```rust
use arrow_flight::flight_service_client::FlightServiceClient;

// Write shuffle data
async fn write_shuffle(shuffle_id: String, batches: Vec<RecordBatch>) {
    let client = FlightServiceClient::connect("http://next_worker").await?;
    client.do_put(batches).await?;
}

// Read shuffle data
async fn read_shuffle(shuffle_id: String, partition: i32) -> Vec<RecordBatch> {
    let client = FlightServiceClient::connect("http://upstream_worker").await?;
    let mut stream = client.do_get(shuffle_id).await?;
    // Collect batches
}
```

## Fault Tolerance

```rust
// Retry failed tasks
async fn execute_with_retry(task: Task, max_retries: usize) -> Result<TaskResult> {
    for attempt in 0..max_retries {
        match execute_task(&task).await {
            Ok(result) => return Ok(result),
            Err(e) if attempt < max_retries - 1 => continue,
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}
```

## Resources

- **[distributed.md](references/distributed.md)**: Comprehensive guide with actor patterns, Arrow Flight shuffle, and fault tolerance
- **[tokio.md](../sail-tokio/references/tokio.md)**: Async runtime foundations
- **[spark-connect.md](../sail-spark-connect/references/spark-connect.md)**: Spark Connect protocol
