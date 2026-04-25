---
name: sail-tokio
description: Expert guidance for Tokio async runtime in Sail's distributed architecture. Use when implementing async services, gRPC endpoints, actor patterns, or debugging async issues. Triggers include writing async code with .await, implementing actors with channels, building gRPC services with Tonic, handling backpressure, or debugging deadlocks and race conditions in async contexts.
---

# Tokio Async Runtime

Sail uses Tokio for async I/O, actors, and distributed coordination.

## Quick Reference

| Module | Location | Purpose |
|--------|----------|---------|
| Actor system | `sail-server/src/` | Actor-based concurrency |
| gRPC server | `sail-spark-connect/src/server.rs` | Async Spark Connect service |
| Execution | `sail-execution/src/` | Async task execution |
| Sessions | `sail-session/src/` | Session management |

## Common Patterns

### Spawning Tasks

```rust
use tokio::task;

// Fire and forget
task::spawn(async move { my_work().await });

// Get handle
let handle = task::spawn(async move { my_work().await });
let result = handle.await?;
```

### Channels (Actor Communication)

```rust
use tokio::sync::mpsc;

let (tx, mut rx) = mpsc::channel(100);

// Sender
tx.send(message).await?;

// Receiver
while let Some(msg) = rx.recv().await {
    // Handle message
}
```

### Async gRPC Service

```rust
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl MyService for MyServer {
    async fn handle_request(&self, req: Request<MyRequest>) -> Result<Response<MyResponse>, Status> {
        Ok(Response::new(MyResponse {}))
    }
}
```

## Key Concepts

- **tokio::spawn**: Run concurrent tasks
- **channels**: mpsc (multi-producer), broadcast, watch, oneshot
- **Mutex/RwLock**: Async-friendly synchronization
- **tracing**: Async-aware logging and instrumentation

## Resources

- **[tokio.md](references/tokio.md)**: Comprehensive Tokio guide with async patterns, channels, and gRPC services
- **[distributed.md](../sail-distributed/references/distributed.md)**: Distributed execution architecture using Tokio
- **[arrow.md](../sail-arrow/references/arrow.md)**: Streaming Arrow data asynchronously
