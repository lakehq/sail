# Tokio Async Runtime Expert

Expert in Tokio async/await patterns for Sail's distributed architecture. Guides async actor pattern implementation, gRPC services, and backpressure handling.

## Context

Sail uses Tokio as its async runtime for distributed execution. Tokio provides:
- Async task scheduling and execution
- Timer utilities
- Networking primitives
- Channel-based communication

## Async/Await Basics

### Spawning Tasks
Run concurrent tasks with `tokio::spawn`:

```rust
use tokio::task;

async fn my_task() -> Result<()> {
    // Async work
    Ok(())
}

// Spawn and forget (fire and forget)
task::spawn(async move {
    my_task().await.unwrap();
});

// Spawn and get handle
let handle = task::spawn(async move {
    my_task().await
});

// Wait for result
let result = handle.await?;
```

### Async Functions
Mark functions with `async`:

```rust
async fn fetch_data(id: i32) -> Vec<u8> {
    // Can use .await here
    let data = read_from_network().await?;
    Ok(data)
}

// Call with .await
let data = fetch_data(42).await?;
```

### Join Multiple Tasks
Run tasks concurrently and wait for all:

```rust
use tokio::try_join;

let (a, b, c) = try_join!(
    task_a(),
    task_b(),
    task_c()
)?;
```

## Channels

### MPSC (Multi-Producer, Single-Consumer)
For actor message passing:

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

### Broadcast
Multiple receivers get all messages:

```rust
use tokio::sync::broadcast;

let (tx, _rx) = broadcast::channel(100);

// Multiple subscribers
let rx1 = tx.subscribe();
let rx2 = tx.subscribe();

// All receivers get every message
tx.send(message)?;
```

### Watch
Single value, multiple readers see latest:

```rust
use tokio::sync::watch;

let (tx, rx) = watch::channel(initial_value);

// Readers see latest value
let mut rx = rx.clone();
while rx.changed().await.is_ok() {
    let latest = rx.borrow();
    // Process latest value
}
```

### oneshot
Single-use channel:

```rust
use tokio::sync::oneshot;

let (tx, rx) = oneshot::channel();

// Send once
tx.send(result)?;

// Receive once
let result = rx.await?;
```

## Actor Pattern

Sail uses actors for concurrent state management:

```rust
use tokio::sync::mpsc;

struct MyActor {
    receiver: mpsc::Receiver<Message>,
    state: ActorState,
}

impl MyActor {
    async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(&mut self, msg: Message) {
        match msg {
            Message::DoWork { respond_to } => {
                let result = // Do work
                let _ = respond_to.send(result);
            }
            // ... other messages
        }
    }
}
```

## Async gRPC Services

### Tonic Server
Sail uses Tonic for gRPC:

```rust
use tonic::{Request, Response, Status};
use sail_spark_connect::proto::spark_connect::*;

#[derive(Debug, Default)]
pub struct SparkConnectService {
    // Service state
}

#[tonic::async_trait]
impl SparkConnectService for SparkConnectService {
    async fn execute_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<Response<ExecutePlanResponse>, Status> {
        // Handle request
        let response = ExecutePlanResponse {};
        Ok(Response::new(response))
    }
}

// Start server
async fn start_server() -> Result<()> {
    let addr = "[::]:50051".parse()?;
    let service = SparkConnectService::default();

    Server::builder()
        .add_service(spark_connect_server(server))
        .serve(addr)
        .await?;

    Ok(())
}
```

## Timing and Timeouts

### sleep
Pause execution:

```rust
use tokio::time::{sleep, Duration};

sleep(Duration::from_secs(1)).await;
```

### timeout
Limit execution time:

```rust
use tokio::time::{timeout, Duration};

let result = timeout(Duration::from_secs(5), async_operation).await?;
match result {
    Ok(value) => /* completed */,
    Err(_) => /* timed out */,
}
```

### interval
Run tasks periodically:

```rust
use tokio::time::{interval, Duration};

let mut ticker = interval(Duration::from_secs(10));
loop {
    ticker.tick().await;
    // Do periodic work
}
```

## Backpressure

### Bounded Channels
Limit memory usage:

```rust
use tokio::sync::mpsc;

// Channel with capacity of 100
let (tx, rx) = mpsc::channel(100);

// Send will wait if channel is full
tx.send(message).await?;
```

### Blocking Send
Send without waiting (returns error if full):

```rust
if let Err(_) = tx.try_send(message) {
    // Channel full, handle backpressure
}
```

### Stream Backpressure
For data streaming:

```rust
use futures::stream::{StreamExt, SinkExt};

// Process stream with backpressure
while let Some(item) = stream.next().await {
    // Process item before requesting next
}
```

## Async Synchronization

### Mutex
Async-friendly mutex:

```rust
use tokio::sync::Mutex;

let mutex = Mutex::new(data);

{
    let mut guard = mutex.lock().await;
    // Access data
    *guard += 1;
}
```

### RwLock
Multiple readers or single writer:

```rust
use tokio::sync::RwLock;

let lock = RwLock::new(data);

// Read lock (multiple concurrent)
let r1 = lock.read().await;
let r2 = lock.read().await;

// Write lock (exclusive)
let mut w = lock.write().await;
```

### Semaphore
Limit concurrent operations:

```rust
use tokio::sync::Semaphore;

let semaphore = Semaphore::new(10); // Max 10 concurrent

let permit = semaphore.acquire().await?;
// Do work
drop(permit);
```

## Tracing and Logging

Async-aware logging with `tracing`:

```rust
use tracing::{info, error, instrument};

#[instrument(skip(self))]
async fn handle_request(&self, request: Request) -> Result<Response> {
    info!("Handling request");
    // ... handle request
    Ok(response)
}
```

## Common Patterns

### Graceful Shutdown
```rust
use tokio::signal;

async fn run_server() -> Result<()> {
    let server_handle = tokio::spawn(start_server());

    // Wait for Ctrl+C
    signal::ctrl_c().await?;

    // Shutdown
    server_handle.abort();
    Ok(())
}
```

### Task Groups
Run multiple tasks with cancellation:

```rust
use tokio::task::JoinSet;

let mut tasks = JoinSet::new();

tasks.spawn(async move { task1().await });
tasks.spawn(async move { task2().await });

while let Some(result) = tasks.join_next().await {
    result??;
}
```

## Critical Files

| File | Purpose |
|------|---------|
| `crates/sail-server/src/` | Actor system foundation |
| `crates/sail-spark-connect/src/server.rs` | Async gRPC server |
| `crates/sail-execution/src/` | Async execution engine |
| `crates/sail-session/src/` | Async session management |

## Debugging Tips

### Enable Tokio Console
```bash
RUSTFLAGS="--cfg tokio_unstable" cargo build
```

### Tracing
```rust
use tracing::{Level, info};

// Initialize tracing
tracing_subscriber::fmt()
    .with_max_level(Level::DEBUG)
    .init();
```

## Performance Tips

1. **Use bounded channels** to prevent unbounded memory growth
2. **Prefer `try_send`** over `send` in hot loops
3. **Use `join_all`** for waiting on multiple futures
4. **Avoid blocking** in async tasks - use `spawn_blocking` for CPU work
5. **Set appropriate channel sizes** based on throughput

## References

- Tokio docs: https://tokio.rs/
- Tonic: https://github.com/hyperium/tonic
- Tracing: https://docs.rs/tracing/
