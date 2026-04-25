# Distributed Systems Architect

Expert in Sail's distributed execution architecture. Guides design of driver-worker model, stage scheduling, Arrow Flight shuffle, fault tolerance, and distributed failures.

## Context

Sail operates in two modes:
- **Local Mode**: Single-process execution with local job runner
- **Cluster Mode**: Distributed execution with driver + workers

## Distributed Architecture

### Components

```
┌─────────────────────────────────────────────────────────────┐
│                        PySpark Client                         │
│                    (Spark Connect Client)                     │
└───────────────────────────────┬───────────────────────────────┘
                                │ gRPC (Spark Connect)
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                      Sail Server (Driver)                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Planner   │  │  Scheduler  │  │  Actor Coordinator  │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└───────────────────────────────┬───────────────────────────────┘
                                │
            ┌───────────────────┼───────────────────┐
            │ Control Plane (gRPC)  │ Data Plane (Arrow Flight)
            ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Worker 1      │    │   Worker 2      │    │   Worker N      │
│  ┌───────────┐  │    │  ┌───────────┐  │    │  ┌───────────┐  │
│  │  Executor │  │    │  │  Executor │  │    │  │  Executor │  │
│  └───────────┘  │    │  └───────────┘  │    │  └───────────┘  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Driver Responsibilities

```rust
// crates/sail-execution/src/driver/actor.rs

pub struct DriverActor {
    // Schedule distributed physical plans
    async fn schedule_physical_plan(&mut self, plan: DistributedPlan) {
        // Break plan into stages
        let stages = self.split_into_stages(plan);

        // For each stage, create tasks
        for stage in stages {
            let tasks = self.create_tasks(&stage);

            // Assign tasks to workers
            for task in tasks {
                let worker = self.select_worker(&task);
                worker.execute_task(task).await?;
            }

            // Wait for stage completion
            self.wait_for_stage(&stage).await?;

            // Exchange shuffle data via Arrow Flight
            self.exchange_shuffle_data(&stage).await?;
        }
    }
}
```

### Worker Responsibilities

```rust
// crates/sail-execution/src/worker/actor.rs

pub struct WorkerActor {
    async fn execute_task(&mut self, task: Task) -> Result<TaskResult> {
        // Execute partition of a stage
        let batch_stream = self.execute_plan_fragment(task.plan)?;

        // Collect results
        let batches: Vec<RecordBatch> = batch_stream.collect().await?;

        // If this stage has shuffle, write to Arrow Flight
        if task.has_shuffle {
            self.write_shuffle_data(task.shuffle_id, batches).await?;
        }

        Ok(TaskResult::Success)
    }
}
```

## Control Plane

### gRPC Orchestration

Driver uses internal gRPC to communicate with workers:

```rust
// crates/sail-server/src/internal/grpc/

async fn register_worker(driver: &DriverActor, worker_url: String) {
    driver.available_workers.push(Worker {
        url: worker_url,
        state: WorkerState::Idle,
    });
}

async fn assign_task(worker: &Worker, task: Task) -> Result<TaskResult> {
    let mut client = WorkerServiceClient::connect(worker.url.clone()).await?;
    let request = ExecuteTaskRequest { task };
    let response = client.execute_task(request).await?;
    Ok(response.result)
}
```

### Worker Discovery

```rust
// Workers register with driver on startup
async fn worker_main(driver_url: String) {
    let mut driver_client = DriverServiceClient::connect(driver_url).await?;

    // Register this worker
    let register_req = RegisterWorkerRequest {
        worker_id: Uuid::new_v4().to_string(),
        // Worker capabilities
    };
    driver_client.register_worker(register_req).await?;

    // Start accepting tasks
    let worker_actor = WorkerActor::new();
    worker_actor.run().await;
}
```

## Data Plane (Arrow Flight)

### Shuffle with Arrow Flight

Sail uses Apache Arrow Flight for efficient data exchange:

```rust
// crates/sail-execution/src/shuffle/arrow_flight/

use arrow_flight::flight_service_client::FlightServiceClient;

async fn write_shuffle_data(
    shuffle_id: String,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    let client = FlightServiceClient::connect("http://next_worker:50052").await?;

    // Upload batches via Arrow Flight
    let flight_data = batches.into_iter().map(|batch| {
        // Convert RecordBatch to FlightData
        (flight_descriptor_from(batch.schema()), batch)
    });

    client.do_put(flight_data).await?;
    Ok(())
}
```

### Reading Shuffle Data

```rust
async fn read_shuffle_data(
    shuffle_id: String,
    partition: i32,
) -> Result<Vec<RecordBatch>> {
    let client = FlightServiceClient::connect("http://upstream_worker:50052").await?;

    let request = FlightDescriptor {
        path: vec![shuffle_id, partition.to_string()],
        ..Default::default()
    };

    let mut stream = client.do_get(request).await?.into_inner();

    let mut batches = Vec::new();
    while let Some(flight_data) = stream.next().await {
        let batch = record_batch_from_flight_data(flight_data?);
        batches.push(batch);
    }

    Ok(batches)
}
```

## Stage Scheduling

### Breaking Plans into Stages

```rust
// crates/sail-execution/src/scheduler/

fn split_into_stages(plan: &PhysicalPlan) -> Vec<Stage> {
    let mut stages = Vec::new();
    let mut current_stage = Stage::new();

    for node in plan.nodes() {
        match node {
            PhysicalPlanNode::ShuffleExchange(_) => {
                // Shuffle boundary - end current stage
                stages.push(current_stage);
                current_stage = Stage::new();
            }
            _ => {
                current_stage.add_node(node);
            }
        }
    }

    stages.push(current_stage);
    stages
}
```

### Creating Tasks from Stages

```rust
fn create_tasks(stage: &Stage, num_partitions: usize) -> Vec<Task> {
    let mut tasks = Vec::new();

    for partition in 0..num_partitions {
        let task = Task {
            id: Uuid::new_v4(),
            stage_id: stage.id.clone(),
            partition: partition as i32,
            plan: stage.plan_for_partition(partition),
        };
        tasks.push(task);
    }

    tasks
}
```

## Fault Tolerance

### Task Retry

```rust
async fn execute_with_retry(
    worker: &Worker,
    task: Task,
    max_retries: usize,
) -> Result<TaskResult> {
    for attempt in 0..max_retries {
        match worker.execute_task(task.clone()).await {
            Ok(result) => return Ok(result),
            Err(e) if attempt < max_retries - 1 => {
                tracing::warn!("Task {} failed (attempt {}), retrying", task.id, attempt);
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}
```

### Worker Failure Detection

```rust
async fn monitor_workers(driver: &mut DriverActor) {
    loop {
        for worker in &mut driver.available_workers {
            // Heartbeat check
            match worker.heartbeat().await {
                Ok(_) => worker.state = WorkerState::Healthy,
                Err(_) => {
                    tracing::error!("Worker {} failed", worker.id);
                    worker.state = WorkerState::Failed;
                    // Reassign tasks from this worker
                    driver.reassign_worker_tasks(worker).await?;
                }
            }
        }
        sleep(Duration::from_secs(5)).await;
    }
}
```

## Backpressure

### Flow Control

```rust
// Prevent overwhelming workers
struct BackpressureController {
    max_inflight_per_worker: usize,
    inflight_tasks: HashMap<WorkerId, usize>,
}

impl BackpressureController {
    fn can_schedule(&self, worker: &Worker) -> bool {
        let inflight = self.inflight_tasks.get(&worker.id).unwrap_or(&0);
        *inflight < self.max_inflight_per_worker
    }

    async fn wait_for_capacity(&mut self, worker: &Worker) {
        while !self.can_schedule(worker) {
            sleep(Duration::from_millis(100)).await;
        }
    }
}
```

## Distributed Execution Flow

1. **Client** submits query via Spark Connect (gRPC)
2. **Driver** parses, plans, and splits into stages
3. **Stages** split into tasks based on partitioning
4. **Tasks** assigned to workers via control plane (gRPC)
5. **Workers** execute tasks and exchange data via Arrow Flight
6. **Results** aggregated and returned to client

## Critical Files

| File | Purpose |
|------|---------|
| `crates/sail-execution/src/` | Distributed execution engine |
| `crates/sail-server/src/actor/` | Actor system implementation |
| `crates/sail-spark-connect/src/server.rs` | gRPC service orchestration |
| `docs/concepts/architecture/index.md` | Architecture documentation |

## Debugging Tips

### Enable Tracing
```bash
RUST_LOG=sail_execution=debug,sail_server=debug
```

### View Stage Plan
```rust
println!("Stage breakdown:\n{:#?}", stages);
```

### Monitor Shuffle
```rust
tracing::info!("Shuffle {} size: {} bytes", shuffle_id, size);
```

## Performance Tips

1. **Minimize shuffle** - filter and aggregate before shuffling
2. **Optimal partitioning** - match partitions to CPU cores
3. **Locality** - schedule tasks on workers with data locality
4. **Batch Arrow data** - send multiple RecordBatches per Flight call
5. **Compression** - enable Arrow Flight compression for large shuffles

## References

- Sail Architecture: `docs/concepts/architecture/index.md`
- Arrow Flight: https://arrow.apache.org/docs/format/Flight.html
- Actor Model: https://github.com/actix/actix
