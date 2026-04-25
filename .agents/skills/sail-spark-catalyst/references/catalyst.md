# Spark Catalyst/Tungsten/Streaming Expert

Expert in Spark's query optimizer (Catalyst), execution engine (Tungsten), and Structured Streaming for Sail compatibility.

## Context

To achieve 100% Spark parity, Sail must understand and replicate Spark's:
- **Catalyst**: Query planning and optimization
- **Tungsten**: Memory management and code generation
- **Structured Streaming**: Incremental query processing

## Spark Codebase Access

Access to Apache Spark source at `/Users/santosh/IdeaProjects/spark/`:

### Catalyst Optimizer
```
/Users/santosh/IdeaProjects/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/
├── analysis/            # Analyzer rules
├── optimizer/           # Optimizer rules
├── planner/             # Logical planner
└── expressions/         # Expression evaluation
```

### Tungsten Execution Engine
```
/Users/santosh/IdeaProjects/spark/sql/core/src/main/scala/org/apache/spark/sql/execution/
├── WholeStageCodegenGen.scala   # Code generation
├── memory/                      # Memory management
├── datasources/                 # Data source implementations
└── exchange/                    # Shuffle exchange
```

### Structured Streaming
```
/Users/santosh/IdeaProjects/spark/sql/streaming/src/main/scala/org/apache/spark/sql/streaming/
├── execution/           # Streaming execution
├── StreamingQuery.scala # Query management
└── triggers/            # Trigger policies
```

## Spark Catalyst

### Phases of Query Planning

```
Unresolved Logical Plan
       │
       ▼ (Analysis)
Resolved Logical Plan
       │
       ▼ (Logical Optimization)
Optimized Logical Plan
       │
       ▼ (Physical Planning)
Physical Plan
       │
       ▼ (Code Generation)
Runnable Code
```

### Analyzer Rules

```scala
// sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/

object ResolveReferences extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
        case p: LogicalPlan if !p.resolved =>
            resolveReferences(p)
    }
}
```

**Sail Equivalent**:
```rust
// crates/sail-sql-analyzer/src/

pub fn resolve_references(plan: LogicalPlan) -> Result<LogicalPlan> {
    // Resolve UnresolvedAttribute to resolved columns
}
```

### Optimizer Rules

```scala
// sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/

object ConstantFolding extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
        case Add(Literal(a), Literal(b)) => Literal(a + b)
        // ... more constant folding
    }
}
```

**Sail Equivalent**:
```rust
// crates/sail-logical-optimizer/src/rules/

pub struct ConstantFoldingRule;

impl OptimizerRule for ConstantFoldingRule {
    fn try_optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        // Fold constants
    }
}
```

### Planner Strategies

```scala
// sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/planner/

object SparkStrategies extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
        case p: logical.Project =>
            ProjectExec(p.projectList, planLater(p.child)) :: Nil
        case f: logical.Filter =>
            FilterExec(f.condition, planLater(f.child)) :: Nil
        // ... more strategies
    }
}
```

**Sail Equivalent**:
```rust
// crates/sail-physical-optimizer/src/

pub fn plan_physical(logical: LogicalPlan) -> Result<PhysicalPlan> {
    match logical {
        LogicalPlan::Projection { input, expr } => {
            Ok(PhysicalPlan::Projection {
                input: plan_physical(*input)?,
                expr,
            })
        }
        // ... more cases
    }
}
```

## Spark Tungsten

### Whole-Stage Code Generation

```scala
// sql/core/src/main/scala/org/apache/spark/sql/execution/

trait CodegenSupport extends SparkPlan {
    def doCodeGen(): CodegenContext
    def inputRDDs(): RDD[InternalRow]
}

// Generates Java code at runtime for entire query stages
WholeStageCodegengen(ProjectExec(FilterExec(...)))
```

**Sail Approach**:
- **No code generation** (Sail uses compiled Rust)
- **Vectorized execution** with Arrow
- **SIMD operations** via Arrow compute kernels

### Unsafe Memory Management

```scala
// sql/core/src/main/scala/org/apache/spark/sql/execution/memory/

// Off-heap memory management
class UnsafeMemoryManager {
    def allocate(size: Long): Long = // Returns memory address
    def free(address: Long): Unit
}
```

**Sail Approach**:
- **Rust ownership** for memory safety
- **Arena allocation** for temporary data
- **Reference counting** via Arc

### BinaryRow Format

```scala
// Compact row representation
class UnsafeRow extends BaseGenericInternalRow {
    // Base object + offset (no per-row allocation)
    private[this] var baseObject: AnyRef = _
    private[this] var baseOffset: Long = _
}
```

**Sail Approach**:
- **Columnar format** (Arrow arrays)
- **No row representation** needed

## Sail vs Spark Architecture

| Spark | Sail |
|-------|------|
| Catalyst Analyzer | `sail-sql-analyzer` |
| Catalyst Optimizer | `sail-logical-optimizer` |
| SparkPlanner | `sail-physical-optimizer` |
| Tungsten WholeStageCodegen | Vectorized Arrow execution |
| Unsafe memory | Rust ownership + Arena |
| Row-based execution | Columnar execution |
| JVM bytecode | Compiled Rust with SIMD |

## Structured Streaming

### Micro-Batch Processing

```
┌─────────────────────────────────────────────────────────────┐
│                  StreamingQuery                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Source    │──│  Process    │──│      Sink           │  │
│  │  (Rate)     │  │  (Map)      │  │    (Memory)         │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│       │                                     │               │
│       ▼                                     ▼               │
│  Trigger every 1 sec                  Write output         │
└─────────────────────────────────────────────────────────────┘
```

### Streaming Execution

```python
# Spark Structured Streaming
df = spark.readStream.format("rate").load()
df.writeStream
    .format("memory")
    .queryName("rate_table")
    .trigger(Trigger.ProcessingTime("1 second"))
    .start()
```

**Sail Status**: Partial support, under development

### Watermarking

```python
# Handle late data
df.withWatermark("timestamp", "10 minutes")
```

### Output Modes

| Mode | Description | Spark | Sail |
|------|-------------|-------|------|
| Append | Only new rows | ✓ | Planned |
| Complete | All rows | ✓ | Planned |
| Update | Changed rows | ✓ | Planned |

### Streaming Sources

| Source | Spark | Sail |
|--------|-------|------|
| Rate (test source) | ✓ | ✓ |
| Socket | ✓ | ✓ |
| Kafka | ✓ | Planned |
| Delta Lake | ✓ | Planned |

## Key Differences

### Execution Model

| Aspect | Spark | Sail |
|--------|-------|------|
| Execution | Row-based with codegen | Columnar with SIMD |
| Memory | JVM heap + off-heap | Rust ownership |
| Concurrency | Java threads | Tokio async |
| Shuffle | Netty (row-based) | Arrow Flight (columnar) |
| Python UDFs | Row-by-row (slow) | Arrow batches (fast) |

### Type System Differences

```python
# Spark 4.0+ has VARIANT type
df.select(parse_json(col))

# Sail: Not yet supported
```

### Function Behavior Differences

```python
# Spark: NULL handling
spark.sql("SELECT NULL + 1")  # Returns NULL

# Sail: Should match (check compatibility)
```

## Critical Files

| File | Purpose |
|------|---------|
| `crates/sail-logical-optimizer/src/` | Sail's logical optimizer |
| `crates/sail-physical-optimizer/src/` | Sail's physical optimizer |
| `crates/sail-plan/src/resolver/` | Query resolution |
| `crates/sail-execution/src/` | Execution engine |
| `/Users/santosh/IdeaProjects/spark/sql/catalyst/` | Spark Catalyst reference |
| `/Users/santosh/IdeaProjects/spark/sql/core/execution/` | Spark Tungsten reference |
| `/Users/santosh/IdeaProjects/spark/sql/streaming/` | Spark Streaming reference |

## Implementing Spark Features

### Adding a Spark Function

1. **Check Spark implementation**:
```bash
grep -r "my_function" /Users/santosh/IdeaProjects/spark/sql/
```

2. **Find function signature**:
```scala
// functions.scala
def my_function(expr: Column): Column = withExpr { MyFunction(expr.expr) }
```

3. **Implement in Sail**:
```rust
// crates/sail-function/src/scalar/my_function.rs
pub fn register(registry: &mut FunctionRegistry) {
    registry.register_udaf(
        "my_function",
        // Signature and implementation
    );
}
```

### Debugging Behavior Differences

1. **Create a test case**:
```python
# python/pysail/tests/spark/test_my_function.py
def test_my_function():
    result = spark.sql("SELECT my_function(1)")
    expected = spark.createDataFrame([(1,)], ["value"])
    assert_compare_results(result, expected)
```

2. **Run against both Spark and Sail**:
```bash
# Test with Spark
spark-submit test_my_function.py

# Test with Sail
sail spark server &
spark-submit --master "local[2]" test_my_function.py
```

3. **Compare results and debug**

## Streaming Implementation Guide

### State Management

```rust
// State store for streaming
pub struct StateStore {
    // Persist state across micro-batches
}

impl StateStore {
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    pub fn put(&mut self, key: &[u8], value: &[u8]);
}
```

### Trigger Execution

```rust
// Execute streaming micro-batch
pub async fn execute_micro_batch(
    query: &StreamingQuery,
    batch_id: i64,
) -> Result<BatchResult> {
    // Read new data from source
    let new_data = query.source.read_batch().await?;

    // Process with state
    let result = query.process(new_data, &query.state_store).await?;

    // Write to sink
    query.sink.write(result).await?;

    Ok(BatchResult { batch_id })
}
```

## References

- Spark Catalyst: `/Users/santosh/IdeaProjects/spark/sql/catalyst/`
- Spark Tungsten: `/Users/santosh/IdeaProjects/spark/sql/core/execution/`
- Spark Streaming: `/Users/santosh/IdeaProjects/spark/sql/streaming/`
- Spark Functions: `sql/core/src/main/scala/org/apache/spark/sql/functions.scala`
