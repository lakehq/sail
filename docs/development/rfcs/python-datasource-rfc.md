# RFC: Python DataSource Support

**Status:** Phase 1 Complete (MVP Read-Only)
**Branch:** `feat/python-datasource-write`
**PR:** #1 - MVP Batch Read
**Authors:** Santosh
**Date:** 2025-12-31

## Summary

This RFC documents the implementation of Python DataSource support in Sail, enabling users to implement custom data sources in Python while leveraging Sail's distributed execution engine. The implementation provides 100% API compatibility with PySpark 3.5+/4.0+ DataSource API.

This RFC represents **Phase 1 (PR #1)** of a broader implementation strategy with 6 planned phases focused on incremental capability delivery.

## Motivation

### Problem Statement

Users need the ability to:
1. Read data from custom sources not natively supported by Sail (APIs, proprietary formats, etc.)
2. Leverage existing Python data processing code within Sail's distributed execution
3. Maintain compatibility with PySpark DataSource implementations for easy migration

**Broader Ecosystem Challenge**: Modern data systems face a tension between **extensibility** and **maintainability**. Adding native support for every data source, API, or proprietary format directly into the core codebase leads to:
- **Code bloat**: Core framework becomes harder to maintain and reason about
- **Release coupling**: New connectors tied to framework release cycles
- **Security surface**: Every integration increases attack surface and dependency risk
- **Barrier to contribution**: Contributors must navigate complex build systems and testing infrastructure

**The Plugin Model Solution**: By providing a **composable plugin architecture**, Sail enables:
- **Decentralized innovation**: Community can build and distribute connectors independently
- **Risk isolation**: Third-party code runs in user space, not embedded in core
- **Rapid iteration**: Connector updates don't require Sail releases
- **Lower barrier to entry**: Python developers can extend Sail without Rust expertise

### Goals

- **100% PySpark API Compatibility**: Existing PySpark DataSource implementations work without modification
- **High Performance**: Zero-copy data transfer via Arrow C Data Interface
- **Distributed Execution**: Parallel partition-based reading across workers
- **Filter Pushdown**: DataFusion expressions converted to Python filter objects
- **Extensibility**: Trait-based design for future subprocess isolation (PR #3)
- **Composability**: Enable users to mix-and-match datasources without core modifications

### Why Python?

Python was chosen as the first (and currently only) extension language for several strategic reasons:

**1. Ecosystem Dominance in Data**
Python is the de facto language for data engineering and data science:
- **PyArrow**: Industry-standard Arrow implementation with mature C Data Interface
- **Database Connectors**: connector-x, DuckDB, psycopg3, turbodbc all have Python bindings
- **Cloud SDKs**: boto3 (AWS), google-cloud-python (GCP), azure-sdk-for-python
- **API Libraries**: requests, httpx, aiohttp for custom API integrations

**2. PySpark Migration Path**
Organizations migrating from PySpark can reuse existing DataSource implementations with zero code changes, drastically reducing migration friction.

**3. Low Barrier to Entry**
Python developers outnumber Rust developers ~50:1 in the data space. Enabling Python extensions means:
- Data engineers can build connectors without learning Rust
- Faster time-to-market for custom integrations
- Broader community contribution potential

**4. Subprocess Isolation Feasibility**
Python's GIL and interpreter architecture make subprocess isolation straightforward:
- Clear process boundaries for crash isolation
- Well-established IPC mechanisms (gRPC, shared memory)
- PyO3 provides robust in-process FFI for MVP simplicity

**Future Languages**: While Python is the current focus, the `PythonExecutor` trait design allows future support for other languages (Go, JavaScript via Node.js) following similar patterns.

### Non-Goals (Deferred to Later Phases)

- Write support (Phase 2)
- Subprocess isolation for GIL parallelism (Phase 3)
- Streaming read/write (Phase 4-5)
- Exotic data types and performance polish (Phase 6)

## Alternatives Considered

### 1. Python Execution Model

| Alternative | Pros | Cons | Decision |
|-------------|------|------|----------|
| **PyO3 in-process (chosen)** | Zero-copy Arrow, simple deployment, low latency | Single GIL, crash takes down server | MVP choice for simplicity |
| **Subprocess + gRPC** | GIL parallelism, crash isolation | Serialization overhead, complex lifecycle | Deferred to PR #3 |
| **Subprocess + shared memory** | GIL parallelism + zero-copy | High complexity, platform-specific | Deferred to PR #3 |

**Rationale**: PyO3 in-process provides the best developer experience for MVP. The `PythonExecutor` trait abstracts the execution model, enabling future migration to subprocess isolation without API changes.

### 2. Serialization Format

| Alternative | Pros | Cons | Decision |
|-------------|------|------|----------|
| **cloudpickle (chosen)** | Pickles lambdas/closures, PySpark compatible | Security considerations (same as Spark) | Chosen for PySpark compatibility |
| **dill** | Similar to cloudpickle | Less maintained, PySpark uses cloudpickle | Rejected |
| **pickle** | Standard library | Cannot pickle lambdas/closures | Rejected |
| **JSON + class registry** | Safe, inspectable | Cannot serialize arbitrary Python objects | Rejected: limits flexibility |

**Rationale**: PySpark uses cloudpickle; using the same ensures zero-friction migration. Security posture matches Spark—datasources are treated as trusted code.

### 3. Filter Representation

| Alternative | Pros | Cons | Decision |
|-------------|------|------|----------|
| **Frozen dataclasses (chosen)** | Immutable, hashable, PySpark compatible | Requires Python 3.7+ | Chosen |
| **Named tuples** | Lightweight, immutable | Less extensible, no methods | Rejected |
| **Protobuf messages** | Language-neutral, schema evolution | Adds dependency, overkill for Python-only | Rejected |
| **Raw dictionaries** | Flexible | No type safety, error-prone | Rejected |

### 4. Data Transfer

| Alternative | Pros | Cons | Decision |
|-------------|------|------|----------|
| **Arrow C Data Interface (chosen)** | Zero-copy, standard | Requires PyArrow | Chosen |
| **Arrow IPC** | Works across processes | Serialization overhead | Used for subprocess mode |
| **Pickle** | Simple | Slow, copies data | Rejected for data plane |

### 5. Extension Mechanism: Python vs Pure FFI

| Alternative | Pros | Cons | Decision |
|-------------|------|------|----------|
| **Python DataSource API (chosen)** | Low barrier to entry, huge ecosystem, PySpark compatibility | GIL limitations (mitigated by subprocess), runtime overhead | Chosen for extensibility |
| **Pure C FFI (dlopen + function pointers)** | Maximum performance, no runtime dependency | High barrier to entry, unsafe, no ecosystem leverage | Rejected for user-facing extensions |
| **WebAssembly (WASM)** | Sandboxing, multi-language | Immature data ecosystem, limited Arrow support | Future consideration |

**Rationale for Rejecting Pure FFI:**

While pure C FFI (e.g., `dlopen` with function pointers) offers maximum performance, it creates an **unreasonably high barrier to entry** for data engineers:

**Technical Complexity**:
- Developers must manage memory safety manually (alloc/free across FFI boundary)
- ABI compatibility issues across compilers and platforms
- No type safety—mistakes manifest as segfaults, not compile errors
- Requires understanding Rust's `#[repr(C)]`, lifetime semantics, and unsafe code

**Toolchain Burden**:
- Every datasource requires a C/C++/Rust compiler
- Platform-specific builds (Linux/macOS/Windows, x86/ARM)
- Dependency management for native libraries
- Testing infrastructure for cross-compilation

**Ecosystem Fragmentation**:
- No standard library for HTTP, JSON, cloud APIs in C
- Developers reinvent wheels (HTTP clients, JSON parsers)
- Security vulnerabilities in hand-rolled code

**Historical Evidence**:
Systems that relied on pure FFI for extensibility (e.g., Postgres prior to PL/Python, early databases with C UDFs) saw limited community adoption compared to higher-level language bindings. The cognitive overhead and crash risk discourage experimentation.

**Python's Advantages**:
- **Safety**: Memory-safe by default; errors are exceptions, not segfaults
- **Ecosystem**: Leverage 350,000+ PyPI packages without reinventing
- **Iteration Speed**: Edit-run cycle measured in seconds, not compile-test-deploy cycles
- **Debugging**: Standard tools (`pdb`, logging) vs debugging C crashes in production

**When Pure FFI Makes Sense**:
For **performance-critical native connectors** (PostgreSQL, Parquet, Arrow Flight), Sail provides direct Rust `TableFormat` implementations. Pure FFI is reserved for internal use, not user-facing extensions.

The Python DataSource API strikes the optimal balance: **accessible for users, performant via Arrow zero-copy, and safe via subprocess isolation**.

## Drawbacks

### 1. GIL Contention (MVP)

**Impact**: In the MVP, all Python code shares a single GIL. This limits parallelism when:
- Multiple partitions execute Python simultaneously
- Pure Python loops dominate (vs NumPy/PyArrow operations)

**Mitigation**:
- Most data operations use Arrow/NumPy which release the GIL
- Subprocess isolation (PR #3) eliminates this limitation
- Recommend libraries like connector-x/DuckDB that release GIL during I/O

### 2. Python Version Coupling

**Impact**: The embedded Python interpreter version must match the environment where datasource classes are defined. Mismatches manifest as:
- `ImportError` when cloudpickle deserializes objects using APIs that changed between versions
- Subtle behavioral differences in standard library (e.g., `datetime` timezone handling changed in 3.9→3.11)
- Binary incompatibility if native extensions (NumPy, PyArrow) were compiled for different Python version

**Example**: A datasource pickled on Python 3.11 using `datetime.fromisoformat()` with new format specifiers will fail on Python 3.9.

**Mitigation**:
- Document supported versions (3.9-3.12)
- Validate Python version at datasource load time with clear error message
- Recommend matching interpreter versions between development and production

### 3. Error Message Quality

**Impact**: Python exceptions are wrapped in DataFusion errors.

**Design Principle**: Application-level errors (e.g., "column 'foo' not found", "invalid date format") MUST match PySpark behavior 1:1 to minimize migration friction. Execution-engine-level differences (e.g., internal stack traces, partition assignment errors) are acceptable.

**Mitigation**:
- Map common Python exceptions to equivalent PySpark error types
- Preserve Python traceback in debug logs
- Test error message parity against PySpark for common failure modes

## Unresolved Questions

### Open for Discussion

1. **Filter pushdown feedback**: Should we provide a way for Python to report *why* a filter was rejected? Currently, readers just return unsupported filters without explanation.

2. **Partition affinity**: Should partitions carry location hints (e.g., "prefer node X")? This could improve data locality but adds complexity.

3. **Schema evolution**: How should we handle schema changes between datasource versions? Current approach: fail fast. Alternative: allow compatible changes.

4. **Metrics API**: What metrics should datasources expose? Options:
   - Counters: rows_read, bytes_read, errors
   - Timers: read_time, filter_time
   - Gauges: active_connections

5. **Resource limits**: Should we enforce memory/CPU limits per datasource? How would this interact with subprocess isolation?

6. **Async Python support**: Should readers support `async def read()`? This could improve I/O-bound datasources but complicates the execution model.

7. **Process reuse strategy**: When subprocess isolation is implemented (PR #3), how should worker processes be managed?
   - Fresh process per query (safe, slow startup)
   - Pooled processes (fast, state leakage risk)
   - Hybrid (fresh for untrusted, pooled for trusted)

8. **Free-threading mode (PEP 703)**: How should Sail adapt to Python's free-threading (no-GIL) mode introduced in Python 3.13?
   - **Detect and adapt**: If free threading detected, use `InProcessExecutor` without GIL concerns
   - **Keep subprocess isolation**: Still valuable for crash/memory/version isolation even without GIL
   - **Configurable**: Let users choose via `SAIL_PYTHON_ISOLATION_MODE` environment variable
   - **Timeline**: Free threading is experimental in 3.13 and not production-ready. Ecosystem (PyO3, NumPy, PyArrow) support is still evolving. We anticipate this becoming relevant in 2026+.
   - **Tracking**: See [PEP 703 – Making the Global Interpreter Lock Optional](https://peps.python.org/pep-0703/)

### Resolved During Implementation

1. ~~**Entry point group name**~~: Using `sail.datasources` (consistent with sail2)
2. ~~**Default batch size**~~: 8192 rows (matches Arrow defaults)
3. ~~**Thread cleanup strategy**~~: RAII with oneshot shutdown signal

## Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| **GIL becomes bottleneck** | Medium | High | Subprocess isolation (PR #3); use connector-x/DuckDB for heavy I/O |
| **Python crash takes down Sail** | Low | Critical | Subprocess isolation (PR #3) provides crash isolation |
| **Memory leak at Python/Rust boundary** | Low | Medium | RAII cleanup in Drop, integration tests for lifecycle |
| **Arrow version mismatch** | Medium | Medium | Pin arrow-pyarrow version, validate at load time |
| **PySpark API divergence** | Low | High | Track PySpark releases, maintain compatibility tests |
| **Free threading makes subprocess obsolete** | Low | Medium | Subprocess isolation provides crash/memory/version isolation independent of GIL; `PythonExecutor` trait allows runtime executor selection |

## Implementation Status

### What's Implemented (PR #1 - MVP Read-Only)

| Component | Status | Location |
|-----------|--------|----------|
| Core Infrastructure | **Complete** | `crates/sail-data-source/src/python_datasource/` |
| Discovery & Registry | **Complete** | `discovery.rs` |
| Filter Pushdown | **Complete** | `filter.rs` |
| Arrow Utilities | **Complete** | `arrow_utils.rs` |
| Execution Plan | **Complete** | `exec.rs` |
| Stream with RAII | **Complete** | `stream.rs` |
| TableProvider | **Complete** | `python_table_provider.rs` |
| TableFormat | **Complete** | `table_format.rs` |
| Python API | **Complete** | `python/pysail/spark/datasource/base.py` |
| Example DataSources | **Complete** | `python/pysail/spark/datasource/examples.py` |

### What's Planned (Future PRs)

| Feature | PR | Status |
|---------|-----|--------|
| Batch Write Support | PR #2 | Planned |
| Subprocess Isolation | PR #3 | Planned |
| Streaming Read | PR #4 | Planned |
| Streaming Write | PR #5 | Planned |
| Performance & Polish | PR #6 | Planned |

## Architecture

### High-Level Design

```
┌─────────────────────────────────────────────────────────────────┐
│                        Sail Server (Rust)                        │
│  ┌─────────────────┐  ┌──────────────────┐  ┌────────────────┐  │
│  │ PythonTableFormat│  │ PythonDataSource │  │ DATASOURCE_    │  │
│  │ (TableFormat)   │──▶│ (PyO3 Bridge)   │◀─│ REGISTRY       │  │
│  └────────┬────────┘  └────────┬─────────┘  └───────▲────────┘  │
│           │                    │                     │           │
│           ▼                    ▼                     │           │
│  ┌─────────────────┐  ┌──────────────────┐          │           │
│  │ PythonTable     │  │ PythonDataSource │   discover_datasources│
│  │ Provider        │  │ Exec (ExecutionPlan)        │           │
│  └────────┬────────┘  └────────┬─────────┘          │           │
│           │                    │                     │           │
│           │                    ▼                     │           │
│           │           ┌──────────────────┐          │           │
│           │           │ PythonDataSource │          │           │
│           │           │ Stream (RAII)    │          │           │
│           │           └────────┬─────────┘          │           │
└───────────│────────────────────│────────────────────│───────────┘
            │                    │                     │
            │         ┌──────────▼──────────┐         │
            │         │  Python Thread      │         │
            │         │  (GIL-bound)        │         │
            │         └──────────┬──────────┘         │
            │                    │                     │
            │                    ▼                     │
            │         ┌──────────────────────────────────────────┐
            │         │           Python Runtime                  │
            │         │  ┌────────────────┐  ┌────────────────┐  │
            │         │  │ DataSource     │  │ DataSourceReader│  │
            │         │  │ (User-defined) │──▶│ (User-defined) │  │
            │         │  └────────────────┘  └───────┬────────┘  │
            │         │                              │            │
            │         │                   ┌──────────▼─────────┐ │
            │         │                   │ InputPartition(s)  │ │
            │         │                   │ (Pickled for dist) │ │
            │         └───────────────────────────────────────────┘
            │                    │
            │         Zero-copy via Arrow C Data Interface
            │                    │
            ▼                    ▼
     ┌──────────────────────────────────────┐
     │        DataFusion Execution          │
     │  (RecordBatch processing, filters)   │
     └──────────────────────────────────────┘
```

### DataFusion Integration

Python DataSources integrate with DataFusion through three key traits. This section provides implementation details for developers extending or debugging the system.

#### TableProvider Integration

`PythonTableProvider` implements DataFusion's `TableProvider` trait, enabling Python datasources to participate in query planning:

```rust
use datafusion::catalog::TableProvider;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};

#[async_trait]
impl TableProvider for PythonTableProvider {
    fn as_any(&self) -> &dyn Any { self }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()  // Cached from Python DataSource.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base  // User-defined table
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,  // Column indices to read
        filters: &[Expr],                  // WHERE clause predicates
        limit: Option<usize>,              // LIMIT clause
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // 1. Convert DataFusion Expr to PythonFilter objects
        let (python_filters, _unsupported) = exprs_to_python_filters(filters);

        // 2. Push filters to Python reader via pushFilters()
        // 3. Get partitions from reader.partitions()
        // 4. Create execution plan with partition info

        Ok(Arc::new(PythonDataSourceExec::new(
            self.datasource.command().to_vec(),
            self.schema.clone(),
            partitions,
        )?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr]
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // MVP: All filters marked as Unsupported (applied by DataFusion post-read)
        // Future: Classify based on Python reader's pushFilters() response
        Ok(vec![TableProviderFilterPushDown::Unsupported; filters.len()])
    }
}
```

**Filter Pushdown Classification:**
- `Exact`: Filter fully handled by datasource, DataFusion skips re-evaluation
- `Inexact`: Datasource partially handles, DataFusion re-evaluates for correctness
- `Unsupported`: Datasource cannot handle, DataFusion applies filter post-read

#### ExecutionPlan Integration

`PythonDataSourceExec` implements DataFusion's `ExecutionPlan` trait for physical execution:

```rust
use datafusion::physical_plan::{ExecutionPlan, PlanProperties, DisplayAs};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};

impl ExecutionPlan for PythonDataSourceExec {
    fn name(&self) -> &'static str { "PythonDataSourceExec" }

    fn as_any(&self) -> &dyn Any { self }

    fn properties(&self) -> &PlanProperties {
        &self.properties  // Pre-computed plan metadata
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]  // Source node - no children
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)  // No children to replace
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Create stream for this partition
        let stream = PythonDataSourceStream::new(
            self.command.clone(),
            self.partitions[partition].clone(),
            self.schema.clone(),
        )?;
        Ok(Box::pin(stream))
    }
}

impl DisplayAs for PythonDataSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PythonDataSourceExec: partitions={}", self.partitions.len())
    }
}
```

**PlanProperties Configuration:**
```rust
let properties = PlanProperties::new(
    EquivalenceProperties::new(schema.clone()),
    Partitioning::UnknownPartitioning(partition_count),  // N parallel partitions
    EmissionType::Incremental,   // Yields batches as available
    Boundedness::Bounded,        // Finite data (batch read)
);
```

#### RecordBatchStream Integration

`PythonDataSourceStream` bridges Python iteration with DataFusion's async streaming:

```rust
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;

impl Stream for PythonDataSourceStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Option<Self::Item>> {
        match &mut self.state {
            StreamState::Running { rx, .. } => {
                // Poll the channel receiving batches from Python thread
                Pin::new(rx).poll_recv(cx)
            }
            StreamState::Stopped => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for PythonDataSourceStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
```

#### Filter Pushdown Pipeline

The complete filter pushdown flow from SQL to Python:

```
┌────────────────────┐     ┌─────────────────────┐     ┌──────────────────┐
│   SQL Query        │     │   DataFusion        │     │   Python         │
│   WHERE x > 10     │────▶│   Expr::BinaryExpr  │────▶│   GreaterThan    │
│   AND y = 'foo'    │     │   {Col, Gt, Lit}    │     │   (x, 10)        │
└────────────────────┘     └─────────────────────┘     └──────────────────┘
                                    │
                           exprs_to_python_filters()
                                    │
                                    ▼
                           ┌─────────────────────┐
                           │ Vec<PythonFilter>   │
                           │ - GreaterThan(x,10) │
                           │ - EqualTo(y,"foo")  │
                           └─────────────────────┘
                                    │
                           to_python_objects()
                                    │
                                    ▼
                           ┌─────────────────────┐
                           │ reader.pushFilters( │
                           │   [GreaterThan(...),│
                           │    EqualTo(...)]    │
                           │ )                   │
                           └─────────────────────┘
                                    │
                           returns unsupported
                                    │
                                    ▼
                           ┌─────────────────────┐
                           │ TableProvider       │
                           │ FilterPushDown:     │
                           │ - Exact/Inexact     │
                           │ - Unsupported       │
                           └─────────────────────┘
```

### Component Details

#### 1. Discovery System (`discovery.rs`)

Discovers Python datasources via two mechanisms:

1. **Entry Points**: `sail.datasources` group via `importlib.metadata.entry_points()`
2. **Python Registry**: `@register` decorator populates `_REGISTERED_DATASOURCES`

```rust
pub static DATASOURCE_REGISTRY: Lazy<PythonDataSourceRegistry> =
    Lazy::new(PythonDataSourceRegistry::new);

pub struct DataSourceEntry {
    pub name: String,
    pub pickled_class: Vec<u8>,  // GIL-free storage
    pub module_path: String,
}
```

**Security Note**: Cloudpickle can execute arbitrary code. Only load datasources from trusted packages.

#### 2. PythonExecutor Trait (`executor.rs`)

Abstract trait enabling future subprocess isolation:

```rust
#[async_trait]
pub trait PythonExecutor: Send + Sync {
    async fn get_schema(&self, command: &[u8]) -> Result<SchemaRef>;
    async fn get_partitions(&self, command: &[u8]) -> Result<Vec<InputPartition>>;
    async fn execute_read(
        &self,
        command: &[u8],
        partition: &InputPartition,
        schema: SchemaRef,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>>;
}

// MVP: In-process via PyO3
pub struct InProcessExecutor { python_ver: String }

// Future (PR #3): Subprocess isolation via gRPC
// pub struct RemoteExecutor { grpc_client: PythonWorkerClient }
```

#### 3. Filter Pushdown (`filter.rs`)

Converts DataFusion expressions to Python filter objects:

```rust
pub enum PythonFilter {
    // Comparison
    EqualTo { column: ColumnPath, value: FilterValue },
    GreaterThan { column: ColumnPath, value: FilterValue },
    LessThan { column: ColumnPath, value: FilterValue },
    // ... more filters
    
    // Null checks
    IsNull { column: ColumnPath },
    IsNotNull { column: ColumnPath },
    
    // Logical
    Not { child: Box<PythonFilter> },
    And { left: Box<PythonFilter>, right: Box<PythonFilter> },
    Or { left: Box<PythonFilter>, right: Box<PythonFilter> },
    
    // String patterns
    StringStartsWith { column: ColumnPath, value: String },
    StringEndsWith { column: ColumnPath, value: String },
    StringContains { column: ColumnPath, value: String },
}
```

#### 4. Arrow Utilities (`arrow_utils.rs`)

Zero-copy conversions via Arrow C Data Interface:

```rust
// Python RecordBatch -> Rust RecordBatch (zero-copy)
pub fn py_record_batch_to_rust(py: Python<'_>, py_batch: &Bound<'_, PyAny>) -> Result<RecordBatch>;

// Rust Schema -> Python Schema
pub fn rust_schema_to_py(py: Python<'_>, schema: &SchemaRef) -> Result<Py<PyAny>>;

// Row-based batch conversion (for tuple yields)
pub fn convert_rows_to_batch(schema: &SchemaRef, pickled_rows: &[Vec<u8>]) -> Result<RecordBatch>;
```

**MVP Data Types:**
- Numeric: Int32, Int64, Float32, Float64
- String: Utf8
- Boolean
- Temporal: Date32, Timestamp(Microsecond, None)
- Null

#### 5. Execution Plan (`exec.rs`)

DataFusion `ExecutionPlan` implementation:

```rust
#[derive(Debug)]
pub struct PythonDataSourceExec {
    command: Vec<u8>,         // Pickled DataSource instance
    schema: SchemaRef,        // Output schema
    partitions: Vec<InputPartition>,  // Parallel partitions
    properties: PlanProperties,
}

impl ExecutionPlan for PythonDataSourceExec {
    fn execute(&self, partition: usize, _context: Arc<TaskContext>) 
        -> Result<SendableRecordBatchStream> 
    {
        let stream = PythonDataSourceStream::new(
            self.command.clone(),
            self.partitions[partition].clone(),
            self.schema.clone(),
        )?;
        Ok(Box::pin(stream))
    }
}
```

#### 6. Stream with RAII Cleanup (`stream.rs`)

Proper resource management for Python threads:

```rust
enum StreamState {
    Running {
        stop_signal: Option<oneshot::Sender<()>>,  // Graceful shutdown
        python_thread: Option<std::thread::JoinHandle<()>>,  // Thread handle
        rx: mpsc::Receiver<Result<RecordBatch>>,   // Batch channel
    },
    Stopped,
}

impl Drop for PythonDataSourceStream {
    fn drop(&mut self) {
        // 1. Send stop signal
        // 2. Join thread to ensure cleanup
        // Prevents resource leaks - critical for production
    }
}
```

### Python API (`base.py`)

100% PySpark-compatible API:

```python
# Core Classes
class DataSource(ABC):
    def __init__(self, options: Dict[str, str]): ...
    @classmethod
    @abstractmethod
    def name(cls) -> str: ...
    @abstractmethod
    def schema(self) -> str: ...  # DDL string or PyArrow Schema
    @abstractmethod
    def reader(self, schema) -> DataSourceReader: ...

class DataSourceReader(ABC):
    def pushFilters(self, filters: List[Filter]) -> Iterator[Filter]: ...
    def partitions(self) -> List[InputPartition]: ...
    @abstractmethod
    def read(self, partition: InputPartition) -> Iterator[Union[Tuple, pa.RecordBatch]]: ...

class InputPartition:
    def __init__(self, partition_id: int = 0): ...

# Filter Classes (frozen dataclasses)
@dataclass(frozen=True)
class EqualTo:
    column: ColumnPath
    value: Any

# ... EqualNullSafe, GreaterThan, LessThan, In, IsNull, Not, And, Or, etc.
```

### Example DataSources

```python
@register
class RangeDataSource(DataSource):
    """Generates sequential integers with filter pushdown support."""
    
    @classmethod
    def name(cls) -> str:
        return "range"
    
    def schema(self):
        return pa.schema([("id", pa.int64())])
    
    def reader(self, schema) -> DataSourceReader:
        start = int(self.options.get("start", "0"))
        end = int(self.options.get("end", "10"))
        return RangeDataSourceReader(start, end, num_partitions=4)

# Usage:
df = spark.read.format("range").option("end", "1000").load()
```

## Data Flow

### Read Path

1. **Discovery** (Session Startup):
   ```
   discover_datasources() -> DATASOURCE_REGISTRY
   PythonTableFormat.register_all(registry)
   ```

2. **Query Planning**:
   ```
   spark.read.format("range").load()
       -> TableFormatRegistry.get("range")
       -> PythonTableFormat.create_provider()
           -> Instantiate DataSource with options
           -> Get schema from Python
           -> Return PythonTableProvider
   ```

3. **Physical Planning**:
   ```
   PythonTableProvider.scan()
       -> Get partitions from Python reader
       -> Create PythonDataSourceExec
   ```

4. **Execution**:
   ```
   PythonDataSourceExec.execute(partition_id)
       -> Create PythonDataSourceStream
           -> Spawn Python thread
           -> Python: reader.read(partition) yields RecordBatches
           -> Zero-copy transfer via mpsc channel
       -> DataFusion consumes RecordBatchStream
   ```

5. **Cleanup**:
   ```
   Stream dropped
       -> Send stop signal
       -> Join Python thread
       -> Resources released
   ```

## Testing

### Unit Tests

| Component | Test Coverage |
|-----------|--------------|
| Registry operations | `discovery.rs` |
| Filter conversion | `filter.rs` |
| Schema validation | `arrow_utils.rs` |
| Execution plan properties | `exec.rs` |
| Stream lifecycle | `stream.rs` |

### Integration Tests

Location: `python/pysail/tests/spark/datasource/`

- `test_rate.py` - Rate source streaming
- `test_socket.py` - Socket source streaming
- `test_datasources.py` - Custom datasource testing

### Example DataSources

| DataSource | Purpose | Features |
|------------|---------|----------|
| `RangeDataSource` | Synthetic integers | Partitioning, filter pushdown |
| `ConstantDataSource` | Fixed values | Basic schema handling |
| `FlappyBirdDataSource` | Fun demo | Multiple columns, types |

## Performance Considerations

### GIL Management

**Current (MVP)**: In-process PyO3 execution
- Control plane (schema, partitions): GIL acquired briefly
- Data plane: Zero-copy via Arrow C Data Interface
- NumPy/PyArrow release GIL during compute

**Future (PR #3)**: Subprocess isolation
- 1 Python subprocess per worker thread
- Full GIL parallelism
- Shared memory for data plane

### Batch Size

Default: `DEFAULT_BATCH_SIZE = 8192`

Configurable per-datasource for optimal throughput.

## Security Considerations

### Cloudpickle Trust Model

- **Warning**: Cloudpickle can execute arbitrary code during deserialization
- Only load datasources from trusted packages
- Entry points provide implicit trust (installed packages)
- `@register` decorator requires explicit code execution

### Validation

```rust
pub fn validate_datasource_class(py: Python<'_>, cls: &Bound<'_, PyAny>) -> Result<()> {
    // Check required methods: name, schema, reader
    // Verify it's callable (is a class, not instance)
}
```

## Migration Guide

### From PySpark

**Zero changes required.** PySpark DataSource implementations work as-is:

```python
# Works identically in PySpark and Sail
from pysail.spark.datasource import DataSource, DataSourceReader, InputPartition

class MyDataSource(DataSource):
    @classmethod
    def name(cls):
        return "myformat"
    
    def schema(self):
        return "id INT, name STRING"
    
    def reader(self, schema):
        return MyReader(self.options)
```

### Registration Methods

1. **Decorator** (recommended):
   ```python
   @register
   class MyDataSource(DataSource): ...
   ```

2. **Entry Points** (`pyproject.toml`):
   ```toml
   [project.entry-points."sail.datasources"]
   myformat = "mypackage:MyDataSource"
   ```

## Future Work

### PR #2: Batch Write Support (~550 LOC)

- `PythonDataSourceWriteExec` execution plan
- `PythonDataSourceWriteStream` for row-by-row writes
- `PythonDataSourceCommitExec` for two-phase commit
- Python: `DataSourceWriter`, `WriterCommitMessage`

### PR #3: Subprocess Isolation (~800 LOC)

- `RemoteExecutor` implementing `PythonExecutor` trait
- `PythonWorkerPool` for process management
- Shared memory data plane (mmap)
- gRPC control plane

#### Protobuf Architecture

Sail already uses Protocol Buffers for distributed execution. The subprocess isolation will leverage this existing infrastructure:

**Existing Proto Files:**
- `sail-execution/proto/sail/driver/service.proto` - Driver↔Worker coordination
- `sail-execution/proto/sail/worker/service.proto` - Task execution
- `sail-common-datafusion/proto/sail/streaming/marker.proto` - Flow markers (Watermark, Checkpoint)

**Proposed Python Worker Service:**

```protobuf
syntax = "proto3";
package sail.python;

import "google/protobuf/empty.proto";

// Service for Python worker subprocess management
service PythonWorkerService {
  // Control plane (low bandwidth, high frequency)
  rpc GetSchema(GetSchemaRequest) returns (GetSchemaResponse);
  rpc GetPartitions(GetPartitionsRequest) returns (GetPartitionsResponse);
  rpc PushFilters(PushFiltersRequest) returns (PushFiltersResponse);

  // Data plane coordination (actual data via shared memory)
  rpc ReadPartition(ReadPartitionRequest) returns (stream DataReadyNotification);
  rpc WritePartition(WritePartitionRequest) returns (WritePartitionResponse);

  // Lifecycle management
  rpc HealthCheck(google.protobuf.Empty) returns (HealthCheckResponse);
  rpc Shutdown(ShutdownRequest) returns (google.protobuf.Empty);
}

message GetSchemaRequest {
  bytes pickled_datasource = 1;  // Cloudpickle'd DataSource instance
}

message GetSchemaResponse {
  bytes arrow_schema_ipc = 1;    // Arrow IPC format schema
}

message ReadPartitionRequest {
  bytes pickled_datasource = 1;
  bytes pickled_partition = 2;   // InputPartition
  uint64 batch_size = 3;
}

// Zero-copy notification: data is in shared memory
message DataReadyNotification {
  uint64 shared_memory_offset = 1;  // Offset in mmap'd region
  uint64 length = 2;                // Bytes written
  bool is_last = 3;                 // End of partition
}

message HealthCheckResponse {
  bool healthy = 1;
  string python_version = 2;
  uint64 memory_usage_bytes = 3;
}
```

**Shared Memory Data Plane:**

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          CONTROL PLANE (gRPC)                           │
│  Low bandwidth, high frequency - commands, metadata, coordination       │
│                                                                         │
│  ┌─────────────────┐     Protobuf/gRPC      ┌─────────────────────┐    │
│  │   Sail (Rust)   │◀──────────────────────▶│   Python Worker     │    │
│  │   RemoteExecutor│   "Read partition 5"   │   (subprocess)      │    │
│  └─────────────────┘   "Data at offset 500" └─────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              │ offset/length references
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA PLANE (Shared Memory)                     │
│  High bandwidth, zero-copy - Arrow RecordBatch buffers                  │
│                                                                         │
│  ┌─────────────────┐                        ┌─────────────────────┐    │
│  │   Arrow Reader  │◀─────── mmap ─────────▶│   Arrow Writer      │    │
│  │   (Rust)        │   /dev/shm/sail_py_*   │   (Python)          │    │
│  └─────────────────┘                        └─────────────────────┘    │
│                                                                         │
│  Buffer Layout:                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ Offset 0    │ Offset 4096  │ Offset 8192  │ ...                  │  │
│  │ RecordBatch │ RecordBatch  │ RecordBatch  │                      │  │
│  │ (Arrow IPC) │ (Arrow IPC)  │ (Arrow IPC)  │                      │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

**Why This Architecture:**

| Aspect | In-Process (MVP) | Subprocess (PR #3) |
|--------|------------------|-------------------|
| **Parallelism** | 1 GIL total | N GILs (1 per worker) |
| **Crash Isolation** | Python crash = Sail crash | Worker crash only |
| **Data Transfer** | Zero-copy (same RAM) | Zero-copy (mmap) |
| **Control Messages** | FFI (nanoseconds) | gRPC (~microseconds) |
| **Complexity** | Simple | Higher |

**Configuration:**
```bash
SAIL_PYTHON_ISOLATION_MODE=subprocess  # or "in_process" (default)
SAIL_PYTHON_WORKER_COUNT=auto          # defaults to CPU count
SAIL_SHARED_MEMORY_SIZE=8GB            # mmap pool size
```

#### Multi-Faceted Benefits of Subprocess Isolation

Subprocess isolation provides value beyond just GIL parallelism:

| Benefit | Description | Remains Valuable With Free Threading? |
|---------|-------------|--------------------------------------|
| **GIL Parallelism** | N workers = N GILs, true concurrent Python execution | ❌ No (free threading eliminates GIL) |
| **Crash Isolation** | Python segfault only kills worker, not Sail server | ✅ Yes - critical for production |
| **Memory Isolation** | Per-worker memory limits via `RLIMIT_AS` | ✅ Yes - prevents OOM cascades |
| **Version Isolation** | Different workers can use different Python versions/deps | ✅ Yes - legacy datasource support |
| **Security Sandboxing** | Untrusted datasources run in isolated process | ✅ Yes - limits attack surface |
| **Resource Limits** | CPU/time limits per worker via `rlimit` | ✅ Yes - fair resource allocation |

**Key Insight**: Even when Python's free-threading mode (PEP 703) becomes stable, subprocess isolation remains valuable for production deployments where crash isolation and resource limits are requirements.

#### Free Threading (PEP 703) Considerations

**Current Status (2025)**:
- Python 3.13+ includes experimental `--disable-gil` build flag
- Free threading is **not production-ready**
- Ecosystem support is evolving (PyO3, NumPy, PyArrow)

**Projected Timeline**:
```
2024-2025: Experimental, ecosystem catching up
2026: Likely stable in Python, ecosystem maturing
2027+: Ecosystem mature (PyO3, NumPy, PyArrow stable)
```

**Adaptive Executor Selection**:

The `PythonExecutor` trait enables runtime adaptation to free threading:

```rust
pub fn create_executor(config: &Config) -> Result<Arc<dyn PythonExecutor>> {
    match config.isolation_mode {
        IsolationMode::InProcess => {
            // Detect free threading at runtime
            if is_free_threading_enabled() {
                log::info!("Free threading detected - InProcessExecutor with full parallelism");
            } else {
                log::info!("GIL detected - InProcessExecutor has single-GIL limitation");
            }
            Arc::new(InProcessExecutor::new(config.python_version.clone()))
        }
        IsolationMode::Subprocess => {
            // Always use subprocess (crash/memory isolation benefits)
            log::info!("Subprocess isolation enabled - crash and memory isolation active");
            Arc::new(RemoteExecutor::new(config)?)
        }
    }
}

fn is_free_threading_enabled() -> bool {
    #[cfg(feature = "python")]
    {
        pyo3::Python::with_gil(|py| {
            // Check sys._is_gil_enabled() (available in Python 3.13+)
            if let Ok(sys) = py.import("sys") {
                if let Ok(is_gil_enabled) = sys.getattr("_is_gil_enabled") {
                    if let Ok(false) = is_gil_enabled.call0()?.extract() {
                        return true;
                    }
                }
            }
            false
        })
    }
    #[cfg(not(feature = "python"))]
    { false }
}
```

**Recommendation**: Subprocess isolation should be the default for production workloads even after free threading stabilizes, with in-process mode available for development/trusted environments.

### PR #4-5: Streaming Support (~1,250 LOC)

- `PythonDataSourceStreamExec` for streaming reads
- `DataSourceStreamReader` with offset management
- Microbatch writes with BatchId coordination

### PR #6: Performance & Polish (~250 LOC)

- Exotic data types (Duration, Interval, Union, Dictionary)
- Metrics/telemetry integration
- Comprehensive documentation

### Database Connectors via Python DataSources

Python libraries like [connector-x](https://github.com/sfu-db/connector-x) and [DuckDB](https://duckdb.org/) provide excellent database connectivity with minimal GIL overhead. Their Rust/C++ backends release the GIL during I/O and compute, making them ideal for Python DataSource implementations.

#### Why These Libraries Work Well

| Library | Backend | GIL Behavior | Arrow Support | Use Case |
|---------|---------|--------------|---------------|----------|
| **connector-x** | Rust | Releases GIL during query | Native Arrow output | Multi-database (PostgreSQL, MySQL, SQLite, etc.) |
| **DuckDB** | C++ | Releases GIL during query | Native Arrow output | Embedded analytics, Parquet/CSV |
| **turbodbc** | C++ | Releases GIL during fetch | Arrow via `fetchallarrow()` | ODBC databases |
| **psycopg3** | C | Releases GIL during I/O | Via PyArrow conversion | PostgreSQL |

**Key Insight**: These libraries spend most execution time in native code with GIL released. The Python DataSource wrapper adds minimal overhead since:
- Schema discovery: One-time GIL acquisition (~10ms)
- Data transfer: Native code releases GIL, returns Arrow directly
- No JVM: Unlike Spark JDBC, zero JVM overhead

#### connector-x DataSource Example

```python
from pysail.spark.datasource import DataSource, DataSourceReader, InputPartition, register
import connectorx as cx
import pyarrow as pa

@register
class ConnectorXDataSource(DataSource):
    """High-performance database reads via connector-x (Rust backend)."""

    @classmethod
    def name(cls) -> str:
        return "connectorx"

    def schema(self):
        # connector-x can infer schema from a LIMIT 0 query
        url = self.options["url"]
        query = self.options["query"]
        df = cx.read_sql(url, f"SELECT * FROM ({query}) t LIMIT 0", return_type="arrow")
        return df.schema

    def reader(self, schema):
        return ConnectorXReader(
            url=self.options["url"],
            query=self.options["query"],
            partition_on=self.options.get("partition_on"),
            partition_num=int(self.options.get("partition_num", "4"))
        )

class ConnectorXReader(DataSourceReader):
    def __init__(self, url, query, partition_on=None, partition_num=4):
        self.url = url
        self.query = query
        self.partition_on = partition_on
        self.partition_num = partition_num

    def partitions(self):
        if self.partition_on:
            # connector-x handles partitioning internally
            return [InputPartition(i) for i in range(self.partition_num)]
        return [InputPartition(0)]

    def read(self, partition):
        # GIL released during cx.read_sql() - Rust does the work
        if self.partition_on:
            table = cx.read_sql(
                self.url, self.query,
                partition_on=self.partition_on,
                partition_num=self.partition_num,
                return_type="arrow"
            )
            # Yield only this partition's chunk
            chunk_size = len(table) // self.partition_num
            start = partition.value * chunk_size
            end = start + chunk_size if partition.value < self.partition_num - 1 else len(table)
            yield table.slice(start, end - start)
        else:
            table = cx.read_sql(self.url, self.query, return_type="arrow")
            yield table

# Usage:
# df = spark.read.format("connectorx") \
#     .option("url", "postgresql://user:pass@localhost/db") \
#     .option("query", "SELECT * FROM users WHERE active = true") \
#     .option("partition_on", "id") \
#     .option("partition_num", "8") \
#     .load()
```

#### DuckDB DataSource Example

```python
@register
class DuckDBDataSource(DataSource):
    """Embedded analytics via DuckDB (C++ backend, Arrow-native)."""

    @classmethod
    def name(cls) -> str:
        return "duckdb"

    def schema(self):
        import duckdb
        conn = duckdb.connect(self.options.get("database", ":memory:"))
        query = self.options["query"]
        return conn.execute(f"DESCRIBE ({query})").arrow().schema

    def reader(self, schema):
        return DuckDBReader(
            database=self.options.get("database", ":memory:"),
            query=self.options["query"]
        )

class DuckDBReader(DataSourceReader):
    def read(self, partition):
        import duckdb
        conn = duckdb.connect(self.database, read_only=True)
        # GIL released during execute() - C++ does the work
        # fetch_arrow_table() returns Arrow directly, no conversion
        result = conn.execute(self.query).fetch_arrow_table()
        yield result

# Usage:
# df = spark.read.format("duckdb") \
#     .option("database", "/path/to/analytics.duckdb") \
#     .option("query", "SELECT * FROM events WHERE date > '2024-01-01'") \
#     .load()
```

#### Performance Characteristics

| Operation | Pure Python | connector-x/DuckDB | Notes |
|-----------|-------------|-------------------|-------|
| 1M rows fetch | ~5s (GIL-bound) | ~0.5s (GIL-free) | 10x faster |
| Arrow conversion | Required | Not needed | Zero-copy |
| Memory overhead | 2x (Python + Arrow) | 1x (Arrow only) | Half memory |
| Parallel partitions | GIL contention | Minimal contention | Native parallelism |

#### Future: Native Rust Integration

For maximum performance, native Rust database connectors can be added directly to Sail's execution engine, bypassing Python entirely:

| Phase | Approach | Status |
|-------|----------|--------|
| **MVP** | Python DataSources with connector-x/DuckDB | Complete |
| **Future** | Native Rust `TableFormat` for PostgreSQL, MySQL | Planned |
| **Future** | Arrow Flight integration | Planned |

## Frequently Asked Questions

### Architecture & Performance

**Q1: Why use in-process PyO3 instead of subprocess isolation?**

The MVP uses in-process execution for simplicity and zero-copy performance. PyO3 allows direct memory sharing between Rust and Python via the Arrow C Data Interface. Subprocess isolation (PR #3) adds complexity but enables true GIL parallelism. The `PythonExecutor` trait abstracts this choice, allowing seamless switching via configuration.

**Q2: How does the GIL affect parallel partition reads?**

In the MVP, all Python code shares a single GIL. However, this is mitigated by:
- **Arrow/NumPy releasing GIL**: Most compute-heavy operations release the GIL
- **Thread-per-partition model**: Each partition runs on a dedicated OS thread
- **Minimal Python in hot path**: Control plane (schema, partitions) is cached; data plane uses zero-copy Arrow

With subprocess isolation (PR #3), each worker has its own GIL, enabling true parallelism.

**Q3: What's the overhead of Python DataSources vs native formats?**

| Operation | Native (Parquet) | Python DataSource |
|-----------|------------------|-------------------|
| Schema discovery | ~1ms | ~10-50ms (GIL acquisition) |
| Partition enumeration | ~1ms | ~10-50ms |
| Batch read (Arrow) | Zero-copy | Zero-copy |
| Batch read (tuples) | N/A | ~100μs per 8K rows |

The overhead is primarily in the control plane. Data transfer is zero-copy when using Arrow RecordBatches.

**Q4: When should I use Python DataSources vs native Rust connectors?**

Use **Python DataSources** when:
- Rapid prototyping or one-off integrations
- Leveraging existing Python libraries (requests, boto3, etc.)
- Complex business logic that's easier in Python
- Migrating from PySpark with existing DataSource implementations

Use **Native Rust connectors** when:
- Maximum performance is critical
- Database connectors (PostgreSQL, MySQL, DuckDB)
- High-throughput production workloads
- GIL contention is a bottleneck

**Q5: How does zero-copy Arrow transfer work?**

When Python yields a `pyarrow.RecordBatch`:
1. Python allocates Arrow buffers in its memory space
2. The Arrow C Data Interface exports pointers (not copies) to Rust
3. Rust's `arrow-pyarrow` crate wraps these pointers as Rust `RecordBatch`
4. DataFusion processes the batch without copying data
5. When Rust drops the batch, Python's reference count decreases

This is implemented in `arrow_utils.rs` using `RecordBatch::from_pyarrow_bound()`.

**Q6: Can NumPy/Pandas release the GIL during compute?**

Yes. NumPy, PyArrow, and Polars release the GIL during most compute operations:
- `numpy.sum()`, `numpy.dot()` - GIL released
- `pyarrow.compute.*` - GIL released
- `polars.DataFrame.filter()` - GIL released

This means compute-heavy Python datasources can achieve good parallelism even in-process. Only pure Python loops hold the GIL continuously.

### Developer Guide

**Q7: How do I create and register a custom DataSource?**

```python
from pysail.spark.datasource import DataSource, DataSourceReader, InputPartition, register

@register  # Automatic registration
class MyApiDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "myapi"

    def schema(self):
        return "id BIGINT, name STRING, value DOUBLE"

    def reader(self, schema):
        return MyApiReader(self.options["endpoint"])

class MyApiReader(DataSourceReader):
    def __init__(self, endpoint):
        self.endpoint = endpoint

    def partitions(self):
        # Return list of partitions for parallel reading
        return [InputPartition(i) for i in range(4)]

    def read(self, partition):
        # Yield tuples or pyarrow.RecordBatch
        import requests
        data = requests.get(f"{self.endpoint}?partition={partition.value}").json()
        for row in data:
            yield (row["id"], row["name"], row["value"])

# Usage:
df = spark.read.format("myapi").option("endpoint", "https://api.example.com").load()
```

**Q8: What's the difference between tuple and RecordBatch yield?**

| Yield Type | Performance | Memory | Use Case |
|------------|-------------|--------|----------|
| **Tuples** | ~100μs/8K rows overhead | Batched in `RowBatchCollector` | Simple implementations |
| **RecordBatch** | Zero-copy | Direct Arrow transfer | High-performance, PyArrow-native |

Prefer `RecordBatch` for large datasets; use tuples for prototyping or when data comes row-by-row.

**Q9: How do I implement filter pushdown in my DataSource?**

```python
from pysail.spark.datasource import DataSourceReader, EqualTo, GreaterThan

class MyReader(DataSourceReader):
    def __init__(self):
        self.pushed_filters = []

    def pushFilters(self, filters):
        for f in filters:
            if isinstance(f, (EqualTo, GreaterThan)):
                self.pushed_filters.append(f)
            else:
                yield f  # Return unsupported filters

    def read(self, partition):
        # Apply self.pushed_filters to reduce data at source
        query = build_query(self.pushed_filters)
        for row in execute_query(query):
            yield row
```

**Q10: How do partitions work in distributed execution?**

1. `reader.partitions()` returns a list of `InputPartition` objects
2. Each partition is pickled and sent to a worker
3. Workers call `reader.read(partition)` in parallel
4. Results are combined by DataFusion

For optimal parallelism, create partitions based on data locality or range boundaries.

**Q11: How do I debug filter pushdown issues?**

Enable debug logging to see which filters are pushed:
```bash
RUST_LOG=sail_data_source::python_datasource::filter=debug sail spark
```

In Python, log received filters:
```python
def pushFilters(self, filters):
    import logging
    logging.info(f"Received filters: {filters}")
    # ... process filters
```

**Q12: How do I add metrics/telemetry to my DataSource?**

Currently, metrics are captured at the DataFusion level. In PR #6, we'll add:
```python
class MyReader(DataSourceReader):
    def read(self, partition):
        with self.metrics.timer("read_time"):
            for row in self._fetch_data():
                self.metrics.counter("rows_read").inc()
                yield row
```

### Migration & Compatibility

**Q13: Can I use existing PySpark DataSource code without changes?**

Yes. Sail provides 100% API compatibility with PySpark 3.5+/4.0+ DataSource API. Simply change the import:

```python
# PySpark
from pyspark.sql.datasource import DataSource, DataSourceReader

# Sail (no other changes needed)
from pysail.spark.datasource import DataSource, DataSourceReader
```

**Q14: What PySpark API version is supported?**

- **PySpark 3.5+**: Full batch read/write support
- **PySpark 4.0+**: Streaming DataSource API (coming in PR #4-5)
- **Python**: 3.9, 3.10, 3.11, 3.12

**Q15: What data types are supported in MVP vs future PRs?**

| PR | Types Added |
|----|-------------|
| **#1 (MVP)** | Int32, Int64, Float32, Float64, Utf8, Boolean, Date32, Timestamp, Null |
| **#2 (Write)** | Binary, Decimal128, Int8, Int16 |
| **#4 (Stream)** | List<T>, Struct, Map<K,V>, LargeUtf8 |
| **#6 (Polish)** | Duration, Interval, FixedSizeBinary, Union, Dictionary |

**Q16: How do I migrate from Spark's JDBC connector?**

Option 1: Use Python DataSource with connector-x:
```python
@register
class JdbcDataSource(DataSource):
    def reader(self, schema):
        import connectorx as cx
        return ConnectorXReader(self.options["url"], self.options["query"])
```

Option 2: Wait for native Rust connectors (see Database Connectors section).

### Security & Operations

**Q17: Is cloudpickle safe? What's the trust model?**

**Warning**: Cloudpickle can execute arbitrary code during deserialization.

Trust model:
- **Entry points** (`sail.datasources`): Implicitly trusted - requires package installation
- **@register decorator**: Requires explicit code execution in Python
- **Dynamic loading**: Validate with `validate_datasource_class()` before use

Only load datasources from trusted packages. Never unpickle datasources from untrusted sources.

**Q18: What happens if Python code crashes during read?**

In **MVP (in-process)**:
- Python exception → `DataFusionError::External`
- Query fails gracefully with error message
- Python segfault → Sail process crashes (use subprocess isolation for safety)

With **subprocess isolation (PR #3)**:
- Worker crash → Worker restarted, partition retried
- No impact on main Sail process

**Q19: When will subprocess isolation be available?**

Subprocess isolation is planned for **PR #3** (Phase 3 in the implementation plan). It provides multiple benefits beyond just GIL parallelism:

| Benefit | Description |
|---------|-------------|
| **GIL Parallelism** | N workers = N GILs, true concurrent Python execution |
| **Crash Isolation** | Worker crash doesn't affect Sail server (production-critical) |
| **Memory Isolation** | Per-worker memory limits prevent OOM cascades |
| **Version Isolation** | Different workers can use different Python versions/deps |
| **Security Sandboxing** | Untrusted datasources run in isolated process |

**Note on Free Threading**: Even when Python's free-threading mode (PEP 703) becomes stable (expected 2026+), subprocess isolation remains valuable for crash isolation, memory limits, and security. The `PythonExecutor` trait allows runtime adaptation—Sail will detect free threading and can use `InProcessExecutor` when appropriate, while still offering subprocess mode for production safety.

**Q20: How do I configure batch size and other tuning parameters?**

```python
class MyReader(DataSourceReader):
    def __init__(self):
        self.batch_size = 8192  # Default, tune based on row size

    def read(self, partition):
        batch = []
        for row in self._fetch_rows():
            batch.append(row)
            if len(batch) >= self.batch_size:
                yield pyarrow.RecordBatch.from_pydict(...)
                batch = []
```

Environment variables:
```bash
SAIL_PYTHON_BATCH_SIZE=16384           # Rows per batch
SAIL_PYTHON_CHANNEL_BUFFER=4           # Batches buffered in channel
```

## Appendix

### File Inventory

#### Rust (`crates/sail-data-source/src/python_datasource/`)

| File | Lines | Purpose |
|------|-------|---------|
| `mod.rs` | 55 | Module exports |
| `discovery.rs` | 331 | Registry & discovery |
| `filter.rs` | 457 | Filter pushdown |
| `arrow_utils.rs` | 342 | Arrow conversions |
| `executor.rs` | 191 | PythonExecutor trait |
| `python_datasource.rs` | 384 | Core PyO3 bridge |
| `python_table_provider.rs` | 115 | TableProvider impl |
| `table_format.rs` | 213 | TableFormat impl |
| `exec.rs` | 197 | ExecutionPlan |
| `stream.rs` | 289 | RecordBatch stream |
| `error.rs` | 52 | Error types |
| **Total** | **~2,626** | |

#### Python (`python/pysail/spark/datasource/`)

| File | Lines | Purpose |
|------|-------|---------|
| `base.py` | 444 | Core API classes |
| `examples.py` | 379 | Example datasources |
| `__init__.py` | ~20 | Module exports |
| **Total** | **~843** | |

### Commit History

```
456fb6dc fix: add Python 3.9 compatibility for entry_points discovery
4754828c fix: Python DataSource discovery and reader fixes
37ae75fc fix: Python DataSource discovery and reader fixes
519e2d9d feat: Python Datasource MVP (PR #1)
```

## Implementation Phases

This RFC describes **Phase 1** of a 6-phase rollout strategy:

| Phase | Focus | Scope | Status |
|-------|-------|-------|--------|
| **Phase 1 (This RFC)** | MVP Batch Read | Discovery, filter pushdown, execution, ~2,626 LOC Rust + 843 LOC Python | ✅ Complete |
| **Phase 2** | Batch Write | Write execution plan, commit protocol, ~550 LOC | Planned |
| **Phase 3** | Subprocess Isolation | GRPC executor, shared memory, crash isolation, ~800 LOC | Planned |
| **Phase 4** | Streaming Read | Offset management, microbatch coordination, ~650 LOC | Planned |
| **Phase 5** | Streaming Write | Streaming write exec, BatchId protocol, ~600 LOC | Planned |
| **Phase 6** | Performance & Polish | Exotic types, metrics, documentation, ~250 LOC | Planned |

**Total Estimated Scope**: ~5,476 lines of code across 6 incremental deliveries.

## References

- [PySpark DataSource API](https://github.com/apache/spark/blob/master/python/pyspark/sql/datasource.py) - Reference implementation for 100% compatibility
- [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html) - Zero-copy specification
- [PyO3 Guide](https://pyo3.rs/) - Rust-Python interop
- [PEP 703 – Making the Global Interpreter Lock Optional](https://peps.python.org/pep-0703/) - Free threading considerations for future Sail versions
- [Connector-X](https://github.com/sfu-db/connector-x) - High-performance database connector example
- [DuckDB Python API](https://duckdb.org/docs/api/python/overview) - Embedded analytics integration example
