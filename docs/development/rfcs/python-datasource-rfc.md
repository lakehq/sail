# RFC: Python DataSource Support

**Status:** Phase 1 Complete (MVP Read-Only)
**Branch:** `feat/python-datasource-write`
**PR:** #1 - MVP Batch Read
**Authors:** Santosh
**Date:** 2025-12-31

## Executive Summary

This RFC documents Python DataSource support in Sail, enabling users to implement custom data sources in Python for AI and data engineering workflows while leveraging Sail's distributed execution engine. The implementation provides **100% API compatibility** with the PySpark 4.0+ DataSource API, allowing existing PySpark DataSource implementations to work without modification.

**Key Benefits:**
- **Zero-friction PySpark migration**: Existing DataSource code works unchanged
- **High performance**: In-process Python execution via [PyO3](https://lakesail.com/blog/sail-and-python-udfs) with zero-serialization Arrow DataSource buffers—no inter-process boundary
- **Distributed execution**: Parallel partition-based reading across workers
- **Low barrier to entry**: Python developers can extend Sail without Rust knowledge
- **Ecosystem leverage**: Access to 350,000+ PyPI packages

**Phase 1 (MVP)** delivers read-only batch support with 5 additional phases planned for write, subprocess isolation, streaming, and performance optimization.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Motivation](#motivation)
3. [Design Decisions](#design-decisions)
4. [Drawbacks](#drawbacks)
5. [Unresolved Questions](#unresolved-questions)
6. [Architecture](#architecture)
7. [API Reference](#api-reference)
8. [Implementation Details](#implementation-details)
9. [PySpark Compatibility](#pyspark-compatibility)
10. [Performance Considerations](#performance-considerations)
11. [Future Work](#future-work)
12. [FAQ](#faq)
13. [Appendix](#appendix)

> **Reference Guide**: See [python-datasource-reference.md](./python-datasource-reference.md) for complete examples, FAQ, and detailed tables.

---

## Quick Start

### Prerequisites

- Python 3.9-3.12
- PyArrow library: `pip install pyarrow`

### Creating a DataSource

```python
from pysail.spark.datasource import DataSource, DataSourceReader, InputPartition, register
import pyarrow as pa

@register
class MyDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "mydatasource"

    def schema(self):
        return pa.schema([("id", pa.int64()), ("name", pa.string())])

    def reader(self, schema):
        return MyDataSourceReader(self.options)

class MyDataSourceReader(DataSourceReader):
    def partitions(self):
        return [InputPartition(0)]

    def read(self, partition):
        # Yield tuples or pyarrow.RecordBatch
        yield (1, "Alice"), (2, "Bob")
```

### Using the DataSource

```python
df = spark.read.format("mydatasource").load()
df.show()
```

### Distribution via Entry Points

For package distribution, register via `pyproject.toml`:

```toml
[project]
name = "my-datasource-package"
version = "1.0.0"
dependencies = ["pyarrow"]

[project.entry-points."sail.datasources"]
mydatasource = "my_package.datasources:MyDataSource"
```

After `pip install -e .`, the DataSource is automatically discovered.

---

## Motivation

### Problem Statement

Modern data systems face a tension between **extensibility** and **maintainability**:

- **Code bloat**: Adding every connector to the core makes the framework harder to maintain
- **Release coupling**: New connectors tied to framework release cycles
- **Security surface**: Every integration increases attack surface
- **Barrier to contribution**: Contributors must navigate complex build systems

### The Plugin Model Solution

Python DataSource support provides a **composable plugin architecture** with PySpark compatibility as the primary driver:

- **PySpark migration**: Reuse existing DataSource implementations with zero code changes
- **Decentralized innovation**: Community builds and distributes connectors independently
- **Risk isolation**: Third-party code runs in user space, not embedded in core
- **Rapid iteration**: Connector updates don't require Sail releases
- **Simplified FFI**: Avoids complex native FFI while providing extension capability

### Why Python?

See [FAQ: Why Python for DataSources?](#q15-why-python-for-datasources)

### Goals

- 100% PySpark API compatibility
- High performance via Arrow zero-copy
- Distributed execution with partition parallelism
- Filter pushdown infrastructure (activated in future phases)
- Extensible design for subprocess isolation

### Non-Goals (Deferred)

- Write support → Phase 2
- Subprocess isolation → Phase 3
- Streaming read/write → Phase 4-5
- Exotic data types → Phase 6

---

## Design Decisions

This section documents key design decisions, distinguishing between:
- **True alternatives**: Mutually exclusive choices where we picked one
- **Phased implementations**: Sequential steps where later phases build on earlier ones
- **Requirements**: Constraints where the only choice is *how* to meet them

### 1. Python Execution Model (Phased)

Sail must execute Python datasource code. These are **phases**, not competing alternatives:

| Phase | Approach | Description | Status |
|-------|----------|-------------|--------|
| **Phase 1** | PyO3 in-process | Python runs in Sail process; PyO3 manages GIL | MVP |
| **Phase 3** | Subprocess workers | Python in separate processes; gRPC control + shared memory data | Planned |

**Why in-process for MVP**:
- Simpler deployment (no worker process management)
- Zero-copy Arrow transfer (same process = direct pointer access)
- Sufficient for trusted datasources
- Sail's Rust-native architecture enables [PyO3](https://lakesail.com/blog/sail-and-python-udfs) to share memory directly with the execution engine—a capability impossible in JVM-based systems

**What changes in Phase 3**:
- Control plane: gRPC for schema/partition requests
- Data plane: Shared memory for Arrow buffers (still zero-copy)
- Benefit: Crash isolation + N GILs for parallelism

**Not competing**: Phase 3 builds on Phase 1's design. The `PythonExecutor` trait abstracts the difference, allowing seamless migration.

### 2. PySpark Compatibility Strategy (Requirement)

PySpark DataSource compatibility is a **requirement**, not a choice. When PySpark datasources are pickled, they reference `pyspark.sql.datasource.DataSource`. Sail includes `pysail` but not `pyspark`, so we must bridge this gap.

We considered *how* to achieve this:

| Approach | How It Works | Decision |
|----------|--------------|----------|
| **sys.modules shim** | Inject `pyspark.sql.datasource` module that re-exports pysail classes | Chosen |
| **Require pysail imports** | Users change imports from `pyspark.sql.datasource` to `pysail.spark.datasource` | Rejected (breaks zero-code-change goal) |
| **Install PySpark** | Include full `pyspark` as dependency | Rejected (too heavy: ~300MB, JVM overhead) |

**Why the shim**: Enables existing PySpark DataSource code to work without modification. Users don't change imports or code—Sail handles the namespace translation.

**Implementation**: Shim is initialized at Sail startup. When unpickling, Python looks up `pyspark.sql.datasource` in `sys.modules` and gets the pysail implementation.

### 3. Data Transfer (Context-Dependent)

| Mode | Transfer Method | When Used |
|------|-----------------|-----------|
| **In-process (Phase 1)** | Arrow C Data Interface | Direct pointer passing, zero-copy |
| **Subprocess (Phase 3)** | Arrow IPC over shared memory | Zero-copy across process boundary |

**Not competing**: Same goal (zero-copy), different mechanisms for different execution modes. Both use Arrow's zero-copy interfaces—C Data works within one process (pointer access), IPC works across processes (shared memory mmap).

### 4. Serialization Format (True Alternative)

| Alternative | Pros | Cons | Decision |
|-------------|------|------|----------|
| **cloudpickle** | Pickles lambdas/closures, PySpark compatible | - | Chosen |
| **dill** | Similar to cloudpickle | Less maintained, PySpark uses cloudpickle | Rejected |
| **pickle** | Standard library | Cannot pickle lambdas/closures | Rejected |
| **JSON + class registry** | Safe, inspectable | Cannot serialize arbitrary Python objects | Rejected |

**Rationale**: PySpark uses cloudpickle; using the same ensures zero-friction migration and compatibility.

### 5. Filter Representation (True Alternative)

| Alternative | Pros | Cons | Decision |
|-------------|------|------|----------|
| **Frozen dataclasses** | Immutable, hashable, PySpark compatible | Requires Python 3.7+ | Chosen |
| **Named tuples** | Lightweight, immutable | Less extensible, no methods | Rejected |
| **Protobuf messages** | Language-neutral, schema evolution | Adds dependency, overkill for Python-only | Rejected |
| **Raw dictionaries** | Flexible | No type safety, error-prone | Rejected |

### 6. Extension Mechanism: Python vs Pure FFI (True Alternative)

| Alternative | Pros | Cons | Decision |
|-------------|------|------|----------|
| **Python DataSource API** | Low barrier to entry, huge ecosystem, PySpark compatibility | GIL limitations (mitigated by subprocess), runtime overhead | Chosen |
| **Pure C FFI (dlopen)** | Maximum performance, no runtime dependency | High barrier to entry, unsafe, no ecosystem leverage | Rejected for user-facing |
| **WebAssembly (WASM)** | Sandboxing, multi-language | Immature data ecosystem, limited Arrow support | Future consideration |

**Why we rejected Pure FFI for user-facing extensions:**

Pure C FFI creates an **unreasonably high barrier to entry** for data engineers:

- **Technical Complexity**: Manual memory management, ABI compatibility issues, no type safety (mistakes = segfaults)
- **Toolchain Burden**: Requires C/C++/Rust compiler, platform-specific builds, cross-compilation testing
- **Ecosystem Fragmentation**: No standard HTTP/JSON/cloud libraries in C, security vulnerabilities in hand-rolled code

**Historical Evidence**: Systems that relied on pure FFI for extensibility (e.g., Postgres prior to PL/Python, early databases with C UDFs) saw limited community adoption compared to higher-level language bindings.

**When Pure FFI Makes Sense**: For **performance-critical native connectors** (PostgreSQL, Parquet, Arrow Flight), Sail provides direct Rust `TableFormat` implementations. Pure FFI is reserved for internal use, not user-facing extensions.

**Python's balance**: Accessible for users, performant via Arrow zero-copy, safe via subprocess isolation.

---

## Drawbacks

### 1. GIL Contention (MVP)

**Impact**: In the MVP, all Python code shares a single GIL. This limits parallelism when:
- Multiple partitions execute Python simultaneously
- Pure Python loops dominate (vs NumPy/PyArrow operations)

**Mitigation**:
- Most data operations use Arrow/NumPy which release the GIL
- Subprocess isolation (Phase 3) eliminates this limitation
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

---

## Unresolved Questions

### Open for Discussion

1. **Schema evolution**: How should we handle schema changes between datasource versions? PySpark fails fast on schema mismatch. Current approach: same behavior.

2. **Metrics API**: What metrics should datasources expose?
   - GIL hold/wait time (starvation indicators)
   - Rust↔Python boundary overhead
   - Counters: rows_read, bytes_read, errors

3. **Resource limits**: Should we enforce memory/CPU limits per datasource? How would this interact with subprocess isolation?

4. **Async Python support**: Should readers support `async def read()`? This could improve I/O-bound datasources but complicates the execution model. PySpark 4.0 does not support async readers.

5. **Process reuse strategy**: When subprocess isolation is implemented (Phase 3), how should worker processes be managed? This also affects UDF execution scope.
   - Fresh process per query (safe, slow startup)
   - Pooled processes (fast, state leakage risk)
   - Hybrid (fresh for untrusted, pooled for trusted)

6. **Free-threading mode (PEP 703)**: Python 3.13 introduces experimental free-threading. The implementation can detect free-threading at runtime and adapt execution strategy. Feature flags will enable controlled rollout and rollback.

### Resolved During Implementation

1. ~~**Entry point group name**~~: Using `sail.datasources`
2. ~~**Default batch size**~~: 8192 rows (configurable)
3. ~~**Thread cleanup strategy**~~: RAII with oneshot shutdown signal

---

## Architecture

### System Overview

The Python DataSource architecture integrates with DataFusion at three levels:

1. **TableFormat**: Discovery and instantiation
2. **TableProvider**: Logical planning with schema and filter pushdown
3. **ExecutionPlan**: Physical execution with partition-level parallelism

### Component Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      Sail Worker (Rust)                      │
│              (Runs on each executor node)                    │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Layer 1: Discovery & Registration                      │  │
│  │  • DATASOURCE_REGISTRY                                 │  │
│  │  • Entry points: sail.datasources                      │  │
│  │  • @register decorator                                 │  │
│  └──────────────────────┬─────────────────────────────────┘  │
│                         ▼                                    │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Layer 2: TableFormat → PythonTableFormat               │  │
│  └──────────────────────┬─────────────────────────────────┘  │
│                         ▼                                    │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Layer 3: TableProvider → PythonTableProvider           │  │
│  │  • Schema, filter pushdown, planning                   │  │
│  └──────────────────────┬─────────────────────────────────┘  │
│                         ▼                                    │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Layer 4: ExecutionPlan → PythonDataSourceExec          │  │
│  │  • Partition-level parallelism                         │  │
│  └──────────────────────┬─────────────────────────────────┘  │
│                         ▼                                    │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Layer 5: Stream → PythonDataSourceStream               │  │
│  │  • Python thread lifecycle (RAII)                      │  │
│  └──────────────────────┬─────────────────────────────────┘  │
│                         ▼                                    │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Layer 6: PyO3 Bridge                                   │  │
│  │  • Zero-copy via Arrow C Data Interface                │  │
│  └──────────────────────┬─────────────────────────────────┘  │
└────────────────────────┼─────────────────────────────────────┘
                         ▼
         ┌────────────────────────────────────────┐
         │   Layer 7: Python Runtime              │
         │   • DataSource (user-defined)          │
         │   • DataSourceReader (user-defined)    │
         │   • InputPartition(s)                  │
         └────────────────────────────────────────┘
```

**Execution Context:**
- **Driver**: Coordinates query planning, receives results
- **Workers**: Execute Python DataSource code (schema, partitions, read)
- **Python Runtime**: Embedded in each worker via PyO3

### Query Execution Flow

```
User: spark.read.format("range").option("end", "1000").load()

┌──────────────────────────────────────────────────────────────┐
│ 1. DISCOVERY (Session Startup, Once)                         │
│  → DATASOURCE_REGISTRY.discover_datasources()                │
│  → importlib.metadata.entry_points("sail.datasources")       │
│  → Collect @register decorated classes                       │
│  → Registry: {"range": RangeDataSource, ...}                 │
└──────────────────────────────────────────────────────────────┘
                         ▼
┌──────────────────────────────────────────────────────────────┐
│ 2. LOGICAL PLANNING                                          │
│  → PythonTableFormat.create_provider(options={"end": "1000"})│
│  → Unpickle RangeDataSource class                           │
│  → ds = RangeDataSource(options) [GIL acquired]             │
│  → schema = ds.schema() [GIL acquired]                      │
│  → Return PythonTableProvider(schema, datasource)           │
└──────────────────────────────────────────────────────────────┘
                         ▼
┌──────────────────────────────────────────────────────────────┐
│ 3. PHYSICAL PLANNING                                         │
│  → PythonTableProvider.scan(filters, projection, limit)      │
│  → Convert DataFusion Expr to PythonFilter objects          │
│  → reader = ds.reader(schema) [GIL acquired]                │
│  → partitions = reader.partitions() [GIL acquired]          │
│  → Create PythonDataSourceExec(datasource, schema, parts)   │
└──────────────────────────────────────────────────────────────┘
                         ▼
┌──────────────────────────────────────────────────────────────┐
│ 4. EXECUTION (Parallel, Per-Partition)                       │
│  For each partition:                                         │
│    → PythonDataSourceStream::new()                          │
│    → Spawn Python thread (GIL-bound)                        │
│    → reader.read(partition) yields RecordBatch              │
│    → Zero-copy transfer via Arrow C Data Interface          │
│    → mpsc::send(RecordBatch) to Rust stream                │
│    → DataFusion consumes stream                             │
└──────────────────────────────────────────────────────────────┘
                         ▼
┌──────────────────────────────────────────────────────────────┐
│ 5. CLEANUP (RAII, Automatic)                                 │
│  → Stream dropped → Send stop signal                         │
│  → Join Python thread                                        │
│  → Release Python resources                                  │
└──────────────────────────────────────────────────────────────┘
```

### Session-Scoped DataSource Registration

In addition to global registry via entry points, Sail supports **session-scoped registration** for DataSources registered dynamically via `spark.dataSource.register()`.

**Architecture:**

```rust
pub struct PythonTableFormat {
    name: String,
    pickled_class: Option<Vec<u8>>,  // None = lookup from global registry
}

impl PythonTableFormat {
    // Entry-point discovered (global)
    pub fn new(name: String) -> Self { ... }
    
    // Session-registered (isolated)
    pub fn with_pickled_class(name: String, bytes: Vec<u8>) -> Self { ... }
}
```

**Lookup precedence:**

1. If `pickled_class` is `Some`, use embedded bytes (session-scoped)
2. Otherwise, lookup from `DATASOURCE_REGISTRY` (global)

This enables multiple sessions to register different implementations of the same datasource name without interference. Session-scoped datasources are stored in the session's `TableFormatRegistry` and are cleaned up when the session ends.

**Performance Characteristics:**
- **Control plane**: Schema and partition discovery incur Python interop overhead, amortized across query execution
- **Data plane**: Zero-copy via Arrow C Data Interface (see [specification](https://arrow.apache.org/docs/format/CDataInterface.html))
- **GIL contention**: Only during control plane; data transfer releases GIL
- **Parallelism**: Limited by global GIL in MVP; Phase 3 subprocess isolation addresses this

> **Note**: For GIL-sensitive workloads, use libraries that minimize GIL usage (connector-x, DuckDB) or enable subprocess-based Python workers (Phase 3).

### Filter Pushdown Pipeline

```
SQL: WHERE x > 10 AND y = 'foo'
         ▼
DataFusion Expr::BinaryExpr {Col, Gt, Lit}
         ▼
exprs_to_python_filters()
         ▼
Vec<PythonFilter> [GreaterThan(x, 10), EqualTo(y, "foo")]
         ▼
to_python_objects() [PyO3]
         ▼
reader.pushFilters([...])
         ▼
Returns unsupported filters
         ▼
Classification: Exact/Inexact/Unsupported
```

**Filter Classification:**
- **Exact**: DataSource fully handles the filter; DataFusion skips re-evaluation
- **Inexact**: DataSource partially handles (e.g., bloom filter); DataFusion re-evaluates
- **Unsupported**: DataSource cannot handle; DataFusion applies post-read filtering

**MVP Status**: Filter infrastructure implemented but NOT ACTIVE. All filters marked `Unsupported` (DataFusion applies post-read). Activation planned for Phase 2+.

---

## API Reference

### Core Classes

```python
from abc import ABC, abstractmethod
from typing import Dict, List, Iterator, Union, Any
import pyarrow as pa

class DataSource(ABC):
    """Base class for custom data sources."""

    def __init__(self, options: Dict[str, str]):
        self.options = options

    @classmethod
    @abstractmethod
    def name(cls) -> str:
        """Unique identifier for this data source."""
        pass

    @abstractmethod
    def schema(self) -> Union[str, pa.Schema]:
        """Return DDL string or PyArrow Schema."""
        pass

    @abstractmethod
    def reader(self, schema) -> "DataSourceReader":
        """Create a reader for this data source."""
        pass

class DataSourceReader(ABC):
    """Handles reading data from partitions."""

    def pushFilters(self, filters: List["Filter"]) -> Iterator["Filter"]:
        """
        Accept filters, return unsupported ones.
        Default: all filters unsupported.
        """
        return iter(filters)

    def partitions(self) -> List["InputPartition"]:
        """Return list of partitions for parallel reading."""
        return [InputPartition(0)]

    @abstractmethod
    def read(self, partition: "InputPartition") -> Iterator[Union[tuple, pa.RecordBatch]]:
        """
        Yield rows as tuples or RecordBatches.
        Tuples: slower, batched automatically
        RecordBatch: zero-copy, high performance
        """
        pass

class InputPartition:
    """Represents a partition of data."""
    def __init__(self, value: Any = 0):
        self.value = value
```

### Filter Classes

```python
from dataclasses import dataclass
from typing import Any, Tuple

# Type alias for column paths (nested columns)
ColumnPath = Tuple[str, ...]

@dataclass(frozen=True)
class EqualTo:
    attribute: ColumnPath
    value: Any

@dataclass(frozen=True)
class EqualNullSafe:
    """Null-safe equals: attribute <=> value"""
    attribute: ColumnPath
    value: Any

@dataclass(frozen=True)
class GreaterThan:
    attribute: ColumnPath
    value: Any

@dataclass(frozen=True)
class GreaterThanOrEqual:
    attribute: ColumnPath
    value: Any

@dataclass(frozen=True)
class LessThan:
    attribute: ColumnPath
    value: Any

@dataclass(frozen=True)
class LessThanOrEqual:
    attribute: ColumnPath
    value: Any

@dataclass(frozen=True)
class In:
    """Filter: attribute IN (values)"""
    attribute: ColumnPath
    values: Tuple[Any, ...]

@dataclass(frozen=True)
class IsNull:
    attribute: ColumnPath

@dataclass(frozen=True)
class IsNotNull:
    attribute: ColumnPath

@dataclass(frozen=True)
class Not:
    child: "Filter"

@dataclass(frozen=True)
class And:
    left: "Filter"
    right: "Filter"

@dataclass(frozen=True)
class Or:
    left: "Filter"
    right: "Filter"

@dataclass(frozen=True)
class StringStartsWith:
    """Filter: attribute LIKE 'value%'"""
    attribute: ColumnPath
    value: str

@dataclass(frozen=True)
class StringEndsWith:
    """Filter: attribute LIKE '%value'"""
    attribute: ColumnPath
    value: str

@dataclass(frozen=True)
class StringContains:
    """Filter: attribute LIKE '%value%'"""
    attribute: ColumnPath
    value: str
```

### Registration

```python
from pysail.spark.datasource import register

@register
class MyDataSource(DataSource):
    """Decorator automatically registers the DataSource."""
    pass
```

**Helper Functions:**

```python
from pysail.spark.datasource import (
    get_registered_datasource,
    list_registered_datasources,
    discover_entry_points,
)

# Lookup a registered datasource by name
ds_class = get_registered_datasource("mydatasource")

# List all registered datasource names
names = list_registered_datasources()  # ["mydatasource", "range", ...]

# Discover datasources from Python entry points
datasources = discover_entry_points("sail.datasources")  # [(name, class), ...]
```

---

## Implementation Details

### Component Breakdown

#### 1. Discovery System ([discovery.rs](../../../crates/sail-data-source/src/python_datasource/discovery.rs))

Discovers DataSources via three mechanisms:

1. **Entry Points**: `sail.datasources` group via `importlib.metadata`
2. **Python Registry**: `@register` decorator populates `_REGISTERED_DATASOURCES`
3. **Runtime Registration**: `spark.dataSource.register()` call (PySpark compatible)

```rust
pub static DATASOURCE_REGISTRY: Lazy<PythonDataSourceRegistry> =
    Lazy::new(PythonDataSourceRegistry::new);

pub struct DataSourceEntry {
    pub name: String,
    pub pickled_class: Vec<u8>,  // GIL-free storage
    pub module_path: String,
}
```

#### 2. PythonExecutor Trait ([executor.rs](../../../crates/sail-data-source/src/python_datasource/executor.rs))

Abstracts execution model for future subprocess isolation:

```rust
#[async_trait]
pub trait PythonExecutor: Send + Sync {
    async fn get_schema(&self, command: &[u8]) -> Result<SchemaRef>;
    async fn get_partitions(&self, command: &[u8]) -> Result<Vec<InputPartition>>;
    async fn execute_read(...) -> Result<BoxStream<'static, Result<RecordBatch>>>;
}

// MVP: In-process via PyO3
pub struct InProcessExecutor { python_ver: String }

// Future (PR #3): Subprocess isolation
// pub struct RemoteExecutor { grpc_client: PythonWorkerClient }
```

#### 3. Arrow Utilities ([arrow_utils.rs](../../../crates/sail-data-source/src/python_datasource/arrow_utils.rs))

Zero-copy conversions via Arrow C Data Interface:

```rust
// Python RecordBatch → Rust RecordBatch (zero-copy)
pub fn py_record_batch_to_rust(py: Python<'_>, py_batch: &Bound<'_, PyAny>)
    -> Result<RecordBatch>;

// Rust Schema → Python Schema
pub fn rust_schema_to_py(py: Python<'_>, schema: &SchemaRef)
    -> Result<Py<PyAny>>;

// Row-based batch conversion (for tuple yields)
pub fn convert_rows_to_batch(schema: &SchemaRef, pickled_rows: &[Vec<u8>])
    -> Result<RecordBatch>;
```

**MVP Data Types**: Int32, Int64, Float32, Float64, Utf8, Boolean, Date32, Timestamp, Null

#### 5. DDL Schema Parsing ([python_datasource.rs](../../../crates/sail-data-source/src/python_datasource/python_datasource.rs))

When `DataSource.schema()` returns a DDL string, Sail parses it using DataFusion's SQL parser:

| DDL Type | Arrow Type |
|----------|------------|
| `INT`, `INTEGER` | Int32 |
| `BIGINT`, `LONG` | Int64 |
| `FLOAT`, `REAL` | Float32 |
| `DOUBLE` | Float64 |
| `BOOLEAN`, `BOOL` | Boolean |
| `STRING`, `VARCHAR`, `TEXT`, `CHAR` | Utf8 |
| `DATE` | Date32 |
| `TIMESTAMP` | Timestamp(Nanosecond, None) |
| `DECIMAL(p,s)` | Decimal128(p, s) |

**Example:**
```python
def schema(self):
    return "id INT, name STRING, created_at TIMESTAMP, amount DECIMAL(10,2)"
```

For complex types (List, Struct, Map), return a PyArrow Schema instead:
```python
def schema(self):
    return pa.schema([
        ("id", pa.int64()),
        ("tags", pa.list_(pa.string())),
    ])
```

#### 4. Stream with RAII ([stream.rs](../../../crates/sail-data-source/src/python_datasource/stream.rs))

Ensures proper Python thread cleanup:

```rust
enum StreamState {
    Running {
        stop_signal: Option<oneshot::Sender<()>>,
        python_thread: Option<std::thread::JoinHandle<()>>,
        rx: mpsc::Receiver<Result<RecordBatch>>,
    },
    Stopped,
}

impl Drop for PythonDataSourceStream {
    fn drop(&mut self) {
        // 1. Send stop signal
        // 2. Join thread to ensure cleanup
        // Prevents resource leaks
    }
}
```

**Rust** (`crates/sail-data-source/src/python_datasource/`):
- `mod.rs` - Module exports
- `discovery.rs` - Registry & discovery
- `filter.rs` - Filter pushdown
- `arrow_utils.rs` - Arrow conversions
- `executor.rs` - PythonExecutor trait
- `python_datasource.rs` - Core PyO3 bridge
- `python_table_provider.rs` - TableProvider impl
- `table_format.rs` - TableFormat impl
- `exec.rs` - ExecutionPlan
- `stream.rs` - RecordBatch stream
- `error.rs` - Error types

**Python** (`python/pysail/spark/datasource/`):
- `__init__.py` - Package exports
- `base.py` - Core API classes (DataSource, DataSourceReader, Filters)
- `compat.py` - PySpark compatibility shim
- `examples.py` - Example datasources (Range, Constant, FlappyBird)
- `test_verify.py` - Verification tests

---

## PySpark Compatibility

### Zero-Code-Change Migration

Sail provides **100% API compatibility** with the PySpark 4.0+ DataSource API. Existing PySpark DataSources work without modification:

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

### Compatibility Strategy: sys.modules Shim

When PySpark DataSources are unpickled, the base class (`pyspark.sql.datasource.DataSource`) must be available. Since Sail includes `pysail` but not `pyspark`, we use a **sys.modules shim** implemented in `pysail/spark/datasource/compat.py`:

**Server-side unpickling** (automatic):
```python
def unpickle_datasource_class(pickled_bytes: bytes):
    """Unpickle a PySpark DataSource class with shim support."""
    install_pyspark_shim(warn_if_pyspark_present=False)
    return cloudpickle.loads(pickled_bytes)
```

**Client-side** (explicit, for testing without PySpark):
```python
from pysail.spark.datasource import install_pyspark_shim
install_pyspark_shim()
```

**Module mappings installed:**
- `pyspark.sql.datasource` → `pysail.spark.datasource`
- `pyspark.sql.datasource_internal` → `pysail.spark.datasource`
- `pyspark.cloudpickle` → `cloudpickle`

> **Warning**: Do not use the shim in environments where real PySpark is also needed—it modifies `sys.modules` globally and will shadow PySpark modules.

**How it works:**
1. PySpark client pickles DataSource → references `pyspark.sql.datasource.DataSource`
2. Sail unpickles → `unpickle_datasource_class()` installs shim first
3. Python looks up module in `sys.modules` → gets pysail implementation
4. Unpickling succeeds with Sail implementation

### Registration Methods

**Method 1: @register Decorator (Current)**

The `@register` decorator requires the module containing the decorated class to be imported. Typically this happens via `import` in your application or via entry points:

```python
# my_datasources.py
from pyspark.sql.datasource import DataSource, DataSourceReader
from pysail.spark.datasource import register

@register
class MyDataSource(DataSource):
    # PySpark base class, Sail registration
    pass
```

```python
# main.py - must import the module to trigger registration
import my_datasources

df = spark.read.format("mydatasource").load()
```

**Method 2: spark.dataSource.register() (Future)**

```python
from pyspark.sql.datasource import DataSource

class MyDataSource(DataSource):
    pass

# Will work after RegisterDataSource command implemented
spark.dataSource.register(MyDataSource)
```

**Implementation location**: [crates/sail-spark-connect/src/server.rs:134-136](../../../crates/sail-spark-connect/src/server.rs#L134-L136)

**Method 3: Entry Points (Package Distribution)**

```toml
[project.entry-points."sail.datasources"]
mydatasource = "my_package:MyDataSource"  # Works with PySpark or Sail base class
```

### Supported Versions

- **PySpark**: 4.0+ (Python DataSource API introduced in Spark 4.0)
- **Python**: 3.9, 3.10, 3.11, 3.12

---

## Performance Considerations

> For detailed examples and library recommendations, see [python-datasource-reference.md](./python-datasource-reference.md#performance-guide).

### Summary

| Aspect | MVP (Phase 1) | Future (Phase 3) |
|--------|---------------|------------------|
| **GIL** | Single, shared | N workers = N GILs |
| **Data Transfer** | Zero-copy (Arrow C Data Interface) | Zero-copy (shared memory) |

### Yield Type Recommendation

Use `pa.RecordBatch` for production (zero-copy); tuples for prototyping.

### GIL-Releasing Libraries

Use libraries with native backends: **connector-x** (Rust), **DuckDB** (C++), **NumPy/Polars**.

See [python-datasource-reference.md](./python-datasource-reference.md#complete-examples) for complete connector-x and DuckDB examples.

## Future Work

### Multimodal DataSource Support (Exploration)

Modern AI pipelines treat **images, audio, and embeddings as first-class data types**. Sail's Python DataSource architecture could potentially support multimodal workloads, drawing inspiration from frameworks like [Daft](https://www.getdaft.io/).

> [!NOTE]
> This section explores **potential future directions**. Scope and prioritization are subject to community feedback and demand.

#### Phase 2: Binary Type Foundation

The `Binary` type (planned for PR #2) provides the foundation for multimodal data. Images, audio, and other blob data could be handled as raw bytes:

```python
class ImageDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "images"

    def schema(self):
        return pa.schema([
            ("path", pa.string()),
            ("image_bytes", pa.binary()),  # Raw image data
            ("metadata", pa.string()),
        ])

    def reader(self, schema):
        return ImageReader(self.options.get("base_path", "."))

class ImageReader(DataSourceReader):
    def __init__(self, base_path):
        self.base_path = base_path

    def partitions(self):
        import glob
        files = glob.glob(f"{self.base_path}/**/*.jpg", recursive=True)
        # Partition by file chunks for parallelism
        chunk_size = max(1, len(files) // 4)
        return [InputPartition(files[i:i+chunk_size]) for i in range(0, len(files), chunk_size)]

    def read(self, partition):
        for path in partition.value:
            with open(path, "rb") as f:
                yield (path, f.read(), "{}")
```

#### Potential Phase 7: Extension Types & Native Operations

If demand warrants, Sail could explore:

| Feature | Description | Status |
|---------|-------------|--------|
| **Arrow Extension Types** | `sail.image`, `sail.audio` with format metadata | Exploring |
| **`decode_image()`** | SQL function to decode Binary → Image struct | Exploring |
| **`embed()`** | SQL function for embedding generation | Exploring |
| **`prompt()`** | LLM inference on multimodal columns | Exploring |

**Extension Type Example:**

```python
# Future: DataSource returns typed images
def schema(self):
    image_type = pa.ExtensionType(pa.binary(), "sail.image", 
                                   metadata={"format": "jpeg"})
    return pa.schema([
        ("path", pa.string()),
        ("image", image_type),
    ])
```

**Potential Native SQL Operations:**

```sql
-- Decode binary to image, generate embedding, run LLM inference
SELECT 
    path,
    decode_image(image_bytes) AS image,
    embed(image_bytes, 'clip-vit-base') AS embedding,
    prompt('Describe this image', image_bytes, model='gpt-4o-mini') AS description
FROM images.load(path='s3://bucket/images/')
```

#### Potential Multimodal Use Cases

| Use Case | DataSource Pattern |
|----------|-------------------|
| **Image classification** | Yield image bytes, apply model via UDF |
| **Embedding generation** | Yield content, use `embed()` or Python UDF with CLIP/Sentence-Transformers |
| **RAG pipelines** | Chunk documents, embed, store in vector DB |
| **Audio transcription** | Yield audio bytes, apply Whisper via UDF |

### Roadmap

| Phase | Focus | Scope | Status |
|-------|-------|-------|--------|
| **Phase 1** | MVP Batch Read | Discovery, execution, filter infra | ✅ Complete |
| **Phase 2** | Batch Write + Binary | Write exec, commit protocol, Binary type | Planned |
| **Phase 3** | Subprocess Isolation | gRPC, shared memory, crash isolation | Planned |
| **Phase 4** | Streaming Read | Offset management | Planned |
| **Phase 5** | Streaming Write | BatchId protocol | Planned |
| **Phase 6** | Performance & Polish | Metrics, caching | Planned |
| **Phase 7** | Multimodal & AI | Extension types, native ops (if demand warrants) | Exploring |

### Phase 3: Subprocess Isolation (Detailed)

**Multi-Faceted Benefits**:

| Benefit | Description | Valuable with Free Threading? |
|---------|-------------|------------------------------|
| **GIL Parallelism** | N workers = N GILs | ❌ No (free threading eliminates GIL) |
| **Crash Isolation** | Worker crash doesn't kill Sail | ✅ Yes - production-critical |
| **Memory Isolation** | Per-worker memory limits | ✅ Yes - prevents OOM |
| **Version Isolation** | Different Python versions/deps | ✅ Yes - legacy support |
| **Security Sandboxing** | Untrusted code isolated | ✅ Yes - limits attack surface |

**Architecture**: gRPC control plane + shared memory data plane

```
┌──────────────────────────────────────────────┐
│        CONTROL PLANE (gRPC/Protobuf)         │
│  Sail (Rust) ←→ Python Worker (subprocess)   │
│  "Read partition 5" → "Data at offset 500"   │
└──────────────────────────────────────────────┘
                    ▼
┌──────────────────────────────────────────────┐
│        DATA PLANE (Shared Memory)            │
│  Arrow Reader ←─ mmap ─→ Arrow Writer        │
│  /dev/shm/sail_py_* (zero-copy buffers)      │
└──────────────────────────────────────────────┘
```

**Configuration**:
```bash
SAIL_PYTHON_ISOLATION_MODE=subprocess  # or "in_process" (default)
SAIL_PYTHON_WORKER_COUNT=auto          # defaults to CPU count
# Shared memory uses /dev/shm by default; file-based alternative available
```

### Free Threading (PEP 703) Considerations

**Status**: Experimental in Python 3.13+, production-ready 2026+

**Adaptive Executor Selection**:
```rust
pub fn create_executor(config: &Config) -> Result<Arc<dyn PythonExecutor>> {
    if config.isolation_mode == IsolationMode::Subprocess {
        // Always use subprocess (crash/memory isolation)
        Arc::new(RemoteExecutor::new(config)?)
    } else if is_free_threading_enabled() {
        // InProcess with full parallelism
        Arc::new(InProcessExecutor::new(config.python_version.clone()))
    } else {
        // InProcess with GIL limitation
        Arc::new(InProcessExecutor::new(config.python_version.clone()))
    }
}
```

**Recommendation**: Subprocess isolation remains default for production even after free threading stabilizes (crash isolation, resource limits).

---

## FAQ

> See [python-datasource-reference.md](./python-datasource-reference.md#faq) for complete FAQ.

**Key Questions:**

| Question | Short Answer |
|----------|--------------|
| Why in-process PyO3? | MVP simplicity + zero-copy. Phase 3 adds subprocess isolation. |
| GIL impact? | Control plane only; data plane uses GIL-releasing libraries. |
| Tuple vs RecordBatch? | RecordBatch for production (zero-copy). |
| PySpark compatibility? | 100% API compatible with PySpark 4.0+ DataSource API. |
| Python versions? | 3.9, 3.10, 3.11, 3.12 |

---

## Appendix

### References

- [PySpark DataSource API](https://github.com/apache/spark/blob/master/python/pyspark/sql/datasource.py)
- [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
- [PyO3 Guide](https://pyo3.rs/)
- [PEP 703 – Free Threading](https://peps.python.org/pep-0703/)
- [Connector-X](https://github.com/sfu-db/connector-x)
- [DuckDB Python API](https://duckdb.org/docs/api/python/overview)
- [Daft – Multimodal Dataframe](https://www.getdaft.io/) (inspiration for multimodal support)

---

**End of RFC**

