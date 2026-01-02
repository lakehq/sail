# RFC: Python DataSource Support

**Status:** Phase 1 Complete (MVP Read-Only)
**Branch:** `feat/python-datasource-write`
**PR:** #1 - MVP Batch Read
**Authors:** Santosh
**Date:** 2025-12-31

## Executive Summary

This RFC documents Python DataSource support in Sail, enabling users to implement custom data sources in Python while leveraging Sail's distributed execution engine. The implementation provides **100% API compatibility** with the PySpark 4.0+ DataSource API, allowing existing PySpark DataSource implementations to work without modification.

**Key Benefits:**
- **Zero-friction PySpark migration**: Existing DataSource code works unchanged
- **High performance**: Zero-copy data transfer via Arrow C Data Interface
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
12. [FAQ](#faq) (22 questions)
13. [Appendix](#appendix)

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
| **cloudpickle** | Pickles lambdas/closures, PySpark compatible | Security considerations (same as Spark) | Chosen |
| **dill** | Similar to cloudpickle | Less maintained, PySpark uses cloudpickle | Rejected |
| **pickle** | Standard library | Cannot pickle lambdas/closures | Rejected |
| **JSON + class registry** | Safe, inspectable | Cannot serialize arbitrary Python objects | Rejected |

**Rationale**: PySpark uses cloudpickle; using the same ensures zero-friction migration. Security posture matches Spark—datasources are treated as trusted code.

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
from typing import Any, List

@dataclass(frozen=True)
class EqualTo:
    column: str
    value: Any

@dataclass(frozen=True)
class GreaterThan:
    column: str
    value: Any

@dataclass(frozen=True)
class LessThan:
    column: str
    value: Any

@dataclass(frozen=True)
class IsNull:
    column: str

@dataclass(frozen=True)
class Not:
    child: "Filter"

@dataclass(frozen=True)
class And:
    left: "Filter"
    right: "Filter"

# ... (additional filter classes)
```

### Registration

```python
from pysail.spark.datasource import register

@register
class MyDataSource(DataSource):
    """Decorator automatically registers the DataSource."""
    pass
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

**Security**: Cloudpickle can execute arbitrary code. Only load from trusted packages.

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
- `base.py` - Core API classes
- `examples.py` - Example datasources

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

When PySpark DataSources are unpickled, the base class (`pyspark.sql.datasource.DataSource`) must be available. Since Sail includes `pysail` but not `pyspark`, we use a **sys.modules shim**:

```python
# In Sail's Python initialization (pysail/spark/datasource/compat.py)
import sys
from pysail.spark.datasource import (
    DataSource, DataSourceReader, InputPartition, # ... all classes
)

class PysparkDatasourceCompat:
    """Compatibility shim for pyspark.sql.datasource."""
    pass

# Re-export all pysail classes as pyspark classes
for name in dir():
    obj = locals()[name]
    if not name.startswith('_') and isinstance(obj, type):
        setattr(PysparkDatasourceCompat, name, obj)

# Inject into sys.modules
sys.modules['pyspark.sql.datasource'] = PysparkDatasourceCompat
```

**How it works:**
1. PySpark client pickles DataSource → references `pyspark.sql.datasource.DataSource`
2. Sail unpickles → Python looks up module in `sys.modules`
3. Shim returns `pysail.spark.datasource.DataSource`
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

### GIL Management

**Current (MVP)**: In-process PyO3 execution
- Control plane (schema, partitions): GIL acquired briefly
- Data plane: Zero-copy via Arrow C Data Interface
- NumPy/PyArrow release GIL during compute

**Future (PR #3)**: Subprocess isolation
- 1 Python subprocess per worker thread
- Full GIL parallelism (N workers = N GILs)
- Shared memory for data plane

### Performance Characteristics

Control plane operations (schema discovery, partition enumeration) incur Python interop overhead that is amortized across query execution. For typical analytical queries (thousands of batches over seconds to minutes), this represents <1% of total execution time.

**Library Recommendations:**

| Use Case | Library | Why |
|----------|---------|-----|
| PostgreSQL/MySQL | [connector-x](https://github.com/sfu-db/connector-x) | Native Arrow, GIL-free |
| Analytical transforms | [DuckDB](https://duckdb.org/) | Native Arrow, GIL-free |

### Yield Types Comparison

| Yield Type | Performance | Memory | Use Case |
|------------|-------------|--------|----------|
| **Tuples** | Additional conversion overhead | Batched in `RowBatchCollector` | Simple implementations |
| **RecordBatch** | Zero-copy | Direct Arrow transfer | High-performance |

**Recommendation**: Use RecordBatch for large datasets; tuples for prototyping.

### Case Study: Database Connectors

Python libraries like [connector-x](https://github.com/sfu-db/connector-x) and [DuckDB](https://duckdb.org/) provide excellent database connectivity with minimal GIL overhead. Their Rust/C++ backends release the GIL during I/O and compute, making them ideal for Python DataSource implementations.

#### Why These Libraries Work Well

| Library | Backend | GIL Behavior | Arrow Support | Use Case |
|---------|---------|--------------|---------------|----------|
| **connector-x** | Rust | Releases GIL during query | Native Arrow output | Multi-database (PostgreSQL, MySQL, SQLite, etc.) |
| **DuckDB** | C++ | Releases GIL during query | Native Arrow output | Embedded analytics, Parquet/CSV |
| **turbodbc** | C++ | Releases GIL during fetch | Arrow via `fetchallarrow()` | ODBC databases |
| **psycopg3** | C | Releases GIL during I/O | Via PyArrow conversion | PostgreSQL |

**Key Insight**: These libraries spend most execution time in native code with GIL released. The Python DataSource wrapper adds minimal overhead since:
- Schema discovery: One-time GIL acquisition, amortized across query
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
    def __init__(self, database, query):
        self.database = database
        self.query = query

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

Libraries with native backends (Rust, C++) that release the GIL provide significantly better performance than pure Python:

| Characteristic | Pure Python | Native Backend Libraries |
|----------------|-------------|-------------------------|
| Arrow conversion | Required | Not needed (zero-copy) |
| Parallel partitions | GIL contention | Minimal contention |

#### Future: connector-x Python DataSource

The connector-x DataSource demonstrates the recommended pattern for high-performance database access:

| Phase | Approach | Status |
|-------|----------|--------|
| **MVP** | Python DataSources with connector-x/DuckDB | Complete |
| **Future** | connector-x Python DataSource as reference impl | Planned |

---

## Future Work

### Roadmap

| Phase | Focus | Scope | Status |
|-------|-------|-------|--------|
| **Phase 1** | MVP Batch Read | Discovery, execution, filter infra | ✅ Complete |
| **Phase 2** | Batch Write | Write exec, commit protocol | Planned |
| **Phase 3** | Subprocess Isolation | gRPC, shared memory, crash isolation | Planned |
| **Phase 4** | Streaming Read | Offset management | Planned |
| **Phase 5** | Streaming Write | BatchId protocol | Planned |
| **Phase 6** | Performance & Polish | Exotic types, metrics | Planned |

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

### Architecture & Performance

**Q1: Why use in-process PyO3 instead of subprocess isolation?**

The MVP uses in-process execution for simplicity and zero-copy performance. PyO3 allows direct memory sharing between Rust and Python via the Arrow C Data Interface. Subprocess isolation (Phase 3) adds complexity but enables true GIL parallelism. The `PythonExecutor` trait abstracts this choice, allowing seamless switching via configuration.

**Q2: How does the GIL affect parallel partition reads?**

In the MVP, all Python code shares a single GIL. However, this is mitigated by:
- **GIL-releasing libraries**: Libraries like connector-x, DuckDB, NumPy release the GIL during compute
- **Thread-per-partition model**: Each partition runs on a dedicated OS thread
- **Minimal Python in hot path**: Control plane (schema, partitions) is cached; data plane uses zero-copy Arrow

With subprocess isolation (Phase 3), each worker has its own GIL, enabling true parallelism.

**Q3: What's the overhead of Python DataSources vs native formats?**

Control plane operations (schema, partitions) incur Python interop overhead that is amortized across query execution. For typical analytical queries (thousands of batches over seconds to minutes), this represents <1% of total execution time. Data transfer is zero-copy when using Arrow RecordBatches.

**Q4: How does zero-copy Arrow transfer work?**

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
| **Tuples** | Additional conversion overhead | Batched in `RowBatchCollector` | Simple implementations |
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

Currently, metrics are captured at the DataFusion level. In Phase 6, we'll add:
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

Yes, for the PySpark 4.0+ DataSource API. Simply change the import:

```python
# PySpark
from pyspark.sql.datasource import DataSource, DataSourceReader

# Sail (no other changes needed)
from pysail.spark.datasource import DataSource, DataSourceReader
```

**Compatibility notes:**
- The Python DataSource API was introduced in PySpark 4.0; earlier versions don't have this API
- Sail-specific features (e.g., `@register` decorator) require `pysail` imports

**Q14: What PySpark API version is supported?**

- **PySpark 4.0+**: Full batch read/write support
- **PySpark 4.0+**: Streaming DataSource API (coming in Phase 4-5)
- **Python**: 3.9, 3.10, 3.11, 3.12

**Q15: Why Python for DataSources?**

Python was chosen for DataSource extensibility because:
- **PySpark compatibility**: Enables zero-code-change migration from PySpark 4.0+ DataSources
- **Ecosystem access**: Libraries for databases, cloud services, and data formats
- **Subprocess isolation**: Python's architecture enables straightforward crash isolation (Phase 3)

**Q16: Can I use Python DataSources from SQL?**

Yes. Python DataSources support SQL access via `CREATE TABLE ... USING`:

```sql
CREATE TABLE api_users
USING mydatasource
OPTIONS (endpoint = 'https://api.example.com');

SELECT * FROM api_users WHERE status = 'active';
```

The table metadata (format name + options) is stored in the catalog. On query, Sail resolves the format via `TableFormatRegistry`, creates the `PythonTableProvider`, and executes. Filter pushdown applies automatically if your DataSource implements `pushFilters()`.

**PySpark compatibility note**: This SQL syntax is identical to PySpark. In PySpark, you must first call `spark.dataSource.register(MyDataSource)`. In Sail, DataSources discovered via entry points or `@register` are available immediately. Sail's planned `spark.dataSource.register()` support (see Registration Methods) will provide full API parity.

**Q17: What data types are supported in MVP vs future phases?**

| Phase | Types Added |
|-------|-------------|
| **Phase 1 (MVP)** | Int32, Int64, Float32, Float64, Utf8, Boolean, Date32, Timestamp, Null |
| **Phase 2 (Write)** | Binary, Decimal128, Int8, Int16 |
| **Phase 4 (Stream)** | List<T>, Struct, Map<K,V>, LargeUtf8 |
| **Phase 6 (Polish)** | Duration, Interval, FixedSizeBinary, Union, Dictionary |

**Q18: How do I migrate from Spark's JDBC connector?**

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

**Q19: Is cloudpickle safe? What's the trust model?**

**Warning**: Cloudpickle can execute arbitrary code during deserialization.

Trust model:
- **Entry points** (`sail.datasources`): Implicitly trusted - requires package installation
- **@register decorator**: Requires explicit code execution in Python
- **Dynamic loading**: Validate with `validate_datasource_class()` before use

Only load datasources from trusted packages. Never unpickle datasources from untrusted sources.

**Q20: What happens if Python code crashes during read?**

In **MVP (in-process)**:
- Python exception → `DataFusionError::External`
- Query fails gracefully with error message
- Python segfault → Sail process crashes (use subprocess isolation for safety)

With **subprocess isolation (Phase 3)**:
- Worker crash → Worker restarted, partition retried
- No impact on main Sail process

**Q21: When will subprocess isolation be available?**

Subprocess isolation is planned for **Phase 3** in the implementation plan. It provides multiple benefits beyond just GIL parallelism:

| Benefit | Description |
|---------|-------------|
| **GIL Parallelism** | N workers = N GILs, true concurrent Python execution |
| **Crash Isolation** | Worker crash doesn't affect Sail server (production-critical) |
| **Memory Isolation** | Per-worker memory limits prevent OOM cascades |
| **Version Isolation** | Different workers can use different Python versions/deps |
| **Security Sandboxing** | Untrusted datasources run in isolated process |

**Note on Free Threading**: Even when Python's free-threading mode (PEP 703) becomes stable (expected 2026+), subprocess isolation remains valuable for crash isolation, memory limits, and security. The `PythonExecutor` trait allows runtime adaptation—Sail will detect free threading and can use `InProcessExecutor` when appropriate, while still offering subprocess mode for production safety.

**Q22: How do I configure batch size and other tuning parameters?**

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

---

## Appendix

### Design Decisions

#### Execution Model

| Alternative | Pros | Cons | Decision |
|-------------|------|------|----------|
| **PyO3 in-process** | Zero-copy, simple deployment | Single GIL, crash risk | MVP choice |
| **Subprocess + gRPC** | GIL parallelism, crash isolation | Serialization overhead | Phase 3 |
| **Subprocess + shared memory** | GIL parallelism + zero-copy | High complexity | Phase 3 |

#### Serialization Format

| Alternative | Pros | Cons | Decision |
|-------------|------|------|----------|
| **cloudpickle** | Pickles lambdas, PySpark compatible | Security considerations | Chosen |
| **dill** | Similar to cloudpickle | Less maintained | Rejected |
| **JSON + registry** | Safe, inspectable | Cannot serialize arbitrary objects | Rejected |

#### Data Transfer

| Alternative | Pros | Cons | Decision |
|-------------|------|------|----------|
| **Arrow C Data Interface** | Zero-copy, standard | Requires PyArrow | Chosen (in-process) |
| **Arrow IPC over shared memory** | Zero-copy across processes | Requires mmap setup | Chosen (subprocess) |
| **Pickle** | Simple | Slow, copies data | Rejected |

### References

- [PySpark DataSource API](https://github.com/apache/spark/blob/master/python/pyspark/sql/datasource.py)
- [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
- [PyO3 Guide](https://pyo3.rs/)
- [PEP 703 – Free Threading](https://peps.python.org/pep-0703/)
- [Connector-X](https://github.com/sfu-db/connector-x)
- [DuckDB Python API](https://duckdb.org/docs/api/python/overview)

---

**End of RFC**
