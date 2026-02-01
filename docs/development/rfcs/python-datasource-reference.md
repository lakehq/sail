# Python DataSource: Reference Guide

This document provides detailed implementation guidance, examples, and FAQ for Python DataSource support. For the core design rationale, see [python-datasource-rfc.md](./python-datasource-rfc.md).

---

## Table of Contents

1. [Quick Reference](#quick-reference)
2. [Complete Examples](#complete-examples)
3. [DDL Schema Reference](#ddl-schema-reference)
4. [Filter Classes Reference](#filter-classes-reference)
5. [PySpark Compatibility Details](#pyspark-compatibility-details)
6. [Performance Guide](#performance-guide)
7. [FAQ](#faq)

---

## Quick Reference

### Minimal DataSource

```python
from pysail.spark.datasource import DataSource, DataSourceReader, InputPartition, register
import pyarrow as pa

@register
class MyDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "myformat"

    def schema(self):
        return "id INT, name STRING"

    def reader(self, schema):
        return MyReader()

class MyReader(DataSourceReader):
    def read(self, partition):
        yield (1, "Alice")
        yield (2, "Bob")
```

### Usage

```python
df = spark.read.format("myformat").load()
```

---

## Complete Examples

### Database Connector (connector-x)

```python
from pysail.spark.datasource import DataSource, DataSourceReader, InputPartition, register
import connectorx as cx

@register
class ConnectorXDataSource(DataSource):
    """High-performance database reads via connector-x (Rust backend)."""

    @classmethod
    def name(cls) -> str:
        return "connectorx"

    def schema(self):
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
            return [InputPartition(i) for i in range(self.partition_num)]
        return [InputPartition(0)]

    def read(self, partition):
        if self.partition_on:
            table = cx.read_sql(
                self.url, self.query,
                partition_on=self.partition_on,
                partition_num=self.partition_num,
                return_type="arrow"
            )
            chunk_size = len(table) // self.partition_num
            start = partition.partition_id * chunk_size
            end = start + chunk_size if partition.partition_id < self.partition_num - 1 else len(table)
            yield table.slice(start, end - start)
        else:
            yield cx.read_sql(self.url, self.query, return_type="arrow")

# Usage:
# df = spark.read.format("connectorx") \
#     .option("url", "postgresql://user:pass@localhost/db") \
#     .option("query", "SELECT * FROM users") \
#     .load()
```

### DuckDB DataSource

```python
@register
class DuckDBDataSource(DataSource):
    """Embedded analytics via DuckDB."""

    @classmethod
    def name(cls) -> str:
        return "duckdb"

    def schema(self):
        import duckdb
        conn = duckdb.connect(self.options.get("database", ":memory:"))
        return conn.execute(f"DESCRIBE ({self.options['query']})").arrow().schema

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
        yield conn.execute(self.query).fetch_arrow_table()
```

### Filter Pushdown Example

```python
from pysail.spark.datasource import DataSourceReader, EqualTo, GreaterThan, In

class MyReader(DataSourceReader):
    def __init__(self):
        self.pushed_filters = []

    def pushFilters(self, filters):
        for f in filters:
            if isinstance(f, (EqualTo, GreaterThan, In)):
                self.pushed_filters.append(f)
            else:
                yield f  # Return unsupported filters

    def read(self, partition):
        # Apply self.pushed_filters to reduce data at source
        for f in self.pushed_filters:
            if isinstance(f, EqualTo):
                # Add WHERE column = value to query
                pass
        # ... fetch and yield data
```

---

## DDL Schema Reference

When `DataSource.schema()` returns a string, Sail parses it as DDL:

| DDL Type | Arrow Type | Notes |
|----------|------------|-------|
| `INT`, `INTEGER` | Int32 | |
| `BIGINT`, `LONG` | Int64 | |
| `FLOAT`, `REAL` | Float32 | |
| `DOUBLE` | Float64 | |
| `BOOLEAN`, `BOOL` | Boolean | |
| `STRING`, `VARCHAR`, `TEXT`, `CHAR` | Utf8 | |
| `DATE` | Date32 | |
| `TIMESTAMP` | Timestamp(Nanosecond, None) | |
| `DECIMAL(p,s)` | Decimal128(p, s) | |

**For complex types**, use PyArrow Schema:

```python
import pyarrow as pa

def schema(self):
    return pa.schema([
        ("id", pa.int64()),
        ("tags", pa.list_(pa.string())),
        ("metadata", pa.struct([
            ("key", pa.string()),
            ("value", pa.string())
        ]))
    ])
```

---

## Filter Classes Reference

All filter classes use `ColumnPath = Tuple[str, ...]` for nested column support.

| Class | SQL Equivalent | Example |
|-------|----------------|---------|
| `EqualTo(attr, val)` | `attr = val` | `EqualTo(("id",), 5)` |
| `EqualNullSafe(attr, val)` | `attr <=> val` | |
| `GreaterThan(attr, val)` | `attr > val` | |
| `GreaterThanOrEqual(attr, val)` | `attr >= val` | |
| `LessThan(attr, val)` | `attr < val` | |
| `LessThanOrEqual(attr, val)` | `attr <= val` | |
| `In(attr, values)` | `attr IN (...)` | `In(("status",), ("A", "B"))` |
| `IsNull(attr)` | `attr IS NULL` | |
| `IsNotNull(attr)` | `attr IS NOT NULL` | |
| `Not(child)` | `NOT child` | `Not(IsNull(("id",)))` |
| `And(left, right)` | `left AND right` | |
| `Or(left, right)` | `left OR right` | |
| `StringStartsWith(attr, val)` | `attr LIKE 'val%'` | |
| `StringEndsWith(attr, val)` | `attr LIKE '%val'` | |
| `StringContains(attr, val)` | `attr LIKE '%val%'` | |

---

## PySpark Compatibility Details

### Module Shim Mappings

```
pyspark.sql.datasource       → pysail.spark.datasource
pyspark.sql.datasource_internal → pysail.spark.datasource  
pyspark.cloudpickle          → cloudpickle
```

### Server-Side (Automatic)

When Sail unpickles a DataSource, `unpickle_datasource_class()` auto-installs the shim:

```python
# In table_format.rs (Rust)
compat.call_method1("unpickle_datasource_class", (class_bytes,))
```

### Client-Side (Explicit)

For testing without PySpark installed:

```python
from pysail.spark.datasource import install_pyspark_shim
install_pyspark_shim()

# Now can unpickle PySpark-pickled DataSources
import cloudpickle
ds_class = cloudpickle.loads(pickled_bytes)
```

> **Warning**: Don't use the shim when real PySpark is needed in the same process.

---

## Performance Guide

### Data Path Comparison

The Python DataSource supports two data paths with significantly different performance characteristics:

| Path | Method | Throughput | Overhead | Use Case |
|------|--------|------------|----------|----------|
| **Arrow zero-copy** | `yield pa.RecordBatch(...)` | ~1GB/s | Near-zero | Production, high volume |
| **Row-based fallback** | `yield (val1, val2, ...)` | ~10MB/s | O(rows × cols) | Prototyping, compatibility |

### When to Use Each Path

**Arrow RecordBatch (Preferred):**

```python
def read(self, partition):
    # Build data in columnar format directly - FAST
    data = pa.table({
        "id": pa.array(range(1000)),
        "name": pa.array(["Alice"] * 1000),
    })
    for batch in data.to_batches(max_chunksize=8192):
        yield batch  # Zero-copy to Rust via Arrow C Data Interface
```

**Tuple Rows (Fallback):**

```python
def read(self, partition):
    # Simple but slow - each row pickled/unpickled
    for i in range(1000):
        yield (i, f"row_{i}")  # ❌ Avoid for large datasets
```

### Performance Characteristics

1. **Arrow path** ([arrow_utils.rs:35-47](file:///crates/sail-data-source/src/python_datasource/arrow_utils.rs#L35-L47)):
   - Uses Arrow C Data Interface (`RecordBatch::from_pyarrow_bound`)
   - Zero memory copy - pointer passed directly from Python heap to Rust
   - GIL released during Rust processing of the batch
   - Columnar format enables SIMD optimizations downstream

2. **Row path** ([arrow_utils.rs:138-256](file:///crates/sail-data-source/src/python_datasource/arrow_utils.rs#L138-L256)):
   - O(rows × columns) complexity - every cell processed individually
   - Each value pickled via cloudpickle, then unpickled
   - Python object overhead per cell (type checking, memory allocation)
   - GIL held throughout entire conversion

### GIL Metrics

The stream implementation includes built-in GIL contention tracking ([stream.rs](file:///crates/sail-data-source/src/python_datasource/stream.rs)):

```bash
# Enable debug logging to see GIL metrics
RUST_LOG=sail_data_source::python_datasource::stream=info sail spark
```

Output example:
```
[PythonDataSource:partition=0] GIL metrics: wait=5ms, hold=120ms, acquisitions=1, batches=15, rows=122880
```

**Interpreting metrics:**
- `wait > hold`: High GIL contention - other Python code competing for GIL
- `wait < hold`: Normal - this partition is the primary GIL user
- Large `acquisitions`: Frequent GIL re-acquisition (subprocess isolation would help)

### GIL-Releasing Libraries

These libraries release the GIL during I/O and compute, enabling true parallelism:

| Library | Backend | GIL Behavior | Arrow Support |
|---------|---------|--------------|---------------|
| connector-x | Rust | Releases during query | Native |
| DuckDB | C++ | Releases during query | Native |
| NumPy | C | Releases during compute | Via conversion |
| Polars | Rust | Releases during compute | Native |

### Batch Size Tuning

```bash
SAIL_PYTHON_BATCH_SIZE=16384      # Rows per batch (default: 8192)
SAIL_PYTHON_CHANNEL_BUFFER=4      # Batches buffered in channel (default: 16)
```

### Optimization Checklist

1. ✅ **Use Arrow RecordBatch** - Always prefer `yield pa.RecordBatch(...)` over tuples
2. ✅ **Use GIL-releasing libraries** - connector-x, DuckDB, Polars for I/O
3. ✅ **Batch appropriately** - 8K-64K rows per batch balances overhead vs memory
4. ✅ **Use multiple partitions** - Enables parallel execution across partitions
5. ✅ **Monitor GIL metrics** - Watch for high wait times indicating contention

---

## FAQ

### Development

**Q: How do I create a custom DataSource?**
See [Quick Reference](#quick-reference) above.

**Q: Tuple vs RecordBatch yield?**
Use RecordBatch for production (zero-copy). Tuples for prototyping.

**Q: How do I debug filter pushdown?**
```bash
RUST_LOG=sail_data_source::python_datasource::filter=debug sail spark
```

### Architecture

**Q: Why in-process PyO3 instead of subprocess?**
MVP simplicity + zero-copy. Subprocess isolation (Phase 3) adds crash isolation and GIL parallelism.

**Q: How does zero-copy work?**
Arrow C Data Interface exports pointers (not copies) from Python to Rust. Same memory, no copying.

**Q: GIL impact on parallel reads?**
Control plane (schema/partitions) holds GIL briefly. Data plane releases GIL if using GIL-releasing libraries.

### Migration

**Q: Can I use existing PySpark DataSource code?**
Yes, for PySpark 4.0+ DataSource API. Change import from `pyspark.sql.datasource` to `pysail.spark.datasource`.

**Q: Supported Python versions?**
3.9, 3.10, 3.11, 3.12

### Security

**Q: Is cloudpickle safe?**
Cloudpickle can execute arbitrary code. Only load from trusted packages.

**Q: What if Python crashes during read?**
In-process: Query fails, possible process crash. With subprocess isolation (Phase 3): Worker restarts, partition retried.
