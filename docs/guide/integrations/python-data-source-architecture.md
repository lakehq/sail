---
title: Python Data Source Architecture
description: Lakesail's bridge for running Python data sources with zero-copy Arrow interoperability.
---

# Python Data Source Architecture

Lakesail ships with the `sail-python-datasource` bridge, which lets you implement data sources in Python while relying on the Rust engine for planning, scheduling, and zero-copy Arrow transport.

## High-Level Flow

```
 ┌────────────────────────────────────────────────────────────┐
 │                     Lakesail (Rust)                        │
 │  ┌──────────────────────────────────────────────────────┐  │
 │  │ PythonDataSourceFormat (TableFormat)                 │  │
 │  │  - Registered as "python" format                     │  │
 │  └────────────────────┬─────────────────────────────────┘  │
 │                       │                                    │
 │  ┌──────────────────────────────────────────────────────┐  │
 │  │ PythonTableProvider (TableProvider)                  │  │
 │  │  - Calls Python to infer schema                      │  │
 │  │  - Calls Python to plan partitions                   │  │
 │  └────────────────────┬─────────────────────────────────┘  │
 │                       │                                    │
 │  ┌──────────────────────────────────────────────────────┐  │
 │  │ PythonExec (ExecutionPlan)                           │  │
 │  │  - Executes partitions in parallel                   │  │
 │  │  - Zero-copy Arrow transfer via FFI                  │  │
 │  └────────────────────┬─────────────────────────────────┘  │
 │                       │ PyO3 + Arrow C Data Interface      │
 └───────────────────────┼────────────────────────────────────┘
                         │
 ┌───────────────────────▼────────────────────────────────────┐
 │                  Python Data Source                        │
 │  ┌──────────────────────────────────────────────────────┐  │
 │  │ User-Defined Python Class                            │  │
 │  │  - infer_schema(options) -> pa.Schema                │  │
 │  │  - plan_partitions(options) -> List[dict]            │  │
 │  │  - read_partition(spec, options) -> Iterator[Batch]  │  │
 │  └──────────────────────────────────────────────────────┘  │
 └────────────────────────────────────────────────────────────┘
```

## Why Use the Bridge

- Implement rich data sources in Python without touching Rust.
- Share Arrow buffers between runtimes for zero-copy performance.
- Execute partitions in parallel across the Lakesail cluster.
- Plug the same provider into Spark Connect with no extra wiring.

## Implementing a Provider

Create a Python class that exposes three lifecycle methods. The bridge passes all `.option()` values as dictionaries.

```python
import pyarrow as pa
from typing import Dict, List, Iterator, Any

class CustomDataSource:
    def infer_schema(self, options: Dict[str, str]) -> pa.Schema:
        """Return the Arrow schema for the data."""

    def plan_partitions(self, options: Dict[str, str]) -> List[Dict[str, Any]]:
        """Return JSON-serialisable partition specs."""

    def read_partition(
        self,
        partition_spec: Dict[str, Any],
        options: Dict[str, str],
    ) -> Iterator[pa.RecordBatch]:
        """Yield Arrow RecordBatches for a partition."""
```

Use the provider from Spark or pysail helpers:

```python
df = spark.read.format("python") \
    .option("python_module", "pysail.jdbc.datasource") \
    .option("python_class", "JDBCArrowDataSource") \
    .option("url", "jdbc:postgresql://localhost/mydb") \
    .load()
```

### Custom Provider Example

```python
import pyarrow as pa
import requests

class RestAPIDataSource:
    """Paginated REST API example."""

    def infer_schema(self, options):
        sample = requests.get(f"{options['endpoint']}?limit=1").json()
        return pa.schema([
            ("id", pa.int64()),
            ("name", pa.string()),
            ("created_at", pa.timestamp("us")),
        ])

    def plan_partitions(self, options):
        total = int(options.get("total_pages", "10"))
        return [{"page": i} for i in range(total)]

    def read_partition(self, partition_spec, options):
        response = requests.get(
            f"{options['endpoint']}?page={partition_spec['page']}"
        ).json()
        for batch in pa.Table.from_pylist(response["items"]).to_batches():
            yield batch
```

## Runtime Details

- **Zero-copy Arrow** – `PythonExec` converts PyArrow batches through the Arrow C Data Interface, sharing buffers without copying.
- **Partition parallelism** – DataFusion executes partitions concurrently by calling `read_partition()` per task.
- **Error handling** – Python exceptions surface as `PythonDataSourceError` instances so failures integrate with the query engine.

```rust
Python::with_gil(|py| {
    datasource.call_method1("read_partition", (spec, options))
        .map_err(|e| PythonDataSourceError::ExecutionError(e.to_string()))
})
```

## Built-in JDBC Provider

The packaged `pysail.jdbc` module implements the interface with ConnectorX, ADBC, and a pure Python fallback. Common options include:

- `url`: JDBC URL (e.g. `jdbc:postgresql://host:5432/db`)
- `dbtable`: Table name or subquery
- `engine`: `connectorx` (default), `adbc`, or `fallback`
- Range partitioning knobs: `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`

> Tip: Install `connectorx` for best performance and leverage partitioning for large tables.

## Example Use Cases

- Databases: JDBC/ODBC sources, NoSQL connectors
- Cloud storage: S3, GCS, Azure Blob with custom slicing
- APIs: REST, GraphQL, gRPC with pagination
- File formats: Proprietary loaders that emit Arrow batches
- ML pipelines: Serving model predictions as queryable data sets

## Test Coverage

- `python/pysail/tests/jdbc/test_datasource.py`
- `python/pysail/tests/jdbc/test_jdbc_options.py`
- `python/pysail/tests/jdbc/test_jdbc_url_parser.py`
- `python/pysail/tests/jdbc/test_partition_planner.py`
- `python/pysail/tests/jdbc/test_utils.py`
- `python/pysail/tests/spark/datasource/test_jdbc.py`

These suites validate lifecycle semantics, option parsing, URL handling, partition planning, and Spark Connect integration end to end.
