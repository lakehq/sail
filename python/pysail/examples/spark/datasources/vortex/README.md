# Vortex DataSource via Python DataSource API

Read and query [Vortex](https://github.com/vortex-data/vortex) files through Sail using PySpark's Python DataSource API — no Rust changes needed.

## Setup

### 1. Install dependencies in the hatch environment

```bash
hatch run pip install vortex-data
```

> **Note:** `vortex-data` 0.58.0 requires `substrait==0.23.0`. If you get a `ModuleNotFoundError: No module named 'substrait.gen.proto'`, downgrade:
> ```bash
> hatch run pip install 'substrait==0.23.0'
> ```

### 2. Start the Sail server with the correct Python

The server's embedded Python (PyO3) must have access to `pyspark` and `vortex-data`. This requires:

1. Building with `PYO3_PYTHON` pointing to the hatch venv (absolute path!)
2. Setting `PYTHONPATH` so the embedded interpreter finds the packages

```bash
# Build pysail into the venv
hatch run maturin develop

# Start the server (run from the repo root)
SAIL_VENV="$(pwd)/.venvs/default" \
VIRTUAL_ENV="$SAIL_VENV" \
PYTHONPATH="$SAIL_VENV/lib/python3.11/site-packages:$(pwd)/python" \
PYO3_PYTHON="$SAIL_VENV/bin/python" \
RUST_LOG='sail=debug' RUST_BACKTRACE=1 \
cargo run -p sail-cli -- spark server --port 50051
```

### 3. Run the benchmark

In a separate terminal:

```bash
export SPARK_REMOTE="sc://localhost:50051"
hatch run python python/pysail/examples/spark/datasources/vortex/benchmark.py
```

Options via environment variables:

| Variable | Default | Description |
|---|---|---|
| `SPARK_REMOTE` | `sc://localhost:50051` | Sail server address |
| `BENCH_ROWS` | `1000000` | Number of rows to generate |
| `BENCH_RUNS` | `3` | Number of runs per query |

## Quick test (no benchmark)

```python
import os
os.environ["SPARK_REMOTE"] = "sc://localhost:50051"

import vortex
import pyarrow as pa
from pyspark.sql import SparkSession

# Write a Vortex file
table = pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"]})
vortex.io.write(table, "/tmp/test.vtx")

# Read it through Sail
from pysail.examples.spark.datasources.vortex.benchmark import VortexDataSource
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.dataSource.register(VortexDataSource)
spark.read.format("vortex").option("path", "/tmp/test.vtx").load().show()
```

## How it works

```
PySpark client  -->  Sail server (gRPC)  -->  Python DataSource (embedded PyO3)
                                                      |
                                              vortex.open(path)
                                                      |
                                              .vtx -> Arrow batches -> rows
```

1. Client registers `VortexDataSource` and calls `.read.format("vortex")`
2. Sail serializes the DataSource class and sends it to the server
3. Server deserializes and calls `schema()` to get the DDL
4. On `.collect()`, server calls `read()` which opens the `.vtx` file via `vortex-data`
5. Rows are yielded back through the Python DataSource protocol

## Benchmark results (1M rows)

### Python DataSource with filter pushdown + RecordBatch yield

| Operation | Parquet | Vortex | Winner |
|---|---|---|---|
| Write | 0.055s | 0.043s | **Vortex (1.28x)** |
| Full Scan | 9.730s | 9.460s | **Vortex (1.03x)** |
| Filter Scan | 4.909s | 4.935s | Parquet (1.01x) |
| Projection | 5.235s | 5.272s | Parquet (1.01x) |
| Aggregation | 0.464s | 0.076s | **Vortex (6.10x)** |
| Count | 0.009s | 0.024s | Parquet (2.74x) |
| File Size | 12.85 MB | 9.20 MB | **Vortex (0.72x)** |

Vortex wins 4 out of 7 categories. The `pushFilters()` method pushes predicates into `vortex.scan(expr=...)`, and `read()` yields Arrow `RecordBatch` instead of row-by-row tuples.

### Native Rust integration (PR #1334)

| Operation | Parquet | Vortex | Winner |
|---|---|---|---|
| Write | 0.320s | 0.229s | **Vortex (1.40x)** |
| Full Scan | 0.014s | 0.007s | **Vortex (2.00x)** |
| Filter Scan | 0.019s | 0.020s | Parquet (1.09x) |
| Projection | 0.008s | 0.007s | **Vortex (1.06x)** |
| Aggregation | 0.045s | 0.031s | **Vortex (1.45x)** |
| File Size | 6.01 MB | 9.90 MB | Parquet (0.61x) |

### Key optimizations

The Python DataSource uses two optimizations to close the gap with Parquet's native Rust reader:

1. **Filter pushdown** via `pushFilters()`: PySpark filters are converted to `vortex.expr` expressions and passed to `vf.scan(expr=...)`, so Vortex filters data before returning it
2. **RecordBatch yield**: `read()` yields Arrow `RecordBatch` objects (columnar) instead of `tuple(...)` per row, eliminating the `.as_py()` conversion overhead

The remaining gap (Filter Scan, Projection, Count) is due to Parquet's native Rust reader having zero Python overhead.

## Troubleshooting

| Error | Solution |
|---|---|
| `No module named 'pyspark'` | Server needs `PYTHONPATH` set (see step 2) |
| `No module named 'substrait.gen.proto'` | `hatch run pip install 'substrait==0.23.0'` |
| `Schema error: Failed to parse DDL` | The `schema()` method must return Spark DDL, not Arrow format |
| `PYO3_PYTHON` build error | Use absolute path: `PYO3_PYTHON="$(pwd)/.venvs/default/bin/python"` |
