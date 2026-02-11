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

### Python DataSource (this example)

| Operation | Parquet | Vortex | Winner |
|---|---|---|---|
| Write | 0.062s | 0.066s | Parquet (1.06x) |
| Full Scan | 9.458s | 11.962s | Parquet (1.26x) |
| Filter Scan | 4.937s | 11.340s | Parquet (2.30x) |
| Projection | 5.240s | 11.342s | Parquet (2.16x) |
| Aggregation | 0.466s | 9.798s | Parquet (21.04x) |
| Count | 0.010s | 9.781s | Parquet (947x) |
| File Size | 12.85 MB | 9.20 MB | **Vortex (0.72x)** |

### Native Rust integration (PR #1334)

| Operation | Parquet | Vortex | Winner |
|---|---|---|---|
| Write | 0.320s | 0.229s | **Vortex (1.40x)** |
| Full Scan | 0.014s | 0.007s | **Vortex (2.00x)** |
| Filter Scan | 0.019s | 0.020s | Parquet (1.09x) |
| Projection | 0.008s | 0.007s | **Vortex (1.06x)** |
| Aggregation | 0.045s | 0.031s | **Vortex (1.45x)** |
| File Size | 6.01 MB | 9.90 MB | Parquet (0.61x) |

### Why the difference?

Parquet uses Sail's **native Rust reader** (columnar Arrow batches, projection/predicate pushdown), while the Vortex Python DataSource goes through a slow path:

```
Parquet:  .parquet  ->  Rust reader  ->  Arrow batches  ->  result
Vortex:   .vtx  ->  Python  ->  vortex-data  ->  Arrow  ->  .as_py()  ->  yield row-by-row  ->  Sail
```

The bottleneck is `yield tuple(...)` row-by-row. Operations like Count and Aggregation are hit hardest because Parquet resolves them via metadata or columnar processing, while Vortex still reads all 1M rows through Python.

The native Rust integration eliminates this overhead entirely, making Vortex faster than Parquet for most operations.

## Troubleshooting

| Error | Solution |
|---|---|
| `No module named 'pyspark'` | Server needs `PYTHONPATH` set (see step 2) |
| `No module named 'substrait.gen.proto'` | `hatch run pip install 'substrait==0.23.0'` |
| `Schema error: Failed to parse DDL` | The `schema()` method must return Spark DDL, not Arrow format |
| `PYO3_PYTHON` build error | Use absolute path: `PYO3_PYTHON="$(pwd)/.venvs/default/bin/python"` |
