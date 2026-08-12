---
title: Running Benchmarks
rank: 90
---

# Running Benchmarks

::: info
This is an experimental process. The script used in this page may be removed in the future, when there is a better way to run benchmarks.
:::

You can use the following command to run individual queries from the derived TPC-H benchmark.

```shell
python -m pysail.examples.spark.tpch \
  --data-path "$BENCHMARK_PATH"/tpch/data \
  --query-path "$BENCHMARK_PATH"/tpch/queries \
  --query 1
```

You need to run this in a Python virtual environment with PySail and PySpark installed.
If you are in the project source directory, you can use Hatch to set up the environment.
Run `hatch run maturin develop` to build and install PySail for the default Hatch environment,
and then run `hatch shell` to enter the environment.

`BENCHMARK_PATH` is the absolute path of the [DataFusion benchmarks repository](https://github.com/apache/datafusion-benchmarks).
Please follow the README in that repository to generate the TPC-H data.

- You can use `--console` instead of `--query [N]` to start a PySpark console and explore the TPC-H data interactively.
- You can use `--url` to specify the Spark URL. Use `local` for Spark local mode (JVM) and use `sc://localhost:50051` for Spark Connect (the default).
- You can use `--format` to specify the data format: `parquet` (default), `delta`, or `iceberg`.
- You can use `--include-load-time` to include data loading time in benchmark measurements. By default, tables are pre-loaded before timing starts.
- You can use `--help` to see all the supported arguments.

## Converting to Delta Lake Format

To benchmark with Delta Lake format, first convert the Parquet data using the provided utility:

```shell
python -m pysail.examples.spark.convert_to_delta \
  --input-path "$BENCHMARK_PATH"/tpch/data \
  --output-path "$BENCHMARK_PATH"/tpch/data_delta
```

Then run the benchmark with `--format delta` and `--data-path` pointing to the Delta Lake directory.

## Running the Server

Please use the following command to run the Spark Connect server.
It runs the server in release mode without debug logs.

```shell
env RUST_LOG=info BENCHMARK=1 scripts/spark-tests/run-server.sh
```

## Rust Hot-Path Microbenchmarks

The Rust workspace contains focused benchmarks for object-store caching, the SQL frontend, and Iceberg metadata planning. Run them with Cargo's optimized benchmark profile:

```shell
cargo bench -p sail-object-store --bench object_store_cache
cargo bench -p sail-sql-analyzer --bench sql_frontend
cargo bench -p sail-iceberg --bench metadata_updates
cargo bench -p sail-iceberg --bench manifest_parsing
cargo bench -p sail-iceberg --bench delete_index
cargo bench -p sail-iceberg --bench metadata_discovery
```

Each executable performs warm-up iterations and reports the minimum, median, and maximum time across repeated samples. Benchmarks that process bytes also report throughput. Set `SAIL_BENCH_FILTER` to run only cases whose names contain a substring:

```shell
SAIL_BENCH_FILTER=metadata_parse cargo bench -p sail-iceberg --bench metadata_updates
```

For before-and-after comparisons, build both revisions with the same Rust toolchain, target CPU flags, benchmark profile, container resource limits, and machine load. Report medians rather than a single run, retain the raw output, and include the exact commit IDs so another contributor can reproduce the result.
