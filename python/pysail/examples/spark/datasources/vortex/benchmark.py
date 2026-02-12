"""
Vortex vs Parquet benchmark via Sail Python DataSource.

Replicates the benchmark from PR #1334 (native Rust integration)
using the Python DataSource API instead.

Requirements:
    pip install vortex-data pyspark pyarrow

Usage:
    # Start the Sail server first (see README.md in this directory)
    export SPARK_REMOTE="sc://localhost:50051"
    python benchmark.py
"""

import os
import random
import statistics
import tempfile
import time

import pyarrow as pa
import pyarrow.parquet as pq
from datasource_vortex import VortexDataSource
from pyspark.sql import SparkSession

import vortex


def generate_data(n=1_000_000):
    """Generate test data matching PR #1334 benchmark."""
    random.seed(42)
    names = ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi"]
    categories = ["A", "B", "C", "D", "E"]

    return pa.table(
        {
            "id": pa.array(range(n), type=pa.int64()),
            "name": pa.array([names[i % len(names)] for i in range(n)], type=pa.string()),
            "category": pa.array([categories[i % len(categories)] for i in range(n)], type=pa.string()),
            "value": pa.array(
                [random.random() * 1000.0 for _ in range(n)],  # noqa: S311
                type=pa.float64(),
            ),
            "count": pa.array(
                [random.randint(1, 100) for _ in range(n)],  # noqa: S311
                type=pa.int64(),
            ),
        }
    )


def bench(spark, label, sql_parquet, sql_vortex, runs=3):
    """Run a query benchmark comparing Parquet vs Vortex."""
    # Warmup
    spark.sql(sql_parquet).collect()
    spark.sql(sql_vortex).collect()

    parquet_times = []
    vortex_times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        spark.sql(sql_parquet).collect()
        parquet_times.append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        spark.sql(sql_vortex).collect()
        vortex_times.append(time.perf_counter() - t0)

    p_avg = statistics.mean(parquet_times)
    v_avg = statistics.mean(vortex_times)
    winner = "Parquet" if p_avg < v_avg else "Vortex"
    ratio = max(p_avg, v_avg) / min(p_avg, v_avg) if min(p_avg, v_avg) > 0 else 0
    print(f"  {label:20s} | Parquet: {p_avg:.3f}s | Vortex: {v_avg:.3f}s | {winner} {ratio:.2f}x faster")
    return {"parquet": p_avg, "vortex": v_avg}


def main():
    n = int(os.environ.get("BENCH_ROWS", "1000000"))
    runs = int(os.environ.get("BENCH_RUNS", "3"))
    spark_remote = os.environ.get("SPARK_REMOTE", "sc://localhost:50051")

    print("Vortex vs Parquet Benchmark (Python DataSource + Pushdown)")
    print(f"  Rows: {n:,}  |  Runs: {runs}  |  Server: {spark_remote}")
    print()

    # Generate data
    print(f"Generating {n:,} rows...")
    arrow_table = generate_data(n)

    tmpdir = tempfile.mkdtemp(prefix="vortex_bench_")
    parquet_path = os.path.join(tmpdir, "data.parquet")
    vortex_path = os.path.join(tmpdir, "data.vtx")

    # Write benchmark
    print("\n=== WRITE ===")
    t0 = time.perf_counter()
    pq.write_table(arrow_table, parquet_path)
    parquet_write = time.perf_counter() - t0
    print(f"  Parquet: {parquet_write:.3f}s")

    t0 = time.perf_counter()
    vortex.io.write(arrow_table, vortex_path)
    vortex_write = time.perf_counter() - t0
    print(f"  Vortex:  {vortex_write:.3f}s")

    # File sizes
    parquet_size = os.path.getsize(parquet_path)
    vortex_size = os.path.getsize(vortex_path)
    print("\n=== FILE SIZE ===")
    print(f"  Parquet: {parquet_size / 1024 / 1024:.2f} MB")
    print(f"  Vortex:  {vortex_size / 1024 / 1024:.2f} MB")
    print(f"  Ratio:   {vortex_size / parquet_size:.2f}x")

    # Connect to Sail
    spark = SparkSession.builder.remote(spark_remote).getOrCreate()
    spark.dataSource.register(VortexDataSource)

    # Load tables
    df_parquet = spark.read.parquet(parquet_path)
    df_parquet.createOrReplaceTempView("bench_parquet")

    df_vortex = spark.read.format("vortex").option("path", vortex_path).load()
    df_vortex.createOrReplaceTempView("bench_vortex")

    # Query benchmarks
    print(f"\n=== QUERIES (avg of {runs} runs) ===")
    results = {}

    results["Full Scan"] = bench(
        spark,
        "Full Scan",
        "SELECT * FROM bench_parquet",
        "SELECT * FROM bench_vortex",
        runs,
    )
    results["Filter Scan"] = bench(
        spark,
        "Filter Scan",
        "SELECT * FROM bench_parquet WHERE value > 500",
        "SELECT * FROM bench_vortex WHERE value > 500",
        runs,
    )
    results["Projection"] = bench(
        spark,
        "Projection",
        "SELECT id, name FROM bench_parquet",
        "SELECT id, name FROM bench_vortex",
        runs,
    )
    results["Aggregation"] = bench(
        spark,
        "Aggregation",
        "SELECT category, AVG(value), SUM(count) FROM bench_parquet GROUP BY category",
        "SELECT category, AVG(value), SUM(count) FROM bench_vortex GROUP BY category",
        runs,
    )
    results["Count"] = bench(
        spark,
        "Count",
        "SELECT COUNT(*) FROM bench_parquet",
        "SELECT COUNT(*) FROM bench_vortex",
        runs,
    )

    # Summary table
    print(f"\n{'=' * 70}")
    print(f"{'SUMMARY':^70}")
    print(f"{'=' * 70}")
    print(f"  {'Operation':<20} {'Parquet':>10} {'Vortex':>10} {'Winner':>15}")
    print(f"  {'-' * 60}")
    print(
        f"  {'Write':<20} {parquet_write:>9.3f}s {vortex_write:>9.3f}s "
        f"{'Vortex' if vortex_write < parquet_write else 'Parquet':>15}"
    )
    for op, r in results.items():
        winner = "Vortex" if r["vortex"] < r["parquet"] else "Parquet"
        print(f"  {op:<20} {r['parquet']:>9.3f}s {r['vortex']:>9.3f}s {winner:>15}")
    print(
        f"  {'File Size':<20} {parquet_size / 1024 / 1024:>8.2f}MB "
        f"{vortex_size / 1024 / 1024:>8.2f}MB "
        f"{'Vortex' if vortex_size < parquet_size else 'Parquet':>15}"
    )
    print("\n  Note: Vortex uses Python DataSource with filter pushdown + RecordBatch yield.")
    print("  Native Rust integration (PR #1334) would be even faster.")
    print(f"\n  Temp dir: {tmpdir}")


if __name__ == "__main__":
    main()
