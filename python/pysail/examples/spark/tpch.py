from __future__ import annotations

import argparse
import contextlib
import time
from pathlib import Path

from pyspark.sql import SparkSession


class TpchBenchmark:
    TABLE_NAMES = ("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")

    def __init__(
        self,
        url: str,
        data_path: str,
        query_path: str,
        format: str = "parquet",  # noqa: A002
    ):
        self.url = url
        self.data_path = data_path
        self.query_path = query_path
        self.format = format

    def _is_remote(self):
        return self.url.startswith("sc://")

    def _load_tables(self, spark: SparkSession):
        """Load tables from disk into temp views."""
        start_time = time.time()
        for table in self.TABLE_NAMES:
            if self.format == "delta":
                path = Path(self.data_path) / table
                df = spark.read.format("delta").load(str(path))
            elif self.format == "iceberg":
                # Iceberg requires file:// URL for local paths (especially with Sail)
                path = Path(self.data_path) / table
                uri = path.resolve().as_uri() + "/"
                df = spark.read.format("iceberg").load(uri)
            elif self.format == "parquet":
                path = Path(self.data_path) / f"{table}.parquet"
                df = spark.read.parquet(str(path))
            elif self.format == "parquet-partitioned":
                path = Path(self.data_path) / table / "*.parquet"
                df = spark.read.parquet(str(path))
            else:
                msg = f"unsupported format: {self.format}"
                raise ValueError(msg)
            df.createOrReplaceTempView(table)
        end_time = time.time()
        print(f"Loaded tables in {end_time - start_time} seconds.")

    @contextlib.contextmanager
    def spark_session(self):
        builder = SparkSession.builder.remote(self.url) if self._is_remote() else SparkSession.builder.master(self.url)
        spark = builder.appName("TPC-H").getOrCreate()
        self._load_tables(spark)
        try:
            yield spark
        finally:
            spark.stop()

    def _run_query(self, spark: SparkSession, query: int, run: int, explain: bool):  # noqa: FBT001
        total_time = 0
        with open(f"{self.query_path}/q{query}.sql") as f:
            for s, sql in enumerate(f.read().split(";")):
                sql = sql.strip()  # noqa: PLW2901
                if not sql:
                    continue
                sql = sql.replace("create view", "create temp view")  # noqa: PLW2901
                if explain:
                    df = spark.sql(f"EXPLAIN EXTENDED {sql}")
                    for (line,) in df.collect():
                        print(line)
                else:
                    start_time = time.time()
                    df = spark.sql(sql)
                    rows = df.toPandas()
                    end_time = time.time()
                    query_time = end_time - start_time
                    total_time += query_time
                    print(f"Run {run + 1} query {query}:{s} returned {len(rows)} row(s) and took {query_time} seconds.")
        return total_time

    def run(self, query: int | None = None, explain: bool = False, num_runs: int = 1):  # noqa: FBT001, FBT002
        with self.spark_session() as spark:
            if query is not None:
                for run in range(num_runs):
                    self._run_query(spark, query, run, explain)
            else:
                all_total_time = []
                for run in range(num_runs):
                    total_time = 0
                    for q in range(1, 23):
                        total_time += self._run_query(spark, q, run, explain)
                    all_total_time.append(total_time)
                if not explain and all_total_time:
                    for i, t in enumerate(all_total_time):
                        print(f"Run {i + 1} total time for all queries: {t} seconds.")
                    print(f"Maximum total time across runs: {max(all_total_time)} seconds.")
                    print(f"Minimum total time across runs: {min(all_total_time)} seconds.")
                    print(f"Average total time across runs: {sum(all_total_time) / len(all_total_time)} seconds.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", type=str, default="sc://localhost:50051")
    parser.add_argument("--data-path", type=str, required=True)
    parser.add_argument("--query-path", type=str, required=True)
    parser.add_argument("--num-runs", type=int, default=1)
    parser.add_argument(
        "--format", type=str, default="parquet", choices=["parquet", "parquet-partitioned", "delta", "iceberg"]
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--console", action="store_true")
    group.add_argument("--query", type=int, choices=range(1, 23))
    group.add_argument("--query-all", action="store_true")
    group.add_argument("--explain", type=int, choices=range(1, 23))
    args = parser.parse_args()

    benchmark = TpchBenchmark(args.url, args.data_path, args.query_path, args.format)
    if args.console:
        with benchmark.spark_session() as spark:
            import code
            import readline
            from rlcompleter import Completer

            namespace = {"spark": spark}
            readline.parse_and_bind("tab: complete")
            readline.set_completer(Completer(namespace).complete)
            code.interact(
                local=namespace,
                banner="Spark TPC-H Data Explorer\nThe Spark session is available as `spark`.",
            )
    elif args.query:
        benchmark.run(args.query, explain=False, num_runs=args.num_runs)
    elif args.explain:
        benchmark.run(args.explain, explain=True, num_runs=args.num_runs)
    elif args.query_all:
        benchmark.run(query=None, explain=False, num_runs=args.num_runs)


if __name__ == "__main__":
    main()
