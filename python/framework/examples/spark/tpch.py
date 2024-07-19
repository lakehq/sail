from __future__ import annotations

import argparse
import contextlib

from pyspark.sql import SparkSession


class TpchBenchmark:
    TABLE_NAMES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]

    def __init__(self, remote: str, data_path: str, query_path: str):
        self.remote = remote
        self.data_path = data_path
        self.query_path = query_path

    @contextlib.contextmanager
    def spark_session(self):
        spark = SparkSession.builder.remote(self.remote).appName("TPC-H").getOrCreate()
        for table in self.TABLE_NAMES:
            path = f"{self.data_path}/{table}.parquet"
            df = spark.read.parquet(path)
            df.createOrReplaceTempView(table)
        try:
            yield spark
        finally:
            spark.stop()

    def _run_query(self, spark: SparkSession, query: int):
        with open(f"{self.query_path}/q{query}.sql") as f:
            for sql in f.read().split(";"):
                if not sql.strip():
                    continue
                sql = sql.strip().replace("create view", "create temp view")
                df = spark.sql(sql)
                _ = df.collect()

    def run(self, query: int | None = None):
        with self.spark_session() as spark:
            if query is not None:
                self._run_query(spark, query)
            else:
                for query in range(1, 23):
                    self._run_query(spark, query)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--remote", type=str, default="sc://localhost:50051")
    parser.add_argument("--data-path", type=str, required=True)
    parser.add_argument("--query-path", type=str, required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--console", action="store_true")
    group.add_argument("--query", type=int, choices=range(1, 23))
    group.add_argument("--query-all", action="store_true")
    args = parser.parse_args()

    benchmark = TpchBenchmark(args.remote, args.data_path, args.query_path)
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
        benchmark.run(args.query)
    elif args.query_all:
        benchmark.run()


if __name__ == "__main__":
    main()
