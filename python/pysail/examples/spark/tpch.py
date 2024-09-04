from __future__ import annotations

import argparse
import contextlib
import time

from pyspark.sql import SparkSession


class TpchBenchmark:
    TABLE_NAMES = ("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")

    def __init__(self, url: str, data_path: str, query_path: str):
        self.url = url
        self.data_path = data_path
        self.query_path = query_path

    def _is_remote(self):
        return self.url.startswith("sc://")

    @contextlib.contextmanager
    def spark_session(self):
        builder = SparkSession.builder.remote(self.url) if self._is_remote() else SparkSession.builder.master(self.url)
        spark = builder.appName("TPC-H").getOrCreate()
        for table in self.TABLE_NAMES:
            path = f"{self.data_path}/{table}.parquet"
            df = spark.read.parquet(path)
            df.createOrReplaceTempView(table)
        try:
            yield spark
        finally:
            spark.stop()

    def _run_query(self, spark: SparkSession, query: int, explain: bool):  # noqa: FBT001
        with open(f"{self.query_path}/q{query}.sql") as f:
            for sql in f.read().split(";"):
                sql = sql.strip()  # noqa: PLW2901
                if not sql:
                    continue
                sql = sql.replace("create view", "create temp view")  # noqa: PLW2901
                print(sql)
                if explain:
                    if self._is_remote():
                        df = spark.sql(f"EXPLAIN VERBOSE {sql}")
                        for type_, line in df.collect():
                            if type_:
                                print()
                                print(f"== {type_} ==")
                            print(line)
                    else:
                        df = spark.sql(f"EXPLAIN EXTENDED {sql}")
                        for (line,) in df.collect():
                            print(line)
                else:
                    start_time = time.time()
                    df = spark.sql(sql)
                    rows = df.collect()
                    end_time = time.time()
                    print(f"The query returned {len(rows)} rows and took {end_time - start_time} seconds.")

    def run(self, query: int | None = None, explain: bool = False):  # noqa: FBT001, FBT002
        with self.spark_session() as spark:
            if query is not None:
                self._run_query(spark, query, explain)
            else:
                for query in range(1, 23):
                    self._run_query(spark, query, explain)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", type=str, default="sc://localhost:50051")
    parser.add_argument("--data-path", type=str, required=True)
    parser.add_argument("--query-path", type=str, required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--console", action="store_true")
    group.add_argument("--query", type=int, choices=range(1, 23))
    group.add_argument("--query-all", action="store_true")
    group.add_argument("--explain", type=int, choices=range(1, 23))
    args = parser.parse_args()

    benchmark = TpchBenchmark(args.url, args.data_path, args.query_path)
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
    elif args.explain:
        benchmark.run(args.explain, explain=True)
    elif args.query_all:
        benchmark.run()


if __name__ == "__main__":
    main()
