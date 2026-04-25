import io
from contextlib import redirect_stdout

import pandas as pd
import pyspark.sql.functions as F  # noqa: N812


def explain_string(df):
    buf = io.StringIO()
    with redirect_stdout(buf):
        df.explain(True)
    return buf.getvalue()


def partition_count(df):
    def counter(_iterator):
        yield pd.DataFrame({"n": [1]})

    return df.mapInPandas(counter, schema="n: long").count()


def partition_ids(df):
    rows = df.select(F.spark_partition_id().alias("pid")).groupBy("pid").count().orderBy("pid").collect()
    return [(row["pid"], row["count"]) for row in rows]


def test_explicit_repartition(spark):
    assert partition_count(spark.range(0, 10, 1, 2)) == 2  # noqa: PLR2004
    assert partition_count(spark.range(0, 10, 1, 3).select("id", F.lit("foo").alias("a")).repartition("a")) == 3  # noqa: PLR2004
    assert partition_count(spark.sql("SELECT 1 AS a, 'foo' as b").repartition(5)) == 5  # noqa: PLR2004
    assert partition_count(spark.sql("SELECT 1 AS a, 'foo' as b").repartition(6, "b", "a")) == 6  # noqa: PLR2004
    assert partition_count(spark.sql("SELECT 1 AS a, 'foo' as b").repartition("a")) == 1


def test_explicit_coalesce(spark):
    assert partition_count(spark.range(0, 10, 1, 2).coalesce(1)) == 1
    assert partition_count(spark.range(0, 10, 1, 2).coalesce(3)) == 2  # noqa: PLR2004
    assert partition_count(spark.range(0, 10, 1, 4).coalesce(2)) == 2  # noqa: PLR2004


def test_spark_partition_id_observes_existing_partitions(spark):
    assert partition_ids(spark.range(0, 10, 1, 2)) == [(0, 5), (1, 5)]
    assert partition_ids(spark.range(0, 10, 1, 2).coalesce(1)) == [(0, 10)]
    assert partition_ids(spark.range(0, 20, 1, 4).coalesce(2)) == [(0, 10), (1, 10)]


def test_repartition_without_expressions_uses_round_robin(spark):
    plan = explain_string(spark.range(0, 20, 1, 4).repartition(3))
    assert "RoundRobin" in plan
    assert "Hash(" not in plan
    assert "hashpartitioning" not in plan
