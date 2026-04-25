import io
from contextlib import redirect_stdout

import pandas as pd
import pyarrow as pa
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


def test_repartition_without_expressions_distributes_rows_round_robin(spark):
    ids = [
        row["pid"]
        for row in spark.range(0, 64, 1, 9)
        .repartition(10)
        .select(F.spark_partition_id().alias("pid"))
        .distinct()
        .orderBy("pid")
        .collect()
    ]
    assert ids == list(range(10))


def test_repartition_before_global_sort_preserves_rows(spark):
    rows = spark.range(0, 50_000).repartition(10).orderBy("id").collect()

    assert len(rows) == 50_000  # noqa: PLR2004
    assert rows[0]["id"] == 0
    assert rows[-1]["id"] == 49_999  # noqa: PLR2004


def test_repartition_before_python_map_udfs_preserves_rows(spark):
    def pandas_mapper(iterator):
        for _ in iterator:
            yield pd.DataFrame({"value": list(range(100))})

    pandas_values = {
        row["value"] for row in spark.range(10).repartition(1).mapInPandas(pandas_mapper, "value long").collect()
    }
    assert pandas_values == set(range(100))

    def arrow_mapper(iterator):
        for _ in iterator:
            yield pa.RecordBatch.from_pandas(pd.DataFrame({"value": list(range(100))}))

    arrow_values = {
        row["value"] for row in spark.range(10).repartition(1).mapInArrow(arrow_mapper, "value long").collect()
    }
    assert arrow_values == set(range(100))


def test_repartition_before_grouped_pandas_udf_and_sort_preserves_groups(spark):
    df = (
        spark.range(2)
        .join(spark.range(4).withColumnRenamed("id", "day"))
        .join(spark.range(10_000).withColumnRenamed("id", "value"))
        .repartition(10)
    )

    def count_group(pdf):
        return pd.DataFrame(
            {
                "id": [pdf["id"][0]],
                "day": [pdf["day"][0]],
                "count": [len(pdf.index)],
            }
        )

    rows = (
        df.groupBy("id", "day")
        .applyInPandas(count_group, "id long, day long, count long")
        .orderBy("id", "day")
        .collect()
    )

    assert [(row["id"], row["day"], row["count"]) for row in rows] == [
        (group_id, day, 10_000) for group_id in range(2) for day in range(4)
    ]
