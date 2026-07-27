"""Input-order preservation cases that are NOT expressible as SQL feature scenarios.

The SQL-expressible majority of the old ``test_input_order.txt`` doctests now lives in
``features/aggregate/input_order.feature``. This module keeps the cases that need the
DataFrame API, Python UDFs, file I/O, partitioning, or plan inspection — plus one known
Sail-vs-Spark divergence (floating-point summation order).

Note: the ``pandas_udf`` tests require the Python UDF worker, which does not run against a
local Sail server (they pass against JVM Spark, matching ``udf/test_pandas_agg_window.py``).
"""

import re
import tempfile

import pandas as pd
import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.functions import pandas_udf

from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.mark.skipif(
    not is_jvm_spark(),
    reason="requires the Python UDF worker (JVM Spark or a sail-server-python launch)",
)
def test_pandas_udf_first_arrival_preserves_order(spark):
    df = spark.createDataFrame([(30, "c"), (10, "a"), (20, "b")], ("v", "k"))

    @pandas_udf("long")
    def first_arrival(v: pd.Series) -> int:
        return int(v.iloc[0])

    result = df.orderBy("k").groupBy().agg(first_arrival("v").alias("fa")).collect()
    assert [tuple(r) for r in result] == [(10,)]


@pytest.mark.skipif(
    not is_jvm_spark(),
    reason="requires the Python UDF worker (JVM Spark or a sail-server-python launch)",
)
def test_pandas_udf_first_arrival_stable_coalesce(spark):
    stable = spark.createDataFrame([(30, "c"), (10, "a"), (20, "b")], ("v", "k")).coalesce(1)

    @pandas_udf("long")
    def first_arrival(v: pd.Series) -> int:
        return int(v.iloc[0])

    result = stable.orderBy("k").groupBy().agg(first_arrival("v").alias("fa")).collect()
    assert [tuple(r) for r in result] == [(10,)]


def test_repartition_then_order_preserved(spark):
    dft = spark.createDataFrame([(1, "a"), (2, "a"), (3, "a")], ("v", "k")).repartition(3)
    result = dft.orderBy("k").groupBy().agg(F.first("v")).collect()
    assert [tuple(r) for r in result] == [(1,)]


def test_coalesce_window_sees_input_order(spark):
    df = spark.createDataFrame([(30, "c", "x"), (10, "a", "x"), (20, "b", "x")], ("v", "k", "g")).coalesce(1)
    w_desc = Window.partitionBy("g").orderBy(F.desc("k"))
    w_none = Window.partitionBy("g")
    result = df.orderBy("k").select("v", "k", F.sum("v").over(w_desc).alias("s"), F.last("v").over(w_none).alias("l"))
    got = sorted((r.v, r.k, r.s, r.l) for r in result.collect())
    assert got == [(10, "a", 60, 10), (20, "b", 50, 10), (30, "c", 30, 10)]


def test_projected_column_order_survives_parquet_roundtrip(spark):
    df = spark.createDataFrame([(30, "c", "x"), (10, "a", "x"), (20, "b", "x")], ("v", "k", "g")).coalesce(1)
    w_desc = Window.partitionBy("g").orderBy(F.desc("k"))
    w_none = Window.partitionBy("g")
    result = df.orderBy("k").select("*", F.sum("v").over(w_desc).alias("s"), F.last("v").over(w_none).alias("l"))
    assert result.columns == ["v", "k", "g", "s", "l"]
    assert result.select("*").columns == ["v", "k", "g", "s", "l"]
    with tempfile.TemporaryDirectory() as path:
        result.write.mode("overwrite").parquet(path)
        written_columns = spark.read.parquet(path).columns
    assert written_columns == ["v", "k", "g", "s", "l"]


def test_monotonically_increasing_id_does_not_disturb_order(spark):
    df = spark.createDataFrame([(30, "c"), (10, "a"), (20, "b")], ("v", "k"))
    result = df.orderBy("k").withColumn("i", F.monotonically_increasing_id()).groupBy().agg(F.first("v")).collect()
    assert [tuple(r) for r in result] == [(10,)]


def test_spark_partition_id_does_not_disturb_order(spark):
    df = spark.createDataFrame([(30, "c"), (10, "a"), (20, "b")], ("v", "k"))
    result = df.orderBy("k").withColumn("p", F.spark_partition_id()).groupBy().agg(F.first("v")).collect()
    assert [tuple(r) for r in result] == [(10,)]


def test_pivot_first_preserves_order(spark):
    pdf = spark.createDataFrame([(30, "c", "x"), (10, "a", "x"), (20, "b", "y")], ("v", "k", "p"))
    result = pdf.orderBy("k").groupBy().pivot("p").agg(F.first("v")).collect()
    assert [tuple(r) for r in result] == [(10, 20)]


def test_first_is_not_rewritten_with_an_order_by_in_the_plan(spark):
    explain = spark.sql(
        "EXPLAIN EXTENDED SELECT first(v) FROM (SELECT * FROM VALUES (30,'c'),(10,'a'),(20,'b') AS t(v, k) ORDER BY k)"
    ).first()[0]
    assert re.search(r"first(?:_value)?\([^\n]* ORDER BY \[", explain, re.IGNORECASE) is None


@pytest.mark.skipif(
    not is_jvm_spark(),
    reason="DataFusion reduces a batch with tree/SIMD summation, so the floating-point "
    "skewness of these near-cancelling values is unstable around 0 on Sail (see the FIXME "
    "in the original doctest). JVM Spark sums sequentially and yields a stable non-zero value.",
)
def test_skewness_is_order_of_summation_dependent(spark):
    sk = (
        spark.sql(
            "SELECT skewness(v) AS sk FROM (SELECT * FROM VALUES "
            "(CAST(1e16 AS DOUBLE),'b'),(CAST(1.0 AS DOUBLE),'c'),(CAST(-1e16 AS DOUBLE),'a') "
            "AS t(v, k) ORDER BY k)"
        )
        .first()
        .sk
    )
    assert sk != 0.0
