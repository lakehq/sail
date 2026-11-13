from __future__ import annotations

import pandas as pd
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pandas.testing import assert_frame_equal
from pyspark.errors import PySparkException
from pyspark.sql.types import Row

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.plan import normalize_plan_text

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail local-cluster mode only")


@pytest.fixture(scope="module")
def remote(tmp_path_factory):
    shuffle_path = tmp_path_factory.mktemp("shuffle_storage")
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE": "storage",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__STORAGE__PATH": shuffle_path.as_uri(),
        "SAIL_CLUSTER__SHUFFLE_BACKEND__STORAGE__MAX_FILE_SIZE": "1024",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__STORAGE__COMPRESSION": "lz4",
    }
    with spark_connect_server(envs=envs) as server:
        yield server.remote


@pytest.mark.yamlsnapshot(group="plan")
def test_query_execution_with_storage_shuffle(spark, snapshot):
    left = spark.range(0, 64, 1, 4).select(
        F.col("id").alias("k"),
        (F.col("id") * 2).alias("v1"),
    )
    right = spark.range(0, 64, 1, 4).select(
        F.col("id").alias("k"),
        (F.col("id") + 1).alias("v2"),
    )

    df = (
        left.repartition(8, "k")
        .join(right.repartition(8, "k"), "k")
        .withColumn("g", F.col("k") % 4)
        .groupBy("g")
        .agg(
            F.count("*").alias("count"),
            F.sum("v1").alias("s1"),
            F.sum("v2").alias("s2"),
        )
        .orderBy("g")
    )

    plan = normalize_plan_text(df._explain_string(mode="codegen"))  # noqa: SLF001
    assert plan == snapshot

    actual = df.toPandas()
    expected = pd.DataFrame(
        {
            "g": [0, 1, 2, 3],
            "count": [16, 16, 16, 16],
            "s1": [960, 992, 1024, 1056],
            "s2": [496, 512, 528, 544],
        }
    ).astype(
        {
            "g": "int64",
            "count": "int64",
            "s1": "int64",
            "s2": "int64",
        }
    )
    assert_frame_equal(actual, expected)


def test_repartition_collect_with_storage_shuffle(spark):
    rows = (
        spark.createDataFrame([Row(id=i, group=i % 3) for i in range(30)])
        .repartition(6, "group")
        .groupBy("group")
        .count()
        .orderBy("group")
        .collect()
    )

    assert rows == [
        Row(group=0, count=10),
        Row(group=1, count=10),
        Row(group=2, count=10),
    ]


# A CTE body that is shared, and that a stage of the same task region reads with a failing
# expression, so that the region is retried after the input of the body is consumed.
RETRIED_CTE_QUERY = (
    "WITH c AS (SELECT k, max(v) AS v, 100 DIV (k - 3) AS f "
    "FROM (SELECT id AS k, id AS v FROM range(0, 2000, 1, 3)) GROUP BY k) "
    "SELECT (SELECT sum(CASE WHEN k <> 3 THEN f END) FROM c), (SELECT count(*) FROM c)"
)


def check_retried_cte_query_fails(spark):
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        for _ in range(24):
            with pytest.raises(PySparkException, match=r"(?i)division by zero"):
                spark.sql(RETRIED_CTE_QUERY).collect()
    finally:
        spark.conf.unset("spark.sql.ansi.enabled")


def test_retried_region_reads_the_output_of_a_blocking_stage(spark):
    check_retried_cte_query_fails(spark)
