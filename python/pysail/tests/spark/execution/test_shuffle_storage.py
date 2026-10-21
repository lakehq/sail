from __future__ import annotations

import pandas as pd
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.types import Row

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")


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


@pytest.mark.parametrize("container", ["struct", "array"])
@pytest.mark.parametrize("join_type", ["inner", "full", "left_semi"])
def test_nested_timestamp_join_preserves_hash_partitioning(spark, container, join_type):
    timestamp = F.expr("TIMESTAMP_NTZ '2024-01-01' + INTERVAL 1 SECOND * id")
    key = F.struct(timestamp.alias("ts")) if container == "struct" else F.array(timestamp)
    # Explain large inputs to choose a partitioned join without executing them.
    left = spark.range(0, 10_000_000, 1, 8).select(key.alias("k"), F.col("id").alias("lid"))
    right = spark.range(0, 10_000_000, 1, 8).select(key.alias("k"), F.col("id").alias("rid"))
    query = left.repartition(8, "k").join(right.repartition(8, "k"), "k", join_type)

    plan = query._explain_string()  # noqa: SLF001
    assert "HashJoinExec: mode=Partitioned" in plan
    expected_repartitions = 2
    assert plan.count("RepartitionExec: partitioning=Hash(") == expected_repartitions


@pytest.mark.parametrize("container", ["struct", "array"])
def test_nested_timestamp_full_join_with_storage_shuffle(spark, container):
    timestamp = F.expr("TIMESTAMP_NTZ '2024-01-01' + INTERVAL 1 SECOND * value")
    key = F.struct(timestamp.alias("ts")) if container == "struct" else F.array(timestamp)
    key = F.when(F.col("value") == -1, F.lit(None)).otherwise(key).alias("k")
    left = spark.createDataFrame(
        [(0, 0), (1, 0), (2, None), (3, -1), (4, 1), (5, 2)], "lid BIGINT, value BIGINT"
    ).select("lid", key)
    right = spark.createDataFrame([(10, 0), (11, None), (12, -1), (13, 2), (14, 3)], "rid BIGINT, value BIGINT").select(
        "rid", key
    )
    query = left.repartition(8, "k").join(right.repartition(8, "k"), "k", "full")
    timestamp = F.col("k.ts") if container == "struct" else F.col("k")[0]

    rows = (
        query.select("lid", "rid", F.col("k").isNull().alias("null_key"), timestamp.isNull().alias("null_ts"))
        .orderBy("lid", "rid")
        .collect()
    )
    assert rows == [
        Row(lid=None, rid=12, null_key=True, null_ts=True),
        Row(lid=None, rid=14, null_key=False, null_ts=False),
        Row(lid=0, rid=10, null_key=False, null_ts=False),
        Row(lid=1, rid=10, null_key=False, null_ts=False),
        Row(lid=2, rid=11, null_key=False, null_ts=True),
        Row(lid=3, rid=None, null_key=True, null_ts=True),
        Row(lid=4, rid=None, null_key=False, null_ts=False),
        Row(lid=5, rid=13, null_key=False, null_ts=False),
    ]


@pytest.mark.parametrize("ansi", ["true", "false"])
@pytest.mark.parametrize("layout", ["plain", "duplicate", "nested_duplicate", "empty"])
def test_nested_timestamp_keys_after_projection_folding(spark, ansi, layout):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    try:

        def key(value):
            if layout == "duplicate":
                return F.struct(value.alias("t"), (value + F.expr("INTERVAL 1 DAY")).alias("t"))
            if layout == "nested_duplicate":
                return F.struct(
                    value.alias("ts"),
                    F.array(F.struct(value.alias("t"), (value + F.expr("INTERVAL 1 DAY")).alias("t"))).alias("nested"),
                )
            if layout == "empty":
                return F.struct(value.alias("ts"), F.struct().alias("empty"))
            return F.struct(value.alias("t"))

        raw = spark.range(1).select(F.lit("2024-01-01").alias("s"))
        left = raw.select(
            key(F.expr("TRY_CAST(s AS TIMESTAMP_NTZ)")).alias("k"),
            F.lit(0).alias("lid"),
        )
        right = spark.range(2).select(
            key(F.expr("TRY_CAST(CASE WHEN id = 0 THEN '2024-01-01' ELSE NULL END AS TIMESTAMP_NTZ)")).alias("k"),
            F.col("id").alias("rid"),
        )
        equality = (
            left.select(F.col("k").alias("lk"), "lid")
            .crossJoin(right.select(F.col("k").alias("rk"), "rid"))
            .select("rid", (F.col("lk") == F.col("rk")).alias("equal"))
            .orderBy("rid")
        )
        assert equality.collect() == [Row(rid=0, equal=True), Row(rid=1, equal=False)]
        assert left.join(right, "k").select("lid", "rid").collect() == [Row(lid=0, rid=0)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)
