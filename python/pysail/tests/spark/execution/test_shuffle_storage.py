from __future__ import annotations

import pandas as pd
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.types import Row

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")


@pytest.fixture(scope="module")
def storage_shuffle_spark(tmp_path_factory):
    shuffle_path = tmp_path_factory.mktemp("shuffle_storage")
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__SHUFFLE_SERVICE__TYPE": "storage",
        "SAIL_CLUSTER__SHUFFLE_SERVICE__STORAGE__PATH": shuffle_path.as_uri(),
        "SAIL_CLUSTER__SHUFFLE_SERVICE__STORAGE__MAX_FILE_SIZE": "1024",
        "SAIL_CLUSTER__SHUFFLE_SERVICE__STORAGE__COMPRESSION": "lz4",
    }
    with spark_connect_server(envs=envs) as server, spark_session_factory(server.remote) as sessions:
        yield sessions.create()


@pytest.mark.yamlsnapshot(group="plan")
def test_query_execution_with_storage_shuffle(storage_shuffle_spark, snapshot):
    left = storage_shuffle_spark.range(0, 64, 1, 4).select(
        F.col("id").alias("k"),
        (F.col("id") * 2).alias("v1"),
    )
    right = storage_shuffle_spark.range(0, 64, 1, 4).select(
        F.col("id").alias("k"),
        (F.col("id") + 1).alias("v2"),
    )

    result = (
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

    plan = normalize_plan_text(result._explain_string())  # noqa: SLF001
    assert plan == snapshot

    actual = result.toPandas()
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


def test_repartition_collect_with_storage_shuffle(storage_shuffle_spark):
    rows = (
        storage_shuffle_spark.createDataFrame([Row(id=i, group=i % 3) for i in range(30)])
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
