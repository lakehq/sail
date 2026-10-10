import pytest
from pyspark.sql import Row
from pyspark.sql.types import LongType, StructField, StructType

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail physical optimizer configuration")


@pytest.fixture(scope="module")
def remote():
    with spark_connect_server(
        envs={"SAIL_MODE": "local-cluster", "SAIL_OPTIMIZER__PREFER_HASH_JOIN": "false"}
    ) as server:
        yield server.remote


@pytest.mark.timeout(30)
@pytest.mark.yamlsnapshot(group="plan")
def test_explain_prepares_sort_merge_join_inputs(spark, snapshot):
    left = spark.range(0, 4, 1, 4).withColumnRenamed("id", "left_key")
    right = spark.range(0, 4, 2, 4).withColumnRenamed("id", "right_key")
    query = left.join(right, left.left_key == right.right_key, "left").orderBy("left_key")

    assert query.collect() == [
        Row(left_key=0, right_key=0),
        Row(left_key=1, right_key=None),
        Row(left_key=2, right_key=2),
        Row(left_key=3, right_key=None),
    ]
    assert query.schema == StructType(
        [StructField("left_key", LongType(), False), StructField("right_key", LongType(), True)]
    )
    assert normalize_plan_text(query._explain_string()) == snapshot  # noqa: SLF001
