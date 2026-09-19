import pytest
from pyspark.sql import Row

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")


@pytest.fixture(scope="module", params=[None, "none", "lz4", "zstd"], ids=["default", "none", "lz4", "zstd"])
def remote(request):
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__TASK_STREAM_BUFFER": "1",
        "SAIL_EXECUTION__BATCH_SIZE": "256",
    }
    if request.param is not None:
        envs["SAIL_CLUSTER__SHUFFLE_BACKEND__FLIGHT__COMPRESSION"] = request.param
    with spark_connect_server(envs=envs) as server:
        yield server.remote


@pytest.mark.timeout(15)
@pytest.mark.parametrize("partitions", [2, 4])
@pytest.mark.parametrize("join_type", ["left_semi", "left_anti"])
def test_shuffle_finishes_when_join_leaves_probe_partitions_unread(spark, partitions, join_type):
    # Only one build partition contains a row. The other join partitions finish
    # without opening their probe inputs. Multiple range batches overflow the
    # one-batch shuffle buffers, including those belonging to unread channels.
    build = spark.range(1, numPartitions=1).repartition(partitions, "id")
    probe = spark.range(10_000, numPartitions=1).repartition(partitions, "id")

    rows = build.join(probe, "id", join_type).collect()

    assert rows == ([Row(id=0)] if join_type == "left_semi" else [])


@pytest.mark.timeout(30)
def test_shuffle_preserves_rows_across_multiple_batches(spark):
    rows = [Row(id=i, payload="shuffle" * 100 if i % 3 else None) for i in range(2048)]
    actual = spark.createDataFrame(rows).repartition(8, "id").orderBy("id").collect()

    assert actual == rows
