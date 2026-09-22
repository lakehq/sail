import pytest
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")


@pytest.fixture(scope="module")
def remote():
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_RUNTIME__TEMPORARY_FILES__MAX_SIZE": "0",
        "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "2",
        "SAIL_CLUSTER__WORKER_MAX_COUNT": "2",
        "SAIL_CLUSTER__TASK_STREAM_BUFFER": "1",
        "SAIL_CLUSTER__TASK_MAX_ATTEMPTS": "1",
    }
    with spark_connect_server(envs=envs) as server:
        yield server.remote


@pytest.mark.timeout(30)
@pytest.mark.parametrize("distribution", ["forward", "shuffle", "merge"])
def test_non_replayable_streams_do_not_require_disk(spark, distribution):
    # Range produces 1024-row batches. Each stream exceeds the replay buffer,
    # but these streams have a single consumer and must not retain history.
    end = 100_000
    data = spark.range(end, numPartitions=4)
    if distribution == "shuffle":
        data = data.repartition(4, "id")
    elif distribution == "merge":
        data = data.coalesce(1)
    assert sorted(row.id for row in data.collect()) == list(range(end))


@pytest.mark.timeout(30)
def test_broadcast_with_one_consumer_does_not_require_disk(spark):
    end = 2048
    build = spark.range(end, numPartitions=1)
    probe = spark.range(end * 4, numPartitions=1)
    rows = probe.join(F.broadcast(build), "id").collect()
    assert sorted(row.id for row in rows) == list(range(end))
