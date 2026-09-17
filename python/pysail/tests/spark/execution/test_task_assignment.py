import pytest
from pyspark.errors import PySparkException

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")


@pytest.fixture(scope="module", params=[1, 32])
def worker_task_slots(request):
    return request.param


@pytest.fixture(scope="module")
def remote(worker_task_slots):
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": "1",
        "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "1",
        "SAIL_CLUSTER__WORKER_MAX_COUNT": "1",
        "SAIL_CLUSTER__WORKER_TASK_SLOTS": str(worker_task_slots),
        # An impossible region must fail well before this timeout, including retries.
        "SAIL_CLUSTER__TASK_LAUNCH_TIMEOUT_SECS": "120",
    }
    with spark_connect_server(envs=envs) as server:
        yield server.remote


@pytest.mark.timeout(30)
def test_task_region_capacity(spark, worker_task_slots):
    query = spark.range(20, numPartitions=4).repartition(4, "id")
    if worker_task_slots == 1:
        with pytest.raises(
            PySparkException,
            match=r"task region requires \d+ worker task slots, but the configured maximum is 1",
        ):
            query.collect()
        # Failure should leave the session usable for regions that fit.
        assert spark.range(20, numPartitions=1).count() == 20  # noqa: PLR2004
    else:
        assert sorted(row.id for row in query.collect()) == list(range(20))
