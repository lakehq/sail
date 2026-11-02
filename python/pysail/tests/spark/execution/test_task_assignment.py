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


@pytest.mark.timeout(30)
def test_partition_regions_run_with_partial_worker_capacity(spark):
    # Only one region fits when worker_task_slots=1. The remaining partitions
    # must make progress in later scheduling snapshots.
    rows = spark.range(80, numPartitions=8).selectExpr("id", "id * 2 AS doubled").collect()
    assert sorted((row.id, row.doubled) for row in rows) == [(i, i * 2) for i in range(80)]


@pytest.mark.timeout(30)
def test_partition_regions_preserve_union_input_partition_indices(spark):
    left = spark.range(0, 40, numPartitions=4)
    right = spark.range(40, 100, numPartitions=6)
    rows = left.union(right).selectExpr("id + 1 AS value").collect()
    assert sorted(row.value for row in rows) == list(range(1, 101))


@pytest.mark.timeout(30)
def test_replay_stages_run_partition_pairs_with_limited_slots(spark):
    query = """
        SELECT /*+ SHUFFLE_REPLICATE_NL(l) */ l.id AS a, r.id AS b, spark_partition_id() AS pid
        FROM range(0, 6, 1, 3) l CROSS JOIN range(0, 4, 1, 2) r
    """
    rows = spark.sql(query).collect()
    assert sorted((r.a, r.b) for r in rows) == [(a, b) for a in range(6) for b in range(4)]
    assert len({r.pid for r in rows}) == 6  # noqa: PLR2004
    left = spark.sql(query).selectExpr("a AS id")
    right = spark.range(40, 44, numPartitions=4)
    assert sorted(r.id for r in left.union(right).collect()) == sorted(list(range(6)) * 4 + list(range(40, 44)))
    # Do not materialize the 100-million-row Cartesian result before LIMIT.
    limited = spark.sql("""
        SELECT /*+ SHUFFLE_REPLICATE_NL(l) */ l.id AS a, r.id AS b
        FROM range(0, 10000, 1, 3) l CROSS JOIN range(0, 10000, 1, 2) r
    """).limit(1)
    assert len(limited.collect()) == 1


@pytest.mark.timeout(30)
def test_range_replay_stages_keep_producer_local_indices(spark):
    source = spark.range(15, numPartitions=5)
    result = source.repartitionByRange(7, "id")
    assert sorted(r.id for r in result.union(spark.range(15, 20, numPartitions=2)).collect()) == list(range(20))


@pytest.mark.timeout(30)
def test_task_batch_failure_leaves_session_usable(spark, worker_task_slots):
    if worker_task_slots < 4:  # noqa: PLR2004
        pytest.skip("the shuffle region needs four slots")
    query = (
        spark.range(64, numPartitions=4)
        .repartition(4, "id")
        .selectExpr("IF(id = 0, raise_error('task batch failure'), id) AS value")
    )
    # A concurrent partition can publish output before the driver observes the
    # failure. In that case the existing retry guard reports the region failure.
    with pytest.raises(
        PySparkException,
        match=r"task batch failure|a different attempt has already produced job output",
    ):
        query.collect()
    assert sorted(row.id for row in spark.range(64, numPartitions=4).collect()) == list(range(64))
