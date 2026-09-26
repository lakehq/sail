import json
import time

import pytest
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")


@pytest.fixture(scope="module")
def spill_path(tmp_path_factory):
    return tmp_path_factory.mktemp("broadcast_spill")


@pytest.fixture(scope="module", params=["flight", "storage"])
def remote(request, tmp_path_factory, spill_path):
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_RUNTIME__TEMPORARY_FILES__PATHS": json.dumps([str(spill_path)]),
        "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "2",
        "SAIL_CLUSTER__WORKER_MAX_COUNT": "2",
        "SAIL_CLUSTER__TASK_STREAM_BUFFER": "1",
        "SAIL_EXECUTION__BATCH_SIZE": "64",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE": request.param,
    }
    if request.param == "storage":
        envs["SAIL_CLUSTER__SHUFFLE_BACKEND__STORAGE__PATH"] = tmp_path_factory.mktemp("broadcast").as_uri()
    with spark_connect_server(envs=envs) as server:
        yield server.remote


@pytest.mark.timeout(30)
def test_broadcast_join_replays_spilled_batches(spark, spill_path):
    build = spark.range(512, numPartitions=2).selectExpr("id", "concat('value-', id) AS value")
    probe = spark.range(8192, numPartitions=16).selectExpr("id AS probe", "id % 512 AS id")
    rows = probe.join(F.broadcast(build), "id").collect()
    assert sorted((row.probe, row.id, row.value) for row in rows) == [
        (i, i % 512, f"value-{i % 512}") for i in range(8192)
    ]
    # Worker cleanup is asynchronous. Replay files must disappear once the job
    # has finished, including the copies cached on consumer workers.
    deadline = time.monotonic() + 5
    while any(path.is_file() for path in spill_path.rglob("*")):
        assert time.monotonic() < deadline, "broadcast spill files were not cleaned up"
        time.sleep(0.01)


@pytest.mark.timeout(30)
def test_broadcast_scalar_subquery_replays_for_tasks_and_isolates_jobs(spark):
    # Scalar inputs use pipelined Flight streams and blocking storage streams.
    # A second job must get its own cached value after the first job is cleaned up.
    for end in [10, 20]:
        spark.range(end).createOrReplaceTempView("broadcast_scalar_values")
        rows = spark.sql(
            "SELECT id, (SELECT sum(id) FROM broadcast_scalar_values) AS total FROM range(0, 128, 1, 16)"
        ).collect()
        assert sorted((row.id, row.total) for row in rows) == [(i, end * (end - 1) // 2) for i in range(128)]


@pytest.mark.timeout(30)
def test_broadcast_with_unread_consumers_finishes(spark):
    build = spark.range(4096, numPartitions=2)
    probe = spark.range(4096, numPartitions=16)
    assert probe.join(F.broadcast(build), "id").limit(1).count() == 1
    assert spark.range(10, numPartitions=2).count() == 10  # noqa: PLR2004
