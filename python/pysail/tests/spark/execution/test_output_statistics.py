from __future__ import annotations

import time

import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")


@pytest.fixture(scope="module", params=["flight", "storage"])
def shuffle_backend(request):
    return request.param


@pytest.fixture(scope="module")
def remote(shuffle_backend, tmp_path_factory):
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE": shuffle_backend,
    }
    if shuffle_backend == "storage":
        envs.update(
            {
                "SAIL_CLUSTER__SHUFFLE_BACKEND__STORAGE__PATH": tmp_path_factory.mktemp("shuffle_stats").as_uri(),
                "SAIL_CLUSTER__SHUFFLE_BACKEND__STORAGE__MAX_FILE_SIZE": "1024",
                "SAIL_CLUSTER__SHUFFLE_BACKEND__STORAGE__COMPRESSION": "lz4",
            }
        )
    with spark_connect_server(envs=envs) as server:
        yield server.remote


def _system_rows(spark, query):
    # System tables use uint64, which PySpark collect() does not support.
    table, _ = spark.sql(query)._to_table()  # noqa: SLF001
    return table.to_pylist()


def _wait_for_output(predicate):
    deadline = time.monotonic() + 15
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() >= deadline:
            pytest.fail("task output statistics were not published")
        time.sleep(0.05)


@pytest.mark.timeout(60)
def test_output_statistics_and_stage_plans(spark, shuffle_backend):
    rows, producers, channels = 10, 4, 8
    result = spark.range(rows, numPartitions=producers).repartition(channels).collect()
    assert sorted(row.id for row in result) == list(range(rows))

    # Select in Python so the observation query cannot match its own plan text.
    def find_stage():
        stages = _system_rows(spark, f"SELECT * FROM system.execution.stages WHERE session_id = '{spark.session_id}'")
        candidates = [
            stage
            for stage in stages
            if "RangeExec:" in stage["plan"] and stage["distribution"] == f"RoundRobinRow(channels={channels})"
        ]
        return max(candidates, key=lambda stage: stage["job_id"]) if candidates else None

    producer = _wait_for_output(find_stage)

    def read_complete(stage):
        tasks = _system_rows(
            spark,
            f"""
            SELECT * FROM system.execution.tasks
            WHERE session_id = '{spark.session_id}' AND job_id = {stage["job_id"]} AND stage = {stage["stage"]}
            """,
        )
        if len(tasks) == stage["partitions"] and all(task["output_statistics"] is not None for task in tasks):
            return tasks
        return None

    tasks = _wait_for_output(lambda: read_complete(producer))
    assert len(tasks) == producers
    assert all(task["status"] == "SUCCEEDED" and task["output_statistics"]["complete"] for task in tasks)
    assert all(len(task["output_statistics"]["partitions"]) == channels for task in tasks)
    partitions = [part for task in tasks for part in task["output_statistics"]["partitions"]]
    assert sum(part["num_rows"] for part in partitions) == rows
    assert any(part["num_rows"] == 0 for part in partitions)
    assert sum(part["batch_bytes"] for part in partitions) > 0
    assert all(part["stored_bytes"] is None for part in partitions)

    if shuffle_backend == "storage":
        stages = _system_rows(
            spark,
            f"""
            SELECT * FROM system.execution.stages
            WHERE session_id = '{spark.session_id}' AND job_id = {producer["job_id"]} AND mode = 'Blocking'
            """,
        )
        assert stages
        for stage in stages:
            tasks = _wait_for_output(lambda stage=stage: read_complete(stage))
            assert all(task["output_statistics"]["complete"] for task in tasks)
            partitions = [part for task in tasks for part in task["output_statistics"]["partitions"]]
            assert sum(part["num_rows"] for part in partitions) == rows
            assert all(part["stored_bytes"] is not None for part in partitions)
            assert sum(part["stored_bytes"] for part in partitions) > 0
            assert all(part["stored_bytes"] == 0 for part in partitions if part["num_rows"] == 0)
