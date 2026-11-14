from __future__ import annotations

import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.errors import PySparkException

from pysail.testing.spark.session import spark_connect_server

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail execution modes only")

ROWS = 1_000_000
# The outcome on a build without the fix depends on timing, so each case is repeated.
# The unit tests of the driver and the task streams pin the behavior deterministically.
RUNS = 10


@pytest.fixture(scope="module", params=["local", "local-cluster", "single-worker", "storage-shuffle"])
def remote(request, tmp_path_factory):
    envs = {"SAIL_MODE": "local" if request.param == "local" else "local-cluster"}
    if request.param == "single-worker":
        # All the tasks of a job share one worker, so a task reads the output
        # of a failed task without a network hop.
        envs["SAIL_CLUSTER__WORKER_INITIAL_COUNT"] = "1"
        envs["SAIL_CLUSTER__WORKER_MAX_COUNT"] = "1"
        envs["SAIL_CLUSTER__WORKER_TASK_SLOTS"] = "32"
    if request.param == "storage-shuffle":
        shuffle_path = tmp_path_factory.mktemp("shuffle_storage")
        envs["SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE"] = "storage"
        envs["SAIL_CLUSTER__SHUFFLE_BACKEND__STORAGE__PATH"] = shuffle_path.as_uri()
    with spark_connect_server(envs=envs) as server:
        yield server.remote


def upstream_failure(spark):
    """A query whose first stage fails on the last row after writing most of its output."""
    return (
        spark.range(0, ROWS, 1, 4)
        .selectExpr(f"IF(id = {ROWS - 1}, raise_error('upstream failure'), id) AS k")
        .repartition(4)
        .agg(F.count("k").alias("n"))
    )


@pytest.mark.timeout(60)
def test_final_stage_failure_reports_task_error(spark):
    # The other partitions of the final stage deliver rows before the failure.
    query = (
        spark.range(64, numPartitions=4)
        .repartition(4, "id")
        .selectExpr("IF(id = 0, raise_error('final stage failure'), id) AS value")
    )
    for _ in range(RUNS):
        with pytest.raises(PySparkException, match="final stage failure"):
            query.collect()
    assert spark.range(64, numPartitions=4).count() == 64  # noqa: PLR2004


@pytest.mark.timeout(60)
def test_upstream_failure_reports_task_error(spark):
    # The final stage raises a different error if its input ends early.
    query = upstream_failure(spark).selectExpr(f"IF(n < {ROWS}, raise_error('truncated input'), n) AS n")
    for _ in range(RUNS):
        with pytest.raises(PySparkException, match="upstream failure"):
            query.collect()


@pytest.mark.timeout(60)
def test_upstream_failure_delivers_no_rows(spark):
    # Any row would be an aggregate over the truncated output of the failed stage.
    query = upstream_failure(spark)
    for _ in range(RUNS):
        rows = []
        with pytest.raises(PySparkException, match="upstream failure"):
            rows.extend(query.toLocalIterator())
        assert rows == []
