"""A task that fails after it has sent some of its output is retried, and the query either
returns the full result or fails with the error of the task. Readers of the failed attempt
must not finish on its partial output, and its partial output must not reach the client.

With the Celeborn shuffle backend, the reader must skip the output that a failed task
attempt pushed before it failed.

A task that fails after the streams of an earlier stage of its region are cleaned up
retries the region, and the retry fails the query with the error of the task, rather than
waiting for the streams of the cleaned stage forever."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.errors import PySparkException

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark

if TYPE_CHECKING:
    from pysail.testing._dev.containers.celeborn import MasterService, WorkerService

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail execution modes")


ROWS = 200_000
# The failing row is in the first partition of `range(0, ROWS, 1, 2)`, after several batches.
FAILING_ROW = 60_000
TOTAL = ROWS * (ROWS - 1) // 2
ERROR = "injected transient failure"
RUNS = 5
# The server modes for task retries.
MODES = ["local", "local-cluster", "storage"]
# The server modes for retries after stage clean-up, with and without shared CTEs.
CLEANUP_MODES = {"shared-ctes": "true", "no-shared-ctes": "false"}


@pytest.fixture(scope="module")
def mode(request):
    return request.param


@pytest.fixture(scope="module")
def remote(request, mode, tmp_path_factory):
    if mode == "celeborn":
        envs = celeborn_envs(request.getfixturevalue("celeborn_master"), request.getfixturevalue("celeborn_workers"))
    elif mode in CLEANUP_MODES:
        envs = {"SAIL_MODE": "local-cluster", "SAIL_OPTIMIZER__SHARE_CTE_DEFINITIONS": CLEANUP_MODES[mode]}
    else:
        envs = {"SAIL_MODE": "local" if mode == "local" else "local-cluster"}
        if mode == "storage":
            envs["SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE"] = "storage"
            envs["SAIL_CLUSTER__SHUFFLE_BACKEND__STORAGE__PATH"] = tmp_path_factory.mktemp("shuffle").as_uri()
    with spark_connect_server(envs=envs) as server:
        yield server.remote


def celeborn_envs(
    celeborn_master: MasterService,
    celeborn_workers: dict[str, WorkerService],
) -> dict[str, str]:
    endpoint_overrides = "[{}]".format(
        ", ".join(
            f'{{ internal = "{hostname}:{port}", external = "{worker.host}:{mapped_port}" }}'
            for hostname, worker in celeborn_workers.items()
            for port, mapped_port in [
                (12000, worker.rpc_port),
                (12001, worker.push_port),
                (12002, worker.fetch_port),
                (12003, worker.replicate_port),
            ]
        )
    )
    return {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE": "celeborn",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__MASTER_ENDPOINTS": (
            f'["{celeborn_master.host}:{celeborn_master.port}"]'
        ),
        "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__ENDPOINT_OVERRIDES": endpoint_overrides,
    }


def fail_once(marker):
    """Returns a UDF that raises at `FAILING_ROW` the first time only (across task attempts)."""

    def f(x):
        if x == FAILING_ROW:
            try:
                fd = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                return x
            os.close(fd)
            raise RuntimeError(ERROR)
        return x

    return F.udf(f, "bigint")


def source(spark, marker):
    return spark.range(0, ROWS, 1, 2).select(fail_once(marker)("id").alias("v"))


def shapes(spark, marker):
    other = spark.range(0, ROWS, 1, 3).withColumnRenamed("id", "v").repartition(3, "v")
    return {
        # The failing stage feeds a stage that streams rows to the client.
        "upstream-rows": source(spark, marker).repartition(3),
        # The failing stage feeds an aggregation.
        "upstream-aggregate": source(spark, marker).repartition(3).agg(F.count("v"), F.sum("v")),
        # The failing stage is one side of a shuffle join.
        "join-side": source(spark, marker).repartition(3, "v").join(other, "v").agg(F.count("v"), F.sum("v")),
        # The failing stage sends rows to the client.
        "final-stage": source(spark, marker),
    }


def collect(df):
    """Reads the result incrementally, so that rows sent before an error are observed."""
    rows = []
    try:
        rows.extend(tuple(row) for row in df.toLocalIterator())
    except PySparkException as e:
        return rows, str(e)
    return rows, None


def expected(shape):
    if shape in ("upstream-rows", "final-stage"):
        return [(i,) for i in range(ROWS)]
    return [(ROWS, TOTAL)]


@pytest.mark.timeout(300)
@pytest.mark.parametrize("shape", ["upstream-rows", "upstream-aggregate", "join-side", "final-stage"])
@pytest.mark.parametrize("mode", MODES, indirect=True)
def test_task_failure_after_partial_output(spark, mode, shape, tmp_path):
    for run in range(RUNS):
        marker = str(tmp_path / f"marker-{run}")
        rows, error = collect(shapes(spark, marker)[shape])
        assert os.path.exists(marker), "the failure was not injected"
        if mode == "local":
            # Tasks are not retried in local mode.
            assert error is not None, rows[:3]
            assert ERROR in error, error
        elif error is None:
            assert sorted(rows) == expected(shape)
        else:
            # The retry can still fail when the failed attempt has already sent rows to the client.
            assert shape in ("upstream-rows", "final-stage"), error
            assert ERROR in error, error


@pytest.mark.timeout(300)
@pytest.mark.parametrize("mode", MODES, indirect=True)
def test_deterministic_task_failure_reports_the_task_error(spark):
    query = """
        WITH c AS (SELECT id, 10 / (id - 2) AS r FROM range(5) GROUP BY id)
        SELECT a.r FROM c a JOIN c b ON a.id = b.id
    """
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        for _ in range(RUNS * 3):
            with pytest.raises(PySparkException, match=r"(?i)division by zero"):
                spark.sql(query).collect()
    finally:
        spark.conf.unset("spark.sql.ansi.enabled")


# TODO: The Celeborn shuffle reader reads the batches that a failed task attempt pushed
#   before it failed, so a retried task can return duplicate rows. The reader should keep
#   only the batches of the attempt that is committed for each map task.
@pytest.mark.integration
@pytest.mark.spark(skip_if_jvm=True, reason="Sail local-cluster mode only")
@pytest.mark.skip(reason="the Celeborn shuffle reader does not skip the output of failed task attempts")
@pytest.mark.timeout(900)
@pytest.mark.parametrize("shape", ["upstream-rows", "upstream-aggregate", "join-side", "final-stage"])
@pytest.mark.parametrize("mode", ["celeborn"], indirect=True)
def test_task_failure_after_partial_output_with_celeborn(spark, shape, tmp_path):
    for run in range(RUNS):
        marker = str(tmp_path / f"marker-{run}")
        rows, error = collect(shapes(spark, marker)[shape])
        assert os.path.exists(marker), "the failure was not injected"
        if error is None:
            assert sorted(rows) == expected(shape)
        else:
            assert shape in ("upstream-rows", "final-stage"), error
            assert ERROR in error, error


GROUPS = (
    "(SELECT k, sum(v) AS v FROM (SELECT id % 5000 AS k, id * 7 % 1009 AS v FROM range(0, 20000, 1, 4)) t GROUP BY k)"
)


@pytest.mark.skipif(is_jvm_spark(), reason="Sail cluster mode")
@pytest.mark.timeout(60)
@pytest.mark.parametrize("mode", list(CLEANUP_MODES), indirect=True)
def test_failing_task_after_stage_clean_up_fails_the_query(spark):
    query = (
        "SELECT count(*) AS n, sum(v) AS s FROM "  # noqa: S608
        f"(SELECT k, 1000000 DIV (k - 3) AS v FROM {GROUPS} a) c "
        f"WHERE v >= (SELECT avg(1000000 DIV (k - 3)) FROM {GROUPS} b)"
    )
    with pytest.raises(PySparkException, match=r"(?i)division by zero"):
        spark.sql(query).collect()
