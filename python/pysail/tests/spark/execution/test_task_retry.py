from pathlib import Path

import pytest
from pyspark.errors import PySparkException

from pysail.testing.spark.session import spark_connect_server


@pytest.fixture(scope="module")
def remote():
    with spark_connect_server(
        envs={
            "SAIL_MODE": "local-cluster",
            "SAIL_EXECUTION__DEFAULT_PARALLELISM": "4",
            "SAIL_CLUSTER__TASK_MAX_ATTEMPTS": "4",
        }
    ) as server:
        yield server.remote


@pytest.mark.timeout(60)
def test_scalar_cardinality_failure_preserves_original_error(spark):
    # Nonfailing keys produce rows in other output partitions. Retrying the
    # failed region must not mask the semantic error with an attempt conflict.
    with pytest.raises(
        PySparkException,
        match="SCALAR_SUBQUERY_TOO_MANY_ROWS: scalar subquery returned more than one row",
    ):
        spark.sql(
            """
            SELECT o.id, (SELECT i.v FROM VALUES (1L, 10), (2L, 20), (2L, 21) i(k, v)
                          WHERE i.k = o.id) AS v
            FROM range(0, 64, 1, 4) o
            """
        ).collect()
    assert sorted(row.id for row in spark.range(64, numPartitions=4).collect()) == list(range(64))


@pytest.mark.timeout(60)
@pytest.mark.parametrize("shuffle", [False, True], ids=["output-stage", "shuffle-stage"])
def test_transient_failure_retries_without_duplicate_rows(spark, tmp_path, shuffle):
    # A shared, atomic fault budget survives Python worker recreation. The only
    # input partition fails before emitting, and its next attempt must succeed.
    failure = str(tmp_path / "failed")
    success = str(tmp_path / "succeeded")

    def fail_once(batches):
        try:
            with Path(failure).open("x"):
                pass
        except FileExistsError:
            with Path(success).open("x"):
                pass
        else:
            msg = "injected transient task failure"
            raise OSError(msg)
        yield from batches

    query = spark.range(64, numPartitions=1).mapInPandas(fail_once, "id long")
    if shuffle:
        query = query.repartition(4, "id")
    rows = query.collect()
    assert Path(failure).is_file(), "the transient failure must actually fire"
    assert Path(success).is_file(), "a later task attempt must execute successfully"
    assert sorted(row.id for row in rows) == list(range(64))
