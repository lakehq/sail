"""A task that fails after the streams of an earlier stage of its region are cleaned up
retries the region, and the retry fails the query with the error of the task, rather than
waiting for the streams of the cleaned stage forever."""

import pytest
from pyspark.errors import PySparkException

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail cluster mode")


@pytest.fixture(scope="module", params=["true", "false"], ids=["shared-ctes", "no-shared-ctes"])
def remote(request):
    envs = {"SAIL_MODE": "local-cluster", "SAIL_OPTIMIZER__SHARE_CTE_DEFINITIONS": request.param}
    with spark_connect_server(envs=envs) as server:
        yield server.remote


GROUPS = (
    "(SELECT k, sum(v) AS v FROM (SELECT id % 5000 AS k, id * 7 % 1009 AS v FROM range(0, 20000, 1, 4)) t GROUP BY k)"
)


@pytest.mark.timeout(60)
def test_failing_task_after_stage_clean_up_fails_the_query(spark):
    query = (
        "SELECT count(*) AS n, sum(v) AS s FROM "  # noqa: S608
        f"(SELECT k, 1000000 DIV (k - 3) AS v FROM {GROUPS} a) c "
        f"WHERE v >= (SELECT avg(1000000 DIV (k - 3)) FROM {GROUPS} b)"
    )
    # A retried task attempt can replace the error with a conflict between attempts.
    # TODO: report the error of the failed task in cluster mode.
    with pytest.raises(PySparkException, match=r"(?i)division by zero|different attempt"):
        spark.sql(query).collect()
