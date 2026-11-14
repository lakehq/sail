"""Task failures retain their original errors across execution modes."""

import pytest
from pyspark.errors import PySparkException

from pysail.testing.spark.session import spark_connect_server

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail execution modes")

RUNS = 5


@pytest.fixture(scope="module", params=["local", "local-cluster", "storage"])
def mode(request):
    return request.param


@pytest.fixture(scope="module")
def remote(mode, tmp_path_factory):
    envs = {"SAIL_MODE": "local" if mode == "local" else "local-cluster"}
    if mode == "storage":
        envs["SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE"] = "storage"
        envs["SAIL_CLUSTER__SHUFFLE_BACKEND__STORAGE__PATH"] = tmp_path_factory.mktemp("shuffle").as_uri()
    with spark_connect_server(envs=envs) as server:
        yield server.remote


@pytest.mark.timeout(300)
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
