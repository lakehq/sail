import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.fixture(scope="module")
def remote():
    with spark_connect_server(envs={"SAIL_MODE": "local-cluster"}) as server:
        yield server.remote


pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail cluster mode")


def test_limit_read_by_every_task_of_a_join_keeps_the_same_rows(spark):
    # Every row of the limited side has a match, so the join returns as many rows as the limit.
    query = (
        "SELECT count(*) FROM (SELECT id, id * 2 AS x FROM range(0, 200, 1, 4) LIMIT 50) a "
        "JOIN (SELECT id, id * 3 AS y FROM range(0, 200, 1, 4)) b ON a.id = b.id"
    )
    for _ in range(20):
        assert spark.sql(query).collect()[0][0] == 50  # noqa: PLR2004
