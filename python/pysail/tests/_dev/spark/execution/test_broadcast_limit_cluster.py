import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.fixture(scope="module")
def remote():
    with spark_connect_server(envs={"SAIL_MODE": "local-cluster"}) as server:
        yield server.remote


pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail cluster mode")


def test_limit_read_by_every_task_of_a_join_keeps_the_same_rows(spark):
    # Each probe partition contains every build key, so compare the selected key sets.
    query = (
        "SELECT b.probe_partition, a.id "
        "FROM (SELECT id FROM range(0, 200, 1, 4) LIMIT 50) a "
        "JOIN (SELECT id % 200 AS id, CAST(floor(id / 200) AS INT) AS probe_partition "
        "FROM range(0, 800, 1, 4)) b ON a.id = b.id"
    )
    for _ in range(20):
        selected = [set() for _ in range(4)]
        for row in spark.sql(query).collect():
            selected[row.probe_partition].add(row.id)
        assert len(selected[0]) == 50  # noqa: PLR2004
        assert all(keys == selected[0] for keys in selected)
