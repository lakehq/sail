import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.fixture(scope="module")
def remote():
    with spark_connect_server(envs={"SAIL_MODE": "local-cluster"}) as server:
        yield server.remote


pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail cluster mode")


def test_shared_cte_is_read_by_several_stages(spark):
    query = """
        WITH c AS (SELECT id % 7 AS k, count(*) AS n FROM range(100000) GROUP BY 1)
        SELECT sum(a.n), sum(b.n), (SELECT max(n) FROM c) FROM c a JOIN c b ON a.k = b.k
    """
    assert spark.sql(query).collect() == [(100000, 100000, 14286)]
    assert "SharedCteExec" in spark.sql(f"EXPLAIN {query}").collect()[0][0]


def test_shared_cte_is_read_by_union_branches(spark):
    query = """
        WITH c AS (SELECT id % 7 AS k, id AS v FROM range(100000))
        SELECT count(*), sum(v) FROM (SELECT * FROM c UNION ALL SELECT * FROM c)
    """
    assert spark.sql(query).collect() == [(200000, 9999900000)]


@pytest.mark.parametrize("depth", [8, 24])
def test_nested_shared_ctes(spark, depth):
    ctes = ["c0 AS (SELECT id FROM range(10))"]
    ctes.extend(
        f"c{i} AS (SELECT a.id FROM c{i - 1} a JOIN c{i - 1} b ON a.id = b.id)" for i in range(1, depth + 1)
    )
    query = f"WITH {', '.join(ctes)} SELECT count(*) FROM c{depth}"
    assert spark.sql(query).collect() == [(10,)]
