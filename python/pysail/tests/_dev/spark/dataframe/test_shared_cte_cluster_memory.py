import pytest
from pyspark.errors import PySparkException

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark, shares_deterministic_ctes


@pytest.fixture(scope="module")
def remote():
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__TASK_STREAM_BUFFER": "2",
        "SAIL_RUNTIME__MEMORY_POOL__TYPE": "greedy",
        "SAIL_RUNTIME__MEMORY_POOL__GREEDY__MAX_SIZE": str(1024 * 1024 * 1024),
    }
    with spark_connect_server(envs=envs) as server:
        yield server.remote


pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail cluster mode")

ROWS = 2_000_000
CTE = f"c AS (SELECT id, max(id % 1000) AS g FROM range({ROWS}) GROUP BY id)"  # noqa: S608


def test_self_join_reads_shared_cte_at_different_paces(spark):
    query = f"WITH {CTE} SELECT count(*), sum(a.g + b.g) FROM c a JOIN c b ON a.id = b.id"  # noqa: S608
    assert spark.sql(query).collect() == [(ROWS, 2 * 999 * ROWS // 2)]
    plan = spark.sql(f"EXPLAIN {query}").collect()[0][0]
    assert ("SharedCteExec" in plan) == shares_deterministic_ctes(), plan


def test_union_all_reads_shared_cte_twice(spark):
    query = f"WITH {CTE} SELECT count(*), sum(g) FROM (SELECT * FROM c UNION ALL SELECT * FROM c)"  # noqa: S608
    assert spark.sql(query).collect() == [(2 * ROWS, 999 * ROWS)]


def test_limit_stops_reading_shared_cte(spark):
    query = f"WITH {CTE} SELECT a.id FROM c a JOIN c b ON a.id = b.id ORDER BY 1 LIMIT 3"  # noqa: S608
    assert spark.sql(query).collect() == [(0,), (1,), (2,)]
    query = f"WITH {CTE} SELECT count(*) FROM (SELECT a.id FROM c a UNION ALL SELECT b.id FROM c b) LIMIT 1"  # noqa: S608
    assert spark.sql(query).collect() == [(2 * ROWS,)]


def test_shared_cte_error_reaches_the_query(spark):
    cte = "c AS (SELECT id, max(100 DIV (id - 3)) AS q FROM range(10) GROUP BY id)"
    query = f"WITH {cte} SELECT sum(a.q + b.q) FROM c a JOIN c b ON a.id = b.id"  # noqa: S608
    with pytest.raises(PySparkException):
        spark.sql(query).collect()
    # The session runs other queries after the failure.
    assert spark.sql("SELECT 1").collect() == [(1,)]
