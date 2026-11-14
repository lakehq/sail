import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.fixture(scope="module")
def remote():
    with spark_connect_server(envs={"SAIL_OPTIMIZER__SHARE_CTE_DEFINITIONS": "false"}) as server:
        yield server.remote


@pytest.mark.skipif(is_jvm_spark(), reason="Sail configuration")
def test_cte_references_are_inlined_when_sharing_is_disabled(spark):
    query = """
        WITH c AS (SELECT id % 7 AS k, count(*) AS n FROM range(1000) GROUP BY 1)
        SELECT sum(a.n + b.n) FROM c a JOIN c b ON a.k = b.k
    """
    assert spark.sql(query).collect() == [(2000,)]
    plan = spark.sql(f"EXPLAIN {query}").collect()[0][0]
    assert "SharedCteExec" not in plan
    assert plan.count("RangeExec") == 2  # noqa: PLR2004


@pytest.mark.skipif(is_jvm_spark(), reason="Sail configuration")
def test_volatile_cte_references_are_evaluated_separately_when_sharing_is_disabled(spark):
    query = (
        "WITH c AS (SELECT id, rand() AS r FROM range(0, 200, 1, 4)) "
        "SELECT count(*) FROM c a JOIN c b ON a.id = b.id WHERE a.r = b.r"
    )
    assert spark.sql(query).collect() == [(0,)]
    plan = spark.sql(f"EXPLAIN {query}").collect()[0][0]
    assert "SharedCteExec" not in plan
    assert plan.count("RangeExec") == 2  # noqa: PLR2004


@pytest.mark.skipif(is_jvm_spark(), reason="Sail configuration")
def test_volatile_cte_in_view_is_evaluated_separately_when_sharing_is_disabled(spark):
    spark.sql(
        "CREATE OR REPLACE TEMPORARY VIEW shared_cte_disabled_view AS "
        "WITH c AS (SELECT id, rand() AS r FROM range(0, 200, 1, 4)) "
        "SELECT count(*) FROM c a JOIN c b ON a.id = b.id WHERE a.r = b.r"
    )
    try:
        assert spark.sql("SELECT * FROM shared_cte_disabled_view").collect() == [(0,)]
    finally:
        spark.sql("DROP VIEW IF EXISTS shared_cte_disabled_view")
