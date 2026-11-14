import random
from collections import defaultdict

import pytest
from pyspark.errors import PySparkException

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark, shares_deterministic_ctes
from pysail.tests._dev.spark.dataframe.test_shared_cte_volatile import ROWS, VOLATILE, cte, query


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
    plan = spark.sql(f"EXPLAIN {query}").collect()[0][0]
    assert ("SharedCteExec" in plan) == shares_deterministic_ctes(), plan


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
        f"c{i} AS (SELECT a.id FROM c{i - 1} a JOIN c{i - 1} b ON a.id = b.id)"  # noqa: S608
        for i in range(1, depth + 1)
    )
    query = f"WITH {', '.join(ctes)} SELECT count(*) FROM c{depth}"  # noqa: S608
    assert spark.sql(query).collect() == [(10,)]


def test_shared_cte_with_non_nullable_scalar_subquery(spark):
    query = """
        WITH c AS (SELECT id AS k, (SELECT count(*) FROM range(2)) AS v FROM range(3) GROUP BY id)
        SELECT sum(a.v) FROM c a JOIN c b ON a.k = b.k
    """
    assert spark.sql(query).collect() == [(6,)]


def test_insert_and_merge_from_shared_cte(spark, tmp_path):
    spark.sql(f"CREATE TABLE shared_cte_target (k BIGINT, n BIGINT) USING delta LOCATION '{tmp_path / 't'}'")
    cte = "c AS (SELECT id % 7 AS k, count(*) AS n FROM range(1000) GROUP BY 1)"
    try:
        spark.sql(f"INSERT INTO shared_cte_target WITH {cte} SELECT a.k, a.n + b.n FROM c a JOIN c b ON a.k = b.k")  # noqa: S608
        assert spark.sql("SELECT count(*), sum(n) FROM shared_cte_target").collect() == [(7, 2000)]
        spark.sql(f"""
            MERGE INTO shared_cte_target t
            USING (WITH {cte} SELECT a.k, a.n + b.n AS n FROM c a JOIN c b ON a.k = b.k) s
            ON t.k = s.k
            WHEN MATCHED THEN UPDATE SET n = t.n + s.n
        """)  # noqa: S608
        assert spark.sql("SELECT count(*), sum(n) FROM shared_cte_target").collect() == [(7, 4000)]
    finally:
        spark.sql("DROP TABLE IF EXISTS shared_cte_target")


def test_shared_cte_with_outer_join_feeds_a_join_on_its_key(spark):
    query = """
        WITH j AS (
            SELECT a.k, b.n
            FROM (SELECT id % 50 AS k FROM range(1000) GROUP BY 1) a
            LEFT JOIN (SELECT id % 10 AS k, count(*) AS n FROM range(1000) GROUP BY 1) b ON a.k = b.k
        )
        SELECT count(*), sum(x.n + y.n) FROM j x JOIN j y ON x.k = y.k
    """
    assert spark.sql(query).collect() == [(50, 2000)]


def test_shared_cte_error_fails_only_the_stages_that_read_it(spark):
    """The division fails for `v = 0`, and the CTE is read only if `e` is not empty."""
    spark.sql("CREATE OR REPLACE TEMP VIEW shared_cte_e AS SELECT id AS k FROM range(0, 1, 1, 2) WHERE id < 0")
    spark.sql(
        "CREATE OR REPLACE TEMP VIEW shared_cte_t AS "
        "SELECT CAST(id % 3 AS INT) AS k, CAST(id % 4 AS BIGINT) AS v FROM range(0, 10, 1, 2)"
    )
    cte = "c AS (SELECT k, v, 100 DIV v AS q FROM shared_cte_t GROUP BY k, v)"
    join = "JOIN c a ON e.k = a.k JOIN c b ON a.k = b.k AND a.q = b.q"
    try:
        query = f"WITH {cte} SELECT count(*) FROM shared_cte_e e {join}"  # noqa: S608
        assert spark.sql(query).collect() == [(0,)]
        plan = spark.sql(f"EXPLAIN {query}").collect()[0][0]
        assert ("SharedCteExec" in plan) == shares_deterministic_ctes(), plan
        query = f"WITH {cte} SELECT count(*) FROM shared_cte_t e {join}"  # noqa: S608
        # The error can be reported as a conflict between task attempts, since the
        # failed stage is retried after the query has received partial output.
        with pytest.raises(PySparkException):
            spark.sql(query).collect()
    finally:
        spark.sql("DROP VIEW IF EXISTS shared_cte_e")
        spark.sql("DROP VIEW IF EXISTS shared_cte_t")


@pytest.fixture
def nondeterministic_udf(spark):
    from pyspark.sql.functions import udf

    spark.udf.register("nondeterministic_udf", udf(lambda: random.random(), "double").asNondeterministic())  # noqa: S311


@pytest.mark.parametrize("shape", ["join", "union-all", "different-filters", "sorted-limit", "nested"])
@pytest.mark.parametrize("function", ["rand", "uuid", "udf"])
def test_stages_read_one_evaluation_of_volatile_cte(spark, nondeterministic_udf, function, shape):  # noqa: ARG001
    sql, expected = query(VOLATILE[function], shape)
    assert tuple(spark.sql(sql).collect()[0]) == expected


@pytest.mark.parametrize("function", ["rand", "uuid"])
def test_scalar_subquery_stage_reads_the_same_evaluation(spark, function):
    sql = f"{cte(VOLATILE[function])} SELECT count(*) FROM c WHERE r = (SELECT max(r) FROM c)"  # noqa: S608
    assert spark.sql(sql).collect()[0][0] >= 1
    sql = f"{cte(VOLATILE[function])} SELECT count_if(a.r <=> b.r) FROM c a JOIN c b ON a.id = b.id"  # noqa: S608
    assert spark.sql(sql).collect() == [(ROWS,)]


@pytest.fixture(scope="module")
def small_cluster_spark(request):
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__WORKER_MAX_COUNT": "1",
        "SAIL_CLUSTER__WORKER_TASK_SLOTS": str(getattr(request, "param", 10)),
    }
    with spark_connect_server(envs=envs) as server, spark_session_factory(server.remote) as sessions:
        yield sessions.create()


@pytest.mark.timeout(60)
@pytest.mark.parametrize(
    ("small_cluster_spark", "function", "depth", "readers"),
    [
        pytest.param(20, "rand()", 1, 2, id="rand-leaf-twenty-slots"),
        pytest.param(20, "uuid()", 1, 2, id="uuid-leaf-twenty-slots"),
        pytest.param(40, "uuid()", 2, 2, id="uuid-nested-forty-slots"),
    ],
    indirect=["small_cluster_spark"],
)
def test_union_of_volatile_cte_with_sufficient_capacity(small_cluster_spark, function, depth, readers):
    """Successfully scheduled references must share one evaluation."""
    ctes = [f"c0 AS (SELECT id, {function} AS r FROM range(0, 100, 1, 10))"]  # noqa: S608
    ctes.extend(
        f"c{i} AS (SELECT * FROM c{i - 1} UNION ALL SELECT * FROM c{i - 1})"  # noqa: S608
        for i in range(1, depth)
    )
    branches = " UNION ALL ".join([f"SELECT * FROM c{depth - 1}"] * readers)  # noqa: S608
    query = f"WITH {', '.join(ctes)} {branches}"
    values = defaultdict(list)
    for row in small_cluster_spark.sql(query).collect():
        values[row.id].append(row.r)
    assert set(values) == set(range(100))
    copies = readers * 2 ** (depth - 1)
    assert all(len(items) == copies and len(set(items)) == 1 for items in values.values())
    # Finishing the query must release its worker capacity for subsequent jobs.
    assert small_cluster_spark.range(1).collect() == [(0,)]


@pytest.mark.timeout(60)
def test_union_of_volatile_cte_rejects_insufficient_capacity(small_cluster_spark):
    query = "WITH c AS (SELECT id, uuid() AS r FROM range(0, 100, 1, 10)) SELECT * FROM c UNION ALL SELECT * FROM c"
    # Insufficient capacity must report the existing error, rather than evaluating
    # the volatile producer independently for each reference.
    with pytest.raises(
        PySparkException,
        match=r"task region requires 20 worker task slots, but the configured maximum is 10",
    ):
        small_cluster_spark.sql(query).collect()
    assert small_cluster_spark.range(1).collect() == [(0,)]


@pytest.mark.timeout(60)
@pytest.mark.parametrize("small_cluster_spark", [20], indirect=True, ids=["twenty-slots"])
def test_insert_from_volatile_cte_on_finite_cluster(small_cluster_spark, tmp_path):
    # The commit stage gathers twenty writer partitions, which also require twenty
    # slots on the merge base. This exercises the supported write path.
    small_cluster_spark.sql(
        f"CREATE TABLE shared_cte_capacity_write (id BIGINT, r STRING) USING delta LOCATION '{tmp_path / 'data'}'"
    )
    try:
        small_cluster_spark.sql(
            "INSERT INTO shared_cte_capacity_write "
            "WITH c AS (SELECT id, uuid() AS r FROM range(0, 100, 1, 10)) "
            "SELECT * FROM c UNION ALL SELECT * FROM c"
        )
        values = defaultdict(list)
        for row in small_cluster_spark.table("shared_cte_capacity_write").collect():
            values[row.id].append(row.r)
        assert set(values) == set(range(100))
        assert all(len(items) == 2 and len(set(items)) == 1 for items in values.values())
    finally:
        small_cluster_spark.sql("DROP TABLE IF EXISTS shared_cte_capacity_write")
