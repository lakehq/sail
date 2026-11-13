"""Non-deterministic CTEs that are referenced more than once.

Spark does not inline a non-deterministic CTE definition that is referenced more than once
(InlineCTE), so every reference sees the same rows. Random values differ from run to run,
so these tests check invariants that hold only when all references read one evaluation.
"""

import random

import pytest

from pysail.testing.spark.session import spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark

ROWS = 200
SOURCE = f"range(0, {ROWS}, 1, 4)"

VOLATILE = {
    "rand": "rand()",
    "randn": "randn()",
    "uuid": "uuid()",
    "shuffle": "array_join(shuffle(sequence(1, 10)), ',')",
    "udf": "nondeterministic_udf()",
    # These are evaluated the same way at every reference when the input is split the same way.
    "seeded": "rand(7)",
    "monotonically_increasing_id": "monotonically_increasing_id()",
    "spark_partition_id": "spark_partition_id()",
}

# (query after the WITH clause, expected single row)
SHAPES = {
    "join": ("SELECT count(*), count_if(a.r <=> b.r) FROM c a JOIN c b ON a.id = b.id", (ROWS, ROWS)),
    "union-all": (
        "SELECT count(*) FROM (SELECT id, r, count(*) AS k FROM (SELECT id, r FROM c UNION ALL SELECT id, r FROM c) "
        "GROUP BY id, r) WHERE k = 2",
        (ROWS,),
    ),
    "union-distinct": ("SELECT count(*) FROM (SELECT id, r FROM c UNION SELECT id, r FROM c)", (ROWS,)),
    "exists": ("SELECT count(*) FROM c a WHERE EXISTS (SELECT 1 FROM c b WHERE b.id = a.id AND b.r <=> a.r)", (ROWS,)),
    "in": ("SELECT count(*) FROM c a WHERE a.r IN (SELECT r FROM c)", (ROWS,)),
    "different-filters": (
        "SELECT count(*), count_if(a.r <=> b.r) FROM (SELECT * FROM c WHERE id < 150) a "
        "JOIN (SELECT * FROM c WHERE id >= 50) b ON a.id = b.id",
        (100, 100),
    ),
    "sorted-limit": (
        "SELECT count(*), count_if(a.r <=> b.r) FROM (SELECT * FROM c ORDER BY id LIMIT 50) a JOIN c b ON a.id = b.id",
        (50, 50),
    ),
    "limit": (
        "SELECT count(*), count_if(a.r <=> b.r) FROM (SELECT * FROM c LIMIT 50) a JOIN c b ON a.id = b.id",
        (50, 50),
    ),
    "aggregates": (
        "SELECT (SELECT max(r) FROM c) <=> (SELECT max(r) FROM c WHERE id >= 0), "
        "(SELECT count(DISTINCT r) FROM c) = (SELECT count(DISTINCT r) FROM (SELECT r FROM c UNION ALL SELECT r FROM c))",
        (True, True),
    ),
    "nested": (
        ", d AS (SELECT id, r FROM c WHERE id % 2 = 0) "
        "SELECT count(*), count_if(a.r <=> b.r) FROM d a JOIN d b ON a.id = b.id",
        (100, 100),
    ),
    "nested-and-direct": (
        ", d AS (SELECT id, r FROM c) SELECT count(*), count_if(c.r <=> d.r) FROM c JOIN d ON c.id = d.id",
        (ROWS, ROWS),
    ),
    "three-references": (
        "SELECT count(*) FROM c a JOIN c b ON a.id = b.id JOIN c x ON x.id = a.id WHERE a.r <=> b.r AND b.r <=> x.r",
        (ROWS,),
    ),
}


@pytest.fixture
def nondeterministic_udf(spark):
    from pyspark.sql.functions import udf

    spark.udf.register("nondeterministic_udf", udf(lambda: random.random(), "double").asNondeterministic())  # noqa: S311


def cte(expression):
    return f"WITH c AS (SELECT id, {expression} AS r FROM {SOURCE})"  # noqa: S608


def query(expression, shape):
    suffix, expected = SHAPES[shape]
    return cte(expression) + ("" if suffix.startswith(",") else " ") + suffix, expected


@pytest.mark.parametrize("shape", list(SHAPES))
@pytest.mark.parametrize("function", list(VOLATILE))
def test_references_to_volatile_cte_read_one_evaluation(spark, nondeterministic_udf, function, shape):  # noqa: ARG001
    sql, expected = query(VOLATILE[function], shape)
    assert tuple(spark.sql(sql).collect()[0]) == expected


@pytest.mark.parametrize("function", list(VOLATILE))
def test_scalar_subquery_reads_the_same_evaluation(spark, nondeterministic_udf, function):  # noqa: ARG001
    sql = f"{cte(VOLATILE[function])} SELECT count(*) FROM c WHERE r = (SELECT max(r) FROM c)"  # noqa: S608
    assert spark.sql(sql).collect()[0][0] >= 1


@pytest.mark.parametrize("function", ["rand", "uuid", "udf"])
def test_cte_in_scalar_subquery_reads_one_evaluation(spark, nondeterministic_udf, function):  # noqa: ARG001
    sql = f"SELECT ({cte(VOLATILE[function])} SELECT count_if(a.r <=> b.r) FROM c a JOIN c b ON a.id = b.id)"  # noqa: S608
    assert spark.sql(sql).collect() == [(ROWS,)]


def test_filters_on_volatile_columns_are_not_pushed_into_the_cte(spark):
    body = cte("rand()")
    sql = f"{body} SELECT count(*) FROM c a JOIN c b ON a.id = b.id WHERE a.r < 0.5 AND b.r >= 0.5"  # noqa: S608
    assert spark.sql(sql).collect() == [(0,)]
    sql = f"{body} SELECT count(*) = count_if(a.r = b.r) FROM (SELECT * FROM c WHERE r < 0.5) a JOIN c b ON a.id = b.id"  # noqa: S608
    assert spark.sql(sql).collect() == [(True,)]


def test_volatile_cte_skips_rows_that_no_reference_reads(spark):
    """The division fails only on a row that no reference reads."""
    body = "WITH c AS (SELECT id, rand() AS r, 10 DIV (id - 3) AS d FROM range(0, 10, 1, 2))"
    sql = f"{body} SELECT count(*), sum(a.d) FROM c a JOIN c b ON a.id = b.id WHERE a.id > 5 AND b.id > 5"  # noqa: S608
    assert spark.sql(sql).collect() == [(4, 8)]
    sql = (
        f"{body} SELECT count(*), sum(d) FROM "  # noqa: S608
        "(SELECT id, d FROM c WHERE id > 5 UNION ALL SELECT id, d FROM c WHERE id < 2)"
    )
    assert spark.sql(sql).collect() == [(6, 0)]


def test_volatile_filter_and_sort_in_cte(spark):
    sql = (
        f"WITH c AS (SELECT id FROM {SOURCE} WHERE rand() < 0.5) "  # noqa: S608
        "SELECT count(*) = (SELECT count(*) FROM c) FROM c a JOIN c b ON a.id = b.id"
    )
    assert spark.sql(sql).collect() == [(True,)]
    sql = (
        f"WITH c AS (SELECT id FROM {SOURCE} ORDER BY rand() LIMIT 20) SELECT count(*) FROM c a JOIN c b ON a.id = b.id"  # noqa: S608
    )
    assert spark.sql(sql).collect() == [(20,)]


def test_aggregates_and_windows_over_volatile_cte(spark):
    # Both aggregates read one evaluation of the CTE, but they can add the same values
    # in a different order, which changes the last bits of a sum of doubles.
    sql = (
        f"WITH c AS (SELECT id % 10 AS g, rand() AS r FROM {SOURCE}) "  # noqa: S608
        "SELECT count(*) FROM (SELECT g, sum(r) AS s FROM c GROUP BY g) a "
        "JOIN (SELECT g, sum(r) AS s FROM c GROUP BY g) b ON a.g = b.g WHERE abs(a.s - b.s) < 1e-9"
    )
    assert spark.sql(sql).collect() == [(10,)]
    sql = (
        f"{cte('rand()')} SELECT count(*) FROM (SELECT id, rank() OVER (ORDER BY r) AS k FROM c) a "  # noqa: S608
        "JOIN (SELECT id, rank() OVER (ORDER BY r) AS k FROM c) b ON a.id = b.id AND a.k = b.k"
    )
    assert spark.sql(sql).collect() == [(ROWS,)]


def test_several_volatile_ctes(spark):
    sql = (
        f"WITH c AS (SELECT id, rand() AS r FROM {SOURCE}), d AS (SELECT id, uuid() AS u FROM {SOURCE}) "  # noqa: S608
        "SELECT count(*) FROM c a JOIN c b ON a.id = b.id JOIN d x ON x.id = a.id JOIN d y ON y.id = a.id "
        "WHERE a.r = b.r AND x.u = y.u"
    )
    assert spark.sql(sql).collect() == [(ROWS,)]


def test_input_file_name_in_volatile_cte(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.range(0, ROWS, 1, 4).write.parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("volatile_cte_input_files")
    try:
        body = "WITH c AS (SELECT id, input_file_name() AS r FROM volatile_cte_input_files)"
        for shape in ["join", "different-filters", "limit", "nested"]:
            suffix, expected = SHAPES[shape]
            sql = body + ("" if suffix.startswith(",") else " ") + suffix
            assert tuple(spark.sql(sql).collect()[0]) == expected, shape
    finally:
        spark.catalog.dropTempView("volatile_cte_input_files")


def test_scalar_in_and_exists_subqueries_read_one_evaluation(spark):
    sql = (
        f"{cte('rand()')} SELECT count(*) FROM c a WHERE a.r IN (SELECT r FROM c) "  # noqa: S608
        "AND EXISTS (SELECT 1 FROM c b WHERE b.id = a.id AND b.r = a.r) "
        "AND a.r <= (SELECT max(r) FROM c) AND (SELECT max(r) FROM c) IN (SELECT r FROM c)"
    )
    assert spark.sql(sql).collect() == [(ROWS,)]


@pytest.mark.spark(version=">=4", reason="Python UDTF")
def test_nondeterministic_udtf_in_cte_reads_one_evaluation(spark):
    """Python UDTFs are non-deterministic unless marked deterministic."""
    from pyspark.sql.functions import udtf

    @udtf(returnType="id: int, r: double")
    class VolatileCteRows:
        def eval(self, n: int):
            for i in range(n):
                yield i, random.random()  # noqa: S311

    spark.udtf.register("volatile_cte_rows", VolatileCteRows)
    body = "WITH c AS (SELECT * FROM volatile_cte_rows(50))"
    sql = f"{body} SELECT count(*), count_if(a.r = b.r) FROM c a JOIN c b ON a.id = b.id"  # noqa: S608
    assert spark.sql(sql).collect() == [(50, 50)]
    sql = f"{body} SELECT count(DISTINCT r) FROM (SELECT r FROM c UNION ALL SELECT r FROM c)"  # noqa: S608
    assert spark.sql(sql).collect() == [(50,)]


@pytest.mark.parametrize(
    ("operation", "expected"),
    [("EXCEPT ALL", 0), ("EXCEPT", 0), ("INTERSECT ALL", ROWS), ("INTERSECT", ROWS)],
)
def test_set_operations_over_volatile_cte(spark, operation, expected):
    sql = f"{cte('rand()')} SELECT count(*) FROM (SELECT id, r FROM c {operation} SELECT id, r FROM c)"  # noqa: S608
    assert spark.sql(sql).collect() == [(expected,)]


@pytest.mark.parametrize("function", ["rand", "seeded", "monotonically_increasing_id", "spark_partition_id"])
def test_repartitioned_volatile_cte(spark, function):
    """Round-robin repartitioning makes even partition-dependent values differ between evaluations."""
    sql = (
        f"WITH c AS (SELECT id, {VOLATILE[function]} AS r "  # noqa: S608
        f"FROM (SELECT /*+ REPARTITION(5) */ id FROM range(0, 2000, 1, 4)) t) "
        "SELECT count(*) FROM c a JOIN c b ON a.id = b.id AND a.r = b.r"
    )
    assert spark.sql(sql).collect() == [(2000,)]


def test_input_file_name_with_rand_in_volatile_cte(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.range(0, ROWS, 1, 4).write.parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("volatile_cte_input_files_rand")
    try:
        sql = (
            "WITH c AS (SELECT id, input_file_name() AS f, rand() AS r FROM volatile_cte_input_files_rand) "
            "SELECT count(*), count_if(a.f = b.f) FROM c a JOIN c b ON a.id = b.id AND a.r = b.r"
        )
        assert spark.sql(sql).collect() == [(ROWS, ROWS)]
    finally:
        spark.catalog.dropTempView("volatile_cte_input_files_rand")


@pytest.mark.spark(version=">=4", reason="Python UDTF")
def test_rand_over_deterministic_udtf_in_cte_reads_one_evaluation(spark):
    from pyspark.sql.functions import udtf

    @udtf(returnType="id: int")
    class DeterministicCteRows:
        def eval(self, n: int):
            yield from ((i,) for i in range(n))

    spark.udtf.register("deterministic_cte_rows", DeterministicCteRows.asDeterministic())
    sql = (
        "WITH c AS (SELECT id, rand() AS r FROM deterministic_cte_rows(50)) "
        "SELECT count(*), count_if(a.r = b.r) FROM c a JOIN c b ON a.id = b.id"
    )
    assert spark.sql(sql).collect() == [(50, 50)]


@pytest.mark.parametrize("operation", ["EXCEPT ALL", "EXCEPT"])
def test_first_reference_in_set_operation(spark, operation):
    sql = (
        f"{cte('rand()')} SELECT count(*) FROM "  # noqa: S608
        f"(SELECT id, r FROM c {operation} SELECT id, CAST(-1 AS DOUBLE) FROM range(0)) x "
        "JOIN c y ON x.id = y.id AND x.r = y.r"
    )
    assert spark.sql(sql).collect() == [(ROWS,)]


@pytest.mark.skipif(is_jvm_spark(), reason="Spark rejects nondeterministic LATERAL subqueries")
def test_first_reference_in_correlated_lateral_subquery(spark):
    """Spark rejects nondeterministic LATERAL subqueries (NON_DETERMINISTIC_LATERAL_SUBQUERIES), while
    Sail accepts them, as it did before, and evaluates the CTE once for all its references."""
    sql = (
        f"{cte('rand()')} SELECT count(*) FROM "  # noqa: S608
        f"(SELECT x.id, x.r FROM range(0, {ROWS}) t, LATERAL (SELECT c.id, c.r FROM c WHERE c.id = t.id) x) z "
        "JOIN c y ON z.id = y.id AND z.r = y.r"
    )
    assert spark.sql(sql).collect() == [(ROWS,)]


def test_first_reference_in_set_operation_with_parameters(spark):
    sql = (
        f"WITH c AS (SELECT id, rand() AS r FROM {SOURCE} WHERE id < :n) SELECT count(*) FROM "  # noqa: S608
        "(SELECT id, r FROM c EXCEPT ALL SELECT id, CAST(-1 AS DOUBLE) FROM range(0)) x "
        "JOIN c y ON x.id = y.id AND x.r = y.r AND y.id < :m"
    )
    assert spark.sql(sql, args={"n": ROWS, "m": 50}).collect() == [(50,)]
    sql = sql.replace(":n", "?").replace(":m", "?")
    assert spark.sql(sql, args=[ROWS, 50]).collect() == [(50,)]


def test_volatile_cte_with_one_reference_ordered_by_hidden_column(spark):
    sql = f"{cte('rand()')} SELECT id FROM c ORDER BY r"  # noqa: S608
    assert sorted(row[0] for row in spark.sql(sql).collect()) == list(range(ROWS))
    sql = f"{cte('rand()')} SELECT id % 2 AS k FROM c GROUP BY 1 HAVING max(r) >= 0 ORDER BY k"  # noqa: S608
    assert spark.sql(sql).collect() == [(0,), (1,)]


SELF_JOIN = "SELECT count(*), count_if(a.r = b.r) FROM c a JOIN c b ON a.id = b.id"
MATCHED = "SELECT x.id, x.r FROM c x JOIN c y ON x.id = y.id AND x.r = y.r"


@pytest.mark.parametrize("kind", ["TEMPORARY", "GLOBAL TEMPORARY"])
def test_volatile_cte_in_view(spark, kind):
    name = "volatile_cte_view" if kind == "TEMPORARY" else "global_temp.volatile_cte_view"
    spark.sql(f"CREATE OR REPLACE {kind} VIEW volatile_cte_view AS {cte('rand()')} {SELF_JOIN}")
    try:
        assert spark.sql(f"SELECT * FROM {name}").collect() == [(ROWS, ROWS)]  # noqa: S608
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {name}")


def test_volatile_cte_in_dataframe_view(spark):
    spark.sql(f"{cte('rand()')} {MATCHED}").createOrReplaceTempView("volatile_cte_df_view")
    try:
        assert spark.sql("SELECT count(*) FROM volatile_cte_df_view").collect() == [(ROWS,)]
        # A query CTE that reads the view reads it once for all its references.
        sql = (
            "WITH q AS (SELECT * FROM volatile_cte_df_view) "
            "SELECT count(*) FROM q a JOIN q b ON a.id = b.id AND a.r = b.r"
        )
        assert spark.sql(sql).collect() == [(ROWS,)]
    finally:
        spark.catalog.dropTempView("volatile_cte_df_view")


def test_view_used_twice_evaluates_its_cte_for_each_use(spark):
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW volatile_cte_twice_view AS {cte('rand()')} SELECT * FROM c")  # noqa: S608
    try:
        sql = (
            "SELECT count(*) FROM volatile_cte_twice_view p JOIN volatile_cte_twice_view q ON p.id = q.id AND p.r = q.r"
        )
        assert spark.sql(sql).collect() == [(0,)]
    finally:
        spark.sql("DROP VIEW IF EXISTS volatile_cte_twice_view")


def test_nested_volatile_ctes_and_scalar_subquery_in_view(spark):
    body = f"{cte('rand()')}, d AS (SELECT id, r FROM c WHERE id % 2 = 0)"  # noqa: S608
    spark.sql(
        f"CREATE OR REPLACE TEMPORARY VIEW volatile_cte_nested_view AS {body} "  # noqa: S608
        "SELECT count(*), count_if(a.r = b.r), (SELECT count(*) FROM c WHERE r = (SELECT max(r) FROM c)) "
        "FROM d a JOIN d b ON a.id = b.id"
    )
    try:
        assert spark.sql("SELECT * FROM volatile_cte_nested_view").collect() == [(100, 100, 1)]
    finally:
        spark.sql("DROP VIEW IF EXISTS volatile_cte_nested_view")


def test_volatile_cte_in_global_view_read_by_another_session(spark, remote):
    spark.sql(f"CREATE OR REPLACE GLOBAL TEMPORARY VIEW volatile_cte_shared_view AS {cte('rand()')} {SELF_JOIN}")
    try:
        with spark_session_factory(remote) as sessions:
            other = sessions.create()
            assert other.sql("SELECT * FROM global_temp.volatile_cte_shared_view").collect() == [(ROWS, ROWS)]
    finally:
        spark.sql("DROP VIEW IF EXISTS global_temp.volatile_cte_shared_view")


def test_volatile_cte_view_input_files(spark):
    spark.sql(f"{cte('rand()')} {MATCHED}").createOrReplaceTempView("volatile_cte_files_view")
    try:
        assert spark.table("volatile_cte_files_view").inputFiles() == []
        assert spark.table("volatile_cte_files_view").columns == ["id", "r"]
    finally:
        spark.catalog.dropTempView("volatile_cte_files_view")


def test_volatile_cte_with_one_reference(spark):
    sql = f"{cte('rand()')} SELECT count(*), count(DISTINCT r) FROM c"  # noqa: S608
    assert spark.sql(sql).collect() == [(ROWS, ROWS)]


def test_separate_with_clauses_are_evaluated_separately(spark):
    """Two WITH clauses define two CTEs, even with the same name and text."""
    body = f"(WITH c AS (SELECT id, rand() AS r FROM {SOURCE}) SELECT * FROM c)"  # noqa: S608
    sql = f"SELECT count(*) FROM {body} a JOIN {body} b ON a.id = b.id WHERE a.r = b.r"  # noqa: S608
    assert spark.sql(sql).collect() == [(0,)]


@pytest.mark.skipif(is_jvm_spark(), reason="Sail physical plans")
def test_volatile_cte_is_shared(spark):
    sql = f"{cte('rand()')} SELECT count(*) FROM c a JOIN c b ON a.id = b.id WHERE a.r = b.r"  # noqa: S608
    assert spark.sql(sql).collect() == [(ROWS,)]
    text = spark.sql(f"EXPLAIN {sql}").collect()[0][0]
    assert "SharedCteExec" in text, text
    assert text.count("RangeExec") == 1, text


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("SELECT (SELECT max(s) FROM c WHERE k < 5) = (SELECT max(s) FROM c WHERE k >= 5)", True),
        ("SELECT (SELECT count(*) FROM c WHERE s = (SELECT max(s) FROM c))", 10),
    ],
    ids=["scalar-subqueries", "nested-scalar-subqueries"],
)
@pytest.mark.parametrize("function", ["rand()", "uuid()"])
def test_cte_reading_a_volatile_cte_through_a_scalar_subquery_is_evaluated_once(spark, function, query, expected):
    """Spark inlines the volatile CTE, which has one reference, into the other CTE, which is then
    non-deterministic and evaluated once for its references (InlineCTE)."""
    ctes = (
        f"WITH c0 AS (SELECT {function} AS r FROM range(1)), "  # noqa: S608
        "c AS (SELECT id AS k, (SELECT max(r) FROM c0) AS s FROM range(10)) "
    )
    assert spark.sql(ctes + query).collect()[0][0] == expected
