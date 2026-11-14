"""CTEs that are referenced more than once.

Spark inlines a deterministic CTE at every reference. Sail evaluates the CTE once for
the references that read the same rows of it, and inlines it for the other references.
These tests check the results on every engine, and on Sail they also check the plans.
"""

import pytest
from pyspark.errors import PySparkException

from pysail.testing.spark.utils.common import is_jvm_spark, shares_deterministic_ctes

CTE = "c AS (SELECT id % 7 AS k, count(*) AS n, max(id) AS m FROM range(1000) GROUP BY 1)"


def plan(spark, query):
    return spark.sql(f"EXPLAIN {query}").collect()[0][0]


def check(spark, query, expected, *, shared, scans, must_share=False):
    """Checks the result, and on Sail whether a CTE is shared and how often the
    range is scanned."""
    assert sorted(tuple(row) for row in spark.sql(query).collect()) == sorted(expected)
    if not is_jvm_spark():
        text = plan(spark, query)
        if shared and not must_share and not shares_deterministic_ctes():
            # Each reference evaluates the CTE, as on the merge base. Only volatile CTEs
            # and the CTEs that refer to other shared CTEs are shared.
            assert "SharedCteExec" not in text, text
            return
        assert ("SharedCteExec" in text) == shared, text
        assert text.count("RangeExec") == scans, text


def nested(depth, projection="count(*)"):
    ctes = ["c0 AS (SELECT id FROM range(10))"]
    ctes.extend(
        f"c{i} AS (SELECT a.id FROM c{i - 1} a JOIN c{i - 1} b ON a.id = b.id)"  # noqa: S608
        for i in range(1, depth + 1)
    )
    return f"WITH {', '.join(ctes)} SELECT {projection} FROM c{depth}"  # noqa: S608


@pytest.mark.parametrize("depth", [12, 16, 24])
def test_nested_cte_plan_is_linear_in_depth(spark, depth):
    query = nested(depth)
    if is_jvm_spark():
        # Spark plans 2^depth copies of the first CTE.
        if depth > 8:  # noqa: PLR2004
            pytest.skip("the plan is exponential in the depth")
    else:
        text = plan(spark, query)
        assert len(text.splitlines()) < 10 * depth + 20
        # The first CTE only projects the range, so it is inlined into the second one,
        # whose join is evaluated by each of its two references in the third one.
        assert text.count("RangeExec") == 4  # noqa: PLR2004
    assert spark.sql(query).collect() == [(10,)]


@pytest.mark.skipif(is_jvm_spark(), reason="Spark expands deterministic CTEs at every reference")
@pytest.mark.timeout(60)
def test_nested_cte_insert_plan_is_linear_in_depth(spark, tmp_path):
    # Searching for DEFAULT in a write must visit CTE definitions without expanding
    # the 2^depth references. EXPLAIN plans the sink without writing any rows.
    depth = 24
    spark.sql(f"CREATE TABLE nested_cte_insert (id BIGINT) USING parquet LOCATION '{tmp_path / 'data'}'")
    try:
        text = plan(spark, f"INSERT INTO nested_cte_insert {nested(depth, projection='id')}")
        assert "DataSinkExec" in text, text
        assert "SharedCteExec" in text, text
        assert len(text.splitlines()) < 10 * depth + 30
        assert "RangeExec" in text, text
        assert spark.table("nested_cte_insert").collect() == []
    finally:
        spark.sql("DROP TABLE nested_cte_insert")


@pytest.mark.parametrize("references", [2, 3, 4])
def test_join_references_share_one_evaluation(spark, references):
    joins = " ".join(f"JOIN c t{i} ON t0.k = t{i}.k" for i in range(1, references))
    total = " + ".join(f"t{i}.n" for i in range(references))
    query = f"WITH {CTE} SELECT sum({total}) FROM c t0 {joins}"  # noqa: S608
    check(spark, query, [(1000 * references,)], shared=True, scans=1)


@pytest.mark.parametrize("references", [2, 3])
def test_union_references_share_one_evaluation(spark, references):
    branches = " UNION ALL ".join("SELECT n FROM c" for _ in range(references))
    query = f"WITH {CTE} SELECT sum(n) FROM ({branches})"  # noqa: S608
    check(spark, query, [(1000 * references,)], shared=True, scans=1)


def test_references_with_different_columns_share_one_evaluation(spark):
    query = f"WITH {CTE} SELECT a.k, a.n, b.m FROM c a JOIN c b ON a.k = b.k WHERE a.k = 0"  # noqa: S608
    check(spark, query, [(0, 143, 994)], shared=True, scans=1)
    query = f"WITH {CTE} SELECT sum(a.n), max(b.m) FROM c a JOIN c b ON a.k = b.k"  # noqa: S608
    check(spark, query, [(1000, 999)], shared=True, scans=1)


def test_references_with_the_same_filter_share_one_evaluation(spark):
    query = f"WITH {CTE} SELECT k, n FROM c WHERE k < 2 UNION ALL SELECT k, m FROM c WHERE k < 2"  # noqa: S608
    check(spark, query, [(0, 143), (1, 143), (0, 994), (1, 995)], shared=True, scans=1)


def test_references_with_different_filters_are_inlined(spark):
    query = f"WITH {CTE} SELECT k, n FROM c WHERE k < 2 UNION ALL SELECT k, n FROM c WHERE k > 5"  # noqa: S608
    check(spark, query, [(0, 143), (1, 143), (6, 142)], shared=False, scans=2)


def test_references_with_and_without_limit_are_inlined(spark):
    query = """
        WITH c AS (SELECT id FROM range(10))
        SELECT count(*) FROM (SELECT * FROM c WHERE id < 2 UNION ALL (SELECT * FROM c LIMIT 3))
    """
    check(spark, query, [(5,)], shared=False, scans=2)


def test_references_with_the_same_limit_are_inlined(spark):
    """A limit stops reading its input early, so the division by zero is not evaluated."""
    cte = "c AS (SELECT id, 1 / (id - 150000) AS r FROM range(0, 200000, 1, 1))"
    branch = "(SELECT r FROM c WHERE id >= 0 LIMIT 1)"
    query = f"WITH {cte} {branch} UNION ALL {branch}"
    assert len(spark.sql(query).collect()) == 2  # noqa: PLR2004
    if not is_jvm_spark():
        assert "SharedCteExec" not in plan(spark, query)
    query = f"WITH {CTE} (SELECT max(n) FROM c) UNION ALL (SELECT max(m) FROM c) LIMIT 2"  # noqa: S608
    check(spark, query, [(143,), (999,)], shared=True, scans=1)


def test_filter_columns_are_evaluated_by_the_filter_only(spark):
    """The second operand of the filter fails for `id = 3`, where the first one holds."""
    cte = "c AS (SELECT id, 1 / (id - 3) AS r FROM range(0, 6, 1, 2))"
    branch = "(SELECT id FROM c WHERE id >= 0 OR r > 0)"
    query = f"WITH {cte} {branch} UNION ALL {branch}"
    assert len(spark.sql(query).collect()) == 12  # noqa: PLR2004


def test_parameters_in_shared_cte(spark):
    cte = "c AS (SELECT id, max(id) + {n} AS v FROM range(10) WHERE id > {lo} GROUP BY id)"
    query = f"WITH {cte} SELECT sum(a.v + b.v) FROM c a JOIN c b ON a.id = b.id"  # noqa: S608
    named = query.format(n=":n", lo=":lo")
    assert spark.sql(named, args={"lo": 3, "n": 100}).collect() == [(1278,)]
    positional = query.format(n="?", lo="?")
    assert spark.sql(positional, args=[100, 3]).collect() == [(1278,)]
    if not is_jvm_spark():
        text = spark.sql(positional, args=[100, 3])._explain_string()  # noqa: SLF001
        assert ("SharedCteExec" in text) == shares_deterministic_ctes(), text


def test_input_files_of_shared_cte(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.range(30).repartition(3).write.parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("shared_cte_input_files")
    try:
        df = spark.sql("WITH c AS (SELECT id FROM shared_cte_input_files) SELECT a.id FROM c a JOIN c b ON a.id = b.id")
        files = spark.read.parquet(path).inputFiles()
        assert files
        assert sorted(df.inputFiles()) == sorted(files)
        assert df.count() == 30  # noqa: PLR2004
    finally:
        spark.catalog.dropTempView("shared_cte_input_files")


def test_filtered_rows_are_not_evaluated(spark):
    """The division fails for `id = 2`, which every reference filters out."""
    cte = "c AS (SELECT id, 10 / (id - 2) AS r FROM range(5) GROUP BY id)"
    query = f"WITH {cte} SELECT a.r, b.r FROM c a JOIN c b ON a.id = b.id WHERE a.id > 2 AND b.id > 2"  # noqa: S608
    check(spark, query, [(10.0, 10.0), (5.0, 5.0)], shared=True, scans=1)
    # A scalar subquery in the filter is not part of what the references read.
    query = (
        f"WITH {cte} SELECT a.r, b.r FROM c a JOIN c b ON a.id = b.id "  # noqa: S608
        "WHERE a.id > (SELECT 2) AND b.id > (SELECT 2)"
    )
    assert sorted(tuple(row) for row in spark.sql(query).collect()) == [(5.0, 5.0), (10.0, 10.0)]
    query = f"WITH {cte} SELECT a.r FROM c a JOIN c b ON a.id = b.id"  # noqa: S608
    # In cluster mode the error can be reported as a conflict between task attempts, since
    # the failed stage is retried after the query has received partial output.
    with pytest.raises(PySparkException, match=r"(?i)divi|different attempt"):
        spark.sql(query).collect()


def test_volatile_cte_is_shared(spark):
    query = (
        "WITH c AS (SELECT id, rand() AS r FROM range(10)) "
        "SELECT count(*) FROM c a JOIN c b ON a.id = b.id AND a.r = b.r"
    )
    check(spark, query, [(10,)], shared=True, scans=1, must_share=True)


def test_volatile_cte_in_projected_exists_is_shared(spark):
    query = (
        "WITH c AS (SELECT id, rand() AS r FROM range(10)) "
        "SELECT count_if(e), count(*) FROM (SELECT EXISTS (SELECT a.r FROM c b) AS e FROM c a)"
    )
    check(spark, query, [(10, 10)], shared=True, scans=1, must_share=True)
    query = (
        "WITH c AS (SELECT id, rand() AS r FROM range(10)) "
        "SELECT count_if(e), count(*) FROM (SELECT EXISTS (SELECT 1 FROM c b WHERE b.r = a.r) AS e FROM c a)"
    )
    check(spark, query, [(10, 10)], shared=True, scans=1, must_share=True)


def test_deterministic_cte_in_view_is_inlined(spark):
    spark.sql(
        "CREATE OR REPLACE TEMPORARY VIEW shared_cte_view AS "  # noqa: S608
        f"WITH {CTE} SELECT a.k, a.n + b.n AS n FROM c a JOIN c b ON a.k = b.k"
    )
    try:
        check(spark, "SELECT sum(n) FROM shared_cte_view", [(2000,)], shared=False, scans=2)
    finally:
        spark.sql("DROP VIEW IF EXISTS shared_cte_view")


def test_nested_ctes_over_outer_join_views(spark):
    """A CTE that only projects a CTE that is referenced once, in a chain over an outer join."""
    spark.sql(
        "CREATE OR REPLACE TEMPORARY VIEW shared_cte_t1 AS "
        "SELECT CAST(CASE id WHEN 0 THEN 1 WHEN 1 THEN 1 WHEN 2 THEN 2 WHEN 3 THEN 2 WHEN 4 THEN 3 WHEN 7 "
        "THEN 4 WHEN 8 THEN 5 WHEN 9 THEN 3 END AS INT) AS k, CAST(CASE id WHEN 0 THEN 10 WHEN 1 THEN 10 "
        "WHEN 2 THEN 20 WHEN 4 THEN 30 WHEN 5 THEN 40 WHEN 7 THEN 0 WHEN 8 THEN -5 WHEN 9 THEN 30 END AS "
        "BIGINT) AS v, CAST(CASE id WHEN 0 THEN 'a' WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN 'B' WHEN "
        "5 THEN 'd' WHEN 7 THEN '' WHEN 8 THEN 'e' WHEN 9 THEN 'c' END AS STRING) AS s FROM range(0, 10, "
        "1, 2)"
    )
    spark.sql(
        "CREATE OR REPLACE TEMPORARY VIEW shared_cte_t2 AS "
        "SELECT CAST(CASE id WHEN 0 THEN 1 WHEN 1 THEN 2 WHEN 2 THEN 2 WHEN 3 THEN 6 WHEN 5 THEN 4 END AS "
        "INT) AS k, CAST(CASE id WHEN 0 THEN 100 WHEN 1 THEN 200 WHEN 2 THEN 201 WHEN 3 THEN 600 WHEN 4 "
        "THEN 700 WHEN 5 THEN 0 END AS BIGINT) AS v, CAST(CASE id WHEN 0 THEN 'x' WHEN 1 THEN 'y' WHEN 2 "
        "THEN 'y' WHEN 3 THEN 'z' WHEN 5 THEN 'a' END AS STRING) AS s FROM range(0, 6, 1, 2)"
    )
    ctes = (
        "WITH c0 AS (SELECT a.k, b.v, b.s FROM shared_cte_t1 a LEFT JOIN shared_cte_t2 b ON a.k = b.k AND a.v < b.v), "
        "c1 AS (SELECT k, v, s FROM c0), "
        "c2 AS (SELECT k, v, s FROM c1 a WHERE EXISTS (SELECT 1 FROM c1 b WHERE a.k = b.k)), "
    )
    try:
        query = (
            f"{ctes} c3 AS (SELECT a.k, b.v, a.s FROM c2 a "  # noqa: S608
            "JOIN (SELECT DISTINCT k, v FROM c2 WHERE s IN ('a', 'b', 'x')) b ON a.k = b.k) "
            "SELECT a.k, a.v, a.s, (SELECT count(*) FROM c1 x WHERE x.v >= a.v) AS q0 FROM c3 a"
        )
        assert sorted(tuple(row) for row in spark.sql(query).collect()) == [(1, 100, "x", 4)] * 2
        query = (
            f"{ctes} c3 AS (SELECT a.k, b.v, a.s FROM c2 a "  # noqa: S608
            "JOIN (SELECT DISTINCT k, v FROM c2 WHERE s IN ('a', 'b', 'x')) b ON a.k = b.k "
            "ORDER BY a.k DESC NULLS LAST, b.v DESC NULLS LAST, a.s DESC NULLS LAST LIMIT 10) "
            "SELECT a.k, a.v, a.s, (SELECT count(DISTINCT x.s) FROM c1 x WHERE x.v >= a.v) AS q0 "
            "FROM (SELECT k, v, concat(s, '_') AS s FROM c3) a "
            "WHERE EXISTS (SELECT 1 FROM (SELECT k, coalesce(v, -1) AS v, coalesce(s, 'n') AS s FROM c0) x "
            "WHERE x.k <=> a.k AND x.s <> a.s)"
        )
        assert sorted(tuple(row) for row in spark.sql(query).collect()) == [(1, 100, "x_", 2)] * 2
    finally:
        spark.sql("DROP VIEW IF EXISTS shared_cte_t1")
        spark.sql("DROP VIEW IF EXISTS shared_cte_t2")


@pytest.mark.skipif(is_jvm_spark(), reason="Spark rejects the cast")
def test_column_that_the_optimizer_finds_not_nullable_is_not_computed(spark):
    """The optimizer removes a column that a reference only tests for null, when it finds the
    column not nullable, as it does with the CTE in place of the reference."""
    spark.conf.set("spark.sql.ansi.enabled", "true")
    q = "CAST(CASE WHEN id = 3 THEN 'x' ELSE CAST(id AS STRING) END AS INT)"
    try:
        query = (
            f"WITH c AS (SELECT id, {q} AS q, rand() AS r FROM range(0, 20, 1, 2)) "  # noqa: S608
            "SELECT count(*) FROM c a JOIN c b ON a.id = b.id AND a.r = b.r WHERE a.q IS NOT NULL AND b.q IS NOT NULL"
        )
        assert spark.sql(query).collect() == [(20,)]
        query = (
            f"WITH c AS (SELECT id, {q} AS q FROM range(0, 20, 1, 2) GROUP BY id) "  # noqa: S608
            "SELECT count_if(a.q IS NULL) FROM c a JOIN c b ON a.id = b.id"
        )
        assert spark.sql(query).collect() == [(0,)]
    finally:
        spark.conf.unset("spark.sql.ansi.enabled")


def test_sorted_cte_is_inlined(spark):
    query = """
        WITH c AS (SELECT id FROM range(5) ORDER BY id DESC)
        SELECT first(id) FROM c UNION ALL SELECT last(id) FROM c
    """
    check(spark, query, [(4,), (0,)], shared=False, scans=2)


def test_from_and_scalar_subquery_references_share_one_evaluation(spark):
    query = f"WITH {CTE} SELECT k, n FROM c WHERE n = (SELECT max(n) FROM c) AND k > 4"  # noqa: S608
    check(spark, query, [(5, 143)], shared=False, scans=2)
    query = f"WITH {CTE} SELECT sum(n), (SELECT max(m) FROM c) FROM c"  # noqa: S608
    check(spark, query, [(1000, 999)], shared=True, scans=1)


def test_nested_cte_references_share_one_evaluation(spark):
    query = f"""
        WITH {CTE},
        d AS (SELECT a.k, a.n + b.n AS t FROM c a JOIN c b ON a.k = b.k)
        SELECT sum(x.t), sum(y.t) FROM d x JOIN d y ON x.k = y.k
    """  # noqa: S608
    # The second CTE refers to the first one, so it is shared in any configuration; the
    # first one is evaluated by each reference in it when deterministic CTEs are not shared.
    scans = 1 if shares_deterministic_ctes() else 2
    check(spark, query, [(2000, 2000)], shared=True, scans=scans, must_share=True)


def test_cte_that_projects_one_source_is_inlined(spark):
    query = "WITH c AS (SELECT id, id + 1 AS j FROM range(10) WHERE id > 1) SELECT sum(a.j + b.j) FROM c a JOIN c b ON a.id = b.id"
    check(spark, query, [(104,)], shared=False, scans=2)


def test_distinct_references_turn_a_join_into_a_semi_join(spark):
    cte = "j AS (SELECT a.id, b.v FROM range(10) a JOIN (SELECT id % 3 AS id, id AS v FROM range(9)) b ON a.id = b.id)"
    query = f"WITH {cte} SELECT count(*) FROM (SELECT DISTINCT id FROM j UNION ALL SELECT DISTINCT id FROM j)"  # noqa: S608
    assert spark.sql(query).collect() == [(6,)]
    if not is_jvm_spark():
        text = plan(spark, query)
        assert "SharedCteExec" not in text
        assert "LeftSemi" in text or "RightSemi" in text


def test_optimizer_failure_with_references_falls_back_to_inlined_ctes(spark):
    """The optimizer fails on this query unless the empty `c2` is in the place of its references."""
    query = """
        WITH c0 AS (SELECT * FROM VALUES (1, 1L, 'p'), (1, 1L, 'p'), (CAST(NULL AS INT), 2L, 'q') AS t(k, v, s)),
        c1 AS (SELECT k, v, s FROM c0),
        c2 AS (SELECT k, v, s FROM c1 WHERE k IN (SELECT k FROM c1 WHERE 1 = 0)),
        c3 AS (SELECT DISTINCT a.k, b.v, a.s FROM c2 a JOIN c0 b ON a.k = b.k
               ORDER BY k NULLS FIRST, v NULLS FIRST, s NULLS FIRST LIMIT 12),
        c4 AS (SELECT k, v, s FROM c3 WHERE k IN (SELECT k FROM c3 WHERE k % 2 = 0))
        SELECT a.k, a.v, a.s, (SELECT sum(x.v) FROM (SELECT k, v, s FROM c4 ORDER BY s) x WHERE x.k <=> a.k) AS q0
        FROM (SELECT DISTINCT * FROM c4) a
        WHERE EXISTS (SELECT 1 FROM (SELECT k, sum(v) AS v, min(s) AS s FROM c2 GROUP BY k) x
                      WHERE x.k <=> a.k AND x.s <> a.s)
    """
    assert spark.sql(query).collect() == []


def test_schema_of_nested_ctes_is_analyzed_once_for_each_cte(spark):
    """The schema of a DataFrame is analyzed without executing it, also in a session
    with temporary views and changed configuration."""
    spark.sql("CREATE OR REPLACE TEMP VIEW shared_cte_schema_view AS SELECT 1 AS c0")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")
    try:
        query = nested(12 if is_jvm_spark() else 28)
        assert [field.name for field in spark.sql(query).schema.fields] == ["count(1)"]
        if not is_jvm_spark():
            assert spark.sql(query).collect() == [(10,)]
            assert len(plan(spark, query).splitlines()) < 300  # noqa: PLR2004
    finally:
        spark.conf.unset("spark.sql.session.timeZone")
        spark.catalog.dropTempView("shared_cte_schema_view")


def test_independent_ctes_with_different_filters_are_inlined(spark):
    query = f"""
        WITH {CTE}, d AS (SELECT id % 5 AS k, sum(id) AS s FROM range(100) GROUP BY 1)
        SELECT (SELECT sum(n) FROM c WHERE k < 2) + (SELECT sum(n) FROM c WHERE k > 4),
               (SELECT sum(s) FROM d WHERE k < 1) + (SELECT sum(s) FROM d WHERE k > 3)
    """  # noqa: S608
    check(spark, query, [(571, 1980)], shared=False, scans=4)


def test_ctes_in_different_sharing_states(spark):
    """The references to `a` partly share, those to `b` read different rows, and `c` is shared."""
    query = """
        WITH d AS (SELECT * FROM VALUES (1, 10L, 'a'), (1, 11L, 'b'), (2, 20L, 'a'), (3, 30L, 'c') AS x(k, v, s)),
        a AS (SELECT x.k, x.v, y.s FROM d x JOIN d y ON x.v = y.v),
        b AS (SELECT k, count(*) c FROM d GROUP BY k),
        c AS (SELECT s, sum(v) t FROM d GROUP BY s)
        SELECT k, v FROM a WHERE k = 1 UNION ALL SELECT k, v FROM a WHERE k = 1
        UNION ALL SELECT k, v FROM a WHERE k = 3
        UNION ALL SELECT k, c FROM b UNION ALL SELECT k, c FROM b WHERE c > 1
        UNION ALL SELECT x.t, y.t FROM c x, c y WHERE x.t = y.t
    """
    expected = [(1, 2), (1, 2), (1, 10), (1, 10), (1, 11), (1, 11), (2, 1), (3, 1), (3, 30)]
    expected += [(11, 11), (30, 30), (30, 30), (30, 30), (30, 30)]
    assert sorted(tuple(row) for row in spark.sql(query).collect()) == sorted(expected)
    # A repeated reference below a projection of all its columns is replaced by its definition.
    query = f"WITH {CTE} SELECT k, n, m FROM c UNION ALL SELECT k, n, m FROM c WHERE k > 5"  # noqa: S608
    expected = [(k, 143, 994 + k) for k in range(6)] + [(6, 142, 993), (6, 142, 993)]
    check(spark, query, expected, shared=False, scans=2)


def test_single_reference_is_inlined(spark):
    query = f"WITH {CTE} SELECT sum(n) FROM c"  # noqa: S608
    check(spark, query, [(1000,)], shared=False, scans=1)


def test_predicate_subquery_references_share_one_evaluation(spark):
    query = f"WITH {CTE} SELECT sum(n) FROM c a WHERE EXISTS (SELECT 1 FROM c b WHERE a.k = b.k AND b.n > 142)"  # noqa: S608
    check(spark, query, [(858,)], shared=False, scans=2)
    query = f"WITH {CTE} SELECT sum(n) FROM c a WHERE k IN (SELECT k FROM c)"  # noqa: S608
    check(spark, query, [(1000,)], shared=True, scans=1)


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "WITH c AS (SELECT id FROM range(4) LIMIT 1 OFFSET 1) SELECT EXISTS (SELECT a.id FROM c b) FROM c a",
            [(True,)],
        ),
        (
            "WITH c AS (SELECT id FROM range(4) OFFSET 1) SELECT EXISTS (SELECT a.id FROM c b) FROM c a",
            [(True,)] * 3,
        ),
        (
            "WITH c AS (SELECT id FROM range(4) LIMIT 1 OFFSET 1) "
            "SELECT EXISTS (SELECT 1 FROM c b WHERE b.id = a.id) FROM c a",
            [(True,)],
        ),
        (
            "WITH c AS (SELECT id FROM range(4) OFFSET 1) "
            "SELECT a.id, EXISTS (SELECT 1 FROM c b WHERE b.id = a.id + 1) FROM c a ORDER BY a.id",
            [(1, True), (2, True), (3, False)],
        ),
        (
            "WITH c AS (SELECT max(id) AS m FROM range(4)) SELECT EXISTS (SELECT 1 FROM c b WHERE b.m = a.m) FROM c a",
            [(True,)],
        ),
        (
            "WITH c AS (SELECT max(id) AS m FROM range(4)) SELECT EXISTS (SELECT 1 FROM c b WHERE b.m <> a.m) FROM c a",
            [(False,)],
        ),
        (
            "WITH c AS (SELECT max(id) AS m FROM range(4)) "
            "SELECT r.id, EXISTS (SELECT 1 FROM c b WHERE b.m = r.id) FROM range(5) r, c ORDER BY r.id",
            [(0, False), (1, False), (2, False), (3, True), (4, False)],
        ),
    ],
    ids=[
        "limit-offset",
        "offset",
        "offset-correlated-filter",
        "offset-correlated-filter-mismatch",
        "aggregate-correlated-filter",
        "aggregate-correlated-filter-mismatch",
        "aggregate-other-outer-query",
    ],
)
def test_correlated_exists_over_cte_reference(spark, query, expected):
    """The CTE reference in the EXISTS subquery is read as a relation, whose definition
    does not refer to the outer query."""
    assert [tuple(r) for r in spark.sql(query).collect()] == expected
