"""CTEs that are referenced more than once.

Spark inlines a deterministic CTE at every reference. Sail evaluates the CTE once for
the references that read the same rows of it, and inlines it for the other references.
These tests check the results on every engine, and on Sail they also check the plans.

The later tests use CTEs at sizes where the references cannot all be served from a small
buffer, and references that stop early or never read.

Spark evaluates a non-deterministic CTE once for all of its references, and inlines a
deterministic CTE into each reference (InlineCTE). Sail may evaluate any CTE once for
several references, but the results must be those of an engine that evaluates a
deterministic CTE per reference: every reference reads all rows of one evaluation, a
volatile CTE is evaluated once, and a query fails only if it reads a row that fails. In
local mode, a reference that is never read does not evaluate the CTE. In cluster mode,
the stage of a shared CTE starts when it is scheduled, as every other stage does.

Spark materializes a non-deterministic CTE in full in a shuffle before any reference
reads it, so it fails on a row that no reference reads (for example under a LIMIT, or
when every reference is the probe side of an empty join). Sail fails only when a
reference reads the row, as it does when the CTE is evaluated for each reference.
"""

import os

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


def nested(depth):
    ctes = ["c0 AS (SELECT id FROM range(10))"]
    ctes.extend(
        f"c{i} AS (SELECT a.id FROM c{i - 1} a JOIN c{i - 1} b ON a.id = b.id)"  # noqa: S608
        for i in range(1, depth + 1)
    )
    return f"WITH {', '.join(ctes)} SELECT count(*) FROM c{depth}"  # noqa: S608


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
    with pytest.raises(PySparkException, match=r"(?i)divi"):
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


# CTEs larger than a small buffer, and references that stop early or never read.

LOCAL_CLUSTER = os.environ.get("SAIL_MODE") == "local-cluster"
DIVISION_BY_ZERO = "(?i)division by zero"

# Wide rows, so that one evaluation of a CTE is about 50 MB of Arrow data.
ROWS = 400_000
PAD = 120
SOURCE = f"range(0, {ROWS}, 1, 4)"
WIDE = f"repeat('x', {PAD})"


@pytest.fixture
def counted(spark, tmp_path):
    """Registers `counted(x)`, a deterministic Python UDF that returns `x` and logs each call,
    and returns a function that reads the number of calls."""
    from pyspark.sql.functions import udf

    counter = tmp_path / "calls.txt"

    def log(x):
        with open(counter, "a") as f:
            f.write("x")
        return x

    spark.udf.register("counted", udf(log, "bigint"))
    return lambda: counter.stat().st_size if counter.exists() else 0


@pytest.fixture
def ansi(spark):
    spark.conf.set("spark.sql.ansi.enabled", "true")
    yield
    spark.conf.unset("spark.sql.ansi.enabled")


def rows(spark, query):
    return [tuple(row) for row in spark.sql(query).collect()]


def volatile(function="rand()"):
    return f"c AS (SELECT id, {function} AS r, {WIDE} AS pad FROM {SOURCE})"  # noqa: S608


# Volatile CTEs: every reference reads the same evaluation, however the references interleave.

VOLATILE_SHAPES = {
    # A hash join reads one side completely before it reads the other side.
    "self-join": (
        "SELECT count(*), count_if(a.r = b.r), sum(length(a.pad) + length(b.pad)) FROM c a JOIN c b ON a.id = b.id",
        (ROWS, ROWS, 2 * PAD * ROWS),
    ),
    "union-all": (
        "SELECT count(*), sum(length(pad)) FROM (SELECT id, r, count(*) AS k, max(pad) AS pad "
        "FROM (SELECT id, r, pad FROM c UNION ALL SELECT id, r, pad FROM c) GROUP BY id, r) WHERE k = 2",
        (ROWS, PAD * ROWS),
    ),
    "three-references": (
        "SELECT count(*) FROM c a JOIN c b ON a.id = b.id AND a.r = b.r JOIN c d ON a.id = d.id AND a.r = d.r",
        (ROWS,),
    ),
    "scalar-subquery": (
        "SELECT count(*), count_if(a.r = b.r), max(a.r) = (SELECT max(r) FROM c) FROM c a JOIN c b ON a.id = b.id",
        (ROWS, ROWS, True),
    ),
    "exists": (
        "SELECT count(*) FROM c x WHERE EXISTS (SELECT 1 FROM c y WHERE y.id = x.id AND y.r = x.r)",
        (ROWS,),
    ),
    "different-filters": (
        "SELECT count(*) FROM (SELECT * FROM c WHERE id % 2 = 0) x JOIN (SELECT * FROM c WHERE id % 3 = 0) y "
        "ON x.id = y.id AND x.r = y.r",
        ((ROWS + 5) // 6,),
    ),
    # The other reference stops after 10 rows, and this one still reads every row.
    "limit-and-full": (
        "SELECT count(*), count_if(x.r = y.r), sum(length(y.pad)) FROM (SELECT * FROM c LIMIT 10) x "
        "JOIN c y ON x.id = y.id",
        (10, 10, 10 * PAD),
    ),
    "sorted-limit-and-full": (
        "SELECT count(*), count_if(x.r = y.r) FROM (SELECT * FROM c ORDER BY id LIMIT 10) x JOIN c y ON x.id = y.id",
        (10, 10),
    ),
    # Two LIMITs without ORDER BY may read different rows, but a row read by both has one value.
    "two-limits": (
        "SELECT count_if(x.r <> y.r) FROM (SELECT * FROM c LIMIT 50) x JOIN (SELECT * FROM c LIMIT 50) y ON x.id = y.id",
        (0,),
    ),
    "nested": (
        ", d AS (SELECT a.id, a.r FROM c a JOIN c b ON a.id = b.id AND a.r = b.r) "
        "SELECT count(*) FROM d x JOIN d y ON x.id = y.id AND x.r = y.r",
        (ROWS,),
    ),
}


@pytest.mark.parametrize("shape", list(VOLATILE_SHAPES))
@pytest.mark.parametrize("function", ["rand()", "uuid()"])
def test_references_to_large_volatile_cte_read_one_evaluation(spark, function, shape):
    suffix, expected = VOLATILE_SHAPES[shape]
    query = f"WITH {volatile(function)}" + ("" if suffix.startswith(",") else " ") + suffix
    assert rows(spark, query) == [expected]


def test_partition_wise_join_of_volatile_cte(spark):
    """The aggregate partitions both references by the join key, so the join can read the
    references partition by partition."""
    query = (
        "WITH c AS (SELECT id % 100000 AS k, sum(r) AS s FROM (SELECT id, rand() AS r FROM range(0, 2000000, 1, 8)) "
        "GROUP BY 1) SELECT count(*), count_if(a.s = b.s) FROM c a JOIN c b ON a.k = b.k"
    )
    assert rows(spark, query) == [(100000, 100000)]


def test_deterministic_cte_over_volatile_cte(spark):
    """`d` is deterministic but reads the volatile `c`: both references of `d` and the direct
    references of `c` read one evaluation of `c`."""
    query = (
        f"WITH {volatile()}, d AS (SELECT id % 1000 AS g, sum(r) AS s, max(pad) AS pad FROM c GROUP BY 1) "  # noqa: S608
        "SELECT count(*), count_if(abs(a.s - b.s) < 1e-9), sum(length(a.pad) + length(b.pad)), "
        "abs((SELECT sum(r) FROM c) - (SELECT sum(s) FROM d)) < 1e-6 FROM d a JOIN d b ON a.g = b.g"
    )
    assert rows(spark, query) == [(1000, 1000, 2 * PAD * 1000, True)]


# Deterministic CTEs: the results of an engine that evaluates the CTE for each reference.


def test_deterministic_cte_read_by_both_sides_of_a_join(spark):
    query = (
        f"WITH c AS (SELECT id % 200000 AS k, count(*) AS n, max({WIDE}) AS pad FROM {SOURCE} GROUP BY 1) "  # noqa: S608
        "SELECT count(*), sum(a.n * b.n), sum(length(a.pad) + length(b.pad)) FROM c a JOIN c b ON a.k = b.k"
    )
    assert rows(spark, query) == [(200000, 800000, 2 * PAD * 200000)]


def test_deterministic_cte_read_by_union_branches_and_a_scalar_subquery(spark):
    query = (
        f"WITH c AS (SELECT id % 200000 AS k, count(*) AS n, max({WIDE}) AS pad FROM {SOURCE} GROUP BY 1) "  # noqa: S608
        "SELECT count(*), sum(n), sum(length(pad)), (SELECT max(k) FROM c) FROM (SELECT * FROM c UNION ALL SELECT * FROM c)"
    )
    assert rows(spark, query) == [(400000, 2 * ROWS, 2 * PAD * 200000, 199999)]


def test_partition_wise_join_of_deterministic_cte(spark):
    query = (
        "WITH c AS (SELECT id % 100000 AS k, sum(id) AS s FROM range(0, 2000000, 1, 8) GROUP BY 1) "
        "SELECT count(*), sum(a.s), sum(b.s) FROM c a JOIN c b ON a.k = b.k"
    )
    assert rows(spark, query) == [(100000, 1999999000000, 1999999000000)]


def test_nested_chain_of_deterministic_ctes(spark):
    ctes = ["c0 AS (SELECT id % 5000 AS k, id AS v FROM range(0, 20000, 1, 4))"]
    ctes.extend(
        f"c{i} AS (SELECT a.k, a.v FROM c{i - 1} a JOIN (SELECT DISTINCT k FROM c{i - 1}) b ON a.k = b.k)"  # noqa: S608
        for i in range(1, 6)
    )
    query = f"WITH {', '.join(ctes)} SELECT count(*), sum(v) FROM c5"  # noqa: S608
    assert rows(spark, query) == [(20000, 199990000)]


def test_each_reference_reads_all_rows_of_one_evaluation(spark):
    """A LIMIT without ORDER BY can return different rows in each evaluation, and a
    repartitioned CTE can assign rows to different partitions in each evaluation. Each
    reference must still read all rows of one evaluation, with no row missing or repeated."""
    body = f"SELECT k, max({WIDE}) AS pad FROM (SELECT id % 300000 AS k FROM {SOURCE}) GROUP BY k"  # noqa: S608
    for cte, n in [
        (f"c AS ({body} LIMIT 250000)", 250000),
        (f"c AS (SELECT /*+ REPARTITION(7) */ * FROM ({body}))", 300000),  # noqa: S608
    ]:
        query = (
            f"WITH {cte} SELECT count(a.k), count(DISTINCT a.k), count(b.k), count(DISTINCT b.k), "  # noqa: S608
            "sum(length(a.pad)) FROM c a FULL JOIN c b ON a.k = b.k"
        )
        assert rows(spark, query) == [(n, n, n, n, PAD * n)], cte


def test_early_stopping_consumers_over_deterministic_cte(spark):
    body = "c AS (SELECT id % 100000 AS k, count(*) AS n FROM range(0, 2000000, 1, 8) GROUP BY 1)"
    query = f"SELECT count(*) FROM (WITH {body} SELECT a.k FROM c a JOIN c b ON a.k = b.k LIMIT 1)"  # noqa: S608
    assert rows(spark, query) == [(1,)]
    query = (
        f"WITH {body} SELECT count(*), sum(n) FROM "  # noqa: S608
        "(SELECT * FROM (SELECT * FROM c LIMIT 7) UNION ALL SELECT * FROM (SELECT * FROM c LIMIT 5))"
    )
    assert rows(spark, query) == [(12, 240)]


def test_limit_reference_joined_with_full_reference_of_deterministic_cte(spark):
    """The LIMIT reference returns 10 rows in total, not 10 rows per partition or per attempt."""
    query = (
        f"WITH c AS (SELECT id % {ROWS} AS id, max({WIDE}) AS pad FROM {SOURCE} GROUP BY 1) "  # noqa: S608
        "SELECT count(*), sum(length(y.pad)) FROM (SELECT * FROM c LIMIT 10) x JOIN c y ON x.id = y.id"
    )
    assert rows(spark, query) == [(10, 10 * PAD)]


# LIMIT over a volatile CTE.


def test_limits_over_volatile_cte_read_one_evaluation(spark, counted):
    query = (
        f"WITH c AS (SELECT id, counted(id) AS v, rand() AS r FROM {SOURCE}) "  # noqa: S608
        "SELECT count_if(x.r <> y.r), count(*) <= 10, count_if(x.v = y.v) = count(*) "
        "FROM (SELECT * FROM c LIMIT 10) x JOIN (SELECT * FROM c LIMIT 10) y ON x.id = y.id"
    )
    assert rows(spark, query) == [(0, True, True)]
    if not is_jvm_spark() and not LOCAL_CLUSTER:
        # Spark evaluates the whole CTE before the references read it. Sail local mode stops
        # evaluating it once every reference has stopped reading. In cluster mode, the stage
        # of the CTE runs ahead of its references, as every other stage does.
        assert counted() < ROWS // 4


def test_limit_and_exists_over_volatile_cte(spark, counted):  # noqa: ARG001
    query = (
        f"WITH c AS (SELECT id, counted(id) AS v, rand() AS r FROM {SOURCE}) "  # noqa: S608
        "SELECT count(*), count(x.v) FROM (SELECT * FROM c LIMIT 10) x "
        "WHERE EXISTS (SELECT 1 FROM c y WHERE y.id = x.id AND y.r = x.r)"
    )
    assert rows(spark, query) == [(10, 10)]


# A reference that is never read does not evaluate the CTE.

EMPTY = "(SELECT id AS k FROM range(100) WHERE id * 7 % 1000 = 999)"

UNREAD = {
    "join-body": "c AS (SELECT a.id AS k, counted(a.id) AS v FROM range(0, 1000, 1, 2) a "
    "JOIN range(0, 10) b ON a.id % 10 = b.id)",
    "union-body": "c AS (SELECT k, counted(k) AS v FROM (SELECT id AS k FROM range(0, 1000, 1, 2) "
    "UNION ALL SELECT id FROM range(0, 1)))",
    "aggregate-body": "c AS (SELECT id % 100 AS k, sum(counted(id)) AS v FROM range(0, 1000, 1, 2) GROUP BY 1)",
    "volatile-body": "c AS (SELECT a.id AS k, counted(a.id) + rand() AS v FROM range(0, 1000, 1, 2) a "
    "JOIN range(0, 10) b ON a.id % 10 = b.id)",
}


@pytest.mark.parametrize("cte", list(UNREAD))
def test_cte_is_not_evaluated_when_the_other_join_side_is_empty(spark, counted, cte):
    query = (
        f"WITH {UNREAD[cte]} SELECT count(*), sum(x.v + y.v + e.k) FROM {EMPTY} e "  # noqa: S608
        "JOIN c x ON e.k = x.k JOIN c y ON x.k = y.k"
    )
    assert rows(spark, query) == [(0, None)]
    if not is_jvm_spark() and not LOCAL_CLUSTER:
        # Spark may evaluate a CTE in a stage of its own before it finds the other side empty.
        # In cluster mode, the stage of the CTE starts when it is scheduled, as every other
        # stage does, before the other side is found empty.
        assert counted() == 0


# Errors: a query fails if and only if a reference reads a row that fails.

FAILS_AT_3 = "100 DIV (id - 3)"
VOLATILE_FALLIBLE = f"c AS (SELECT id AS k, id AS v, rand() AS u, {FAILS_AT_3} AS f FROM range(0, 2000, 1, 3))"  # noqa: S608
DETERMINISTIC_FALLIBLE = (
    "c AS (SELECT k, max(v) AS v, 100 DIV (k - 3) AS f FROM (SELECT id AS k, id AS v FROM range(0, 2000, 1, 3)) "
    "GROUP BY k)"
)
EXPECTED_SUM_F = sum(100 // (k - 3) if (k - 3) > 0 else -(100 // (3 - k)) for k in range(2000) if k != 3)  # noqa: PLR2004


@pytest.mark.parametrize("cte", [VOLATILE_FALLIBLE, DETERMINISTIC_FALLIBLE], ids=["volatile", "deterministic"])
def test_failing_row_filtered_out_by_the_reference_that_reads_the_column(spark, ansi, cte):  # noqa: ARG001
    """Only `a` filters out the failing row, and `b` does not read the failing column."""
    query = f"WITH {cte} SELECT count(*), sum(a.f) FROM (SELECT * FROM c WHERE k <> 3) a JOIN c b ON a.k = b.k"  # noqa: S608
    assert rows(spark, query) == [(1999, EXPECTED_SUM_F)]


@pytest.mark.parametrize("cte", [VOLATILE_FALLIBLE, DETERMINISTIC_FALLIBLE], ids=["volatile", "deterministic"])
def test_failing_row_filtered_out_by_the_union_branch_that_reads_the_column(spark, ansi, cte):  # noqa: ARG001
    """Only the first branch reads the failing column, and it filters out the failing row.
    Spark materializes a volatile CTE with the failing column for every row, because the
    second branch has no filter to push into it, so it fails; Sail computes the column only
    for the rows that the first branch keeps, as it does when the CTE is evaluated for each
    reference."""
    query = f"WITH {cte} SELECT count(*), sum(f) FROM (SELECT k, f FROM c WHERE k <> 3 UNION ALL SELECT k, 0 FROM c)"  # noqa: S608
    if is_jvm_spark() and cte == VOLATILE_FALLIBLE:
        with pytest.raises(Exception, match="DIVIDE_BY_ZERO"):
            spark.sql(query).collect()
    else:
        assert rows(spark, query) == [(3999, EXPECTED_SUM_F)]


@pytest.mark.parametrize("cte", [VOLATILE_FALLIBLE, DETERMINISTIC_FALLIBLE], ids=["volatile", "deterministic"])
def test_failing_column_that_no_reference_reads(spark, ansi, cte):  # noqa: ARG001
    query = f"WITH {cte} SELECT count(*), sum(a.v) FROM c a JOIN c b ON a.k = b.k"  # noqa: S608
    assert rows(spark, query) == [(2000, 1999000)]


@pytest.mark.parametrize("volatile", [False, True])
def test_failing_row_filtered_out_by_every_reference(spark, ansi, volatile):  # noqa: ARG001
    if volatile:
        cte = "c AS (SELECT id AS k, rand() AS r, 10 DIV (id - 5) AS q FROM range(0, 1000, 1, 4))"
        on, expected = "a.k = b.k AND a.r = b.r", [(999, sum(2 * int(10 / (k - 5)) for k in range(1000) if k != 5))]  # noqa: PLR2004
    else:
        cte = "c AS (SELECT k, 10 DIV (k - 5) AS q FROM (SELECT id % 10 AS k FROM range(0, 1000, 1, 4)) GROUP BY k)"
        on, expected = "a.k = b.k", [(9, -4)]
    query = f"WITH {cte} SELECT count(*), sum(a.q + b.q) FROM c a JOIN c b ON {on} WHERE a.k <> 5 AND b.k <> 5"  # noqa: S608
    assert rows(spark, query) == expected


def test_limits_over_volatile_cte_do_not_read_a_failing_row(spark, ansi):  # noqa: ARG001
    """The last row of the CTE fails, and the references read 5 rows each. The two references
    may read different rows, but a row that both read has one value."""
    body = "WITH c AS (SELECT id, rand() AS r, 10 DIV (id - 199999) AS q FROM range(0, 200000, 1, 1)) "
    union = (
        f"{body} SELECT count(*), count(q), max(id) < 199999 FROM "  # noqa: S608
        "(SELECT * FROM (SELECT * FROM c LIMIT 5) UNION ALL SELECT * FROM (SELECT * FROM c LIMIT 5))"
    )
    join = (
        f"{body} SELECT count(*) <= 5, count_if(x.r = y.r AND x.q = y.q) = count(*) FROM (SELECT * FROM c LIMIT 5) x "  # noqa: S608
        "JOIN (SELECT * FROM c LIMIT 5) y ON x.id = y.id"
    )
    if is_jvm_spark():
        for query in [union, join]:
            with pytest.raises(Exception, match="DIVIDE_BY_ZERO"):
                spark.sql(query).collect()
    else:
        assert rows(spark, union) == [(10, 10, True)]
        assert rows(spark, join) == [(True, True)]


def test_sorted_limits_over_deterministic_cte_do_not_compute_a_failing_column(spark, ansi):  # noqa: ARG001
    """The failing column is computed for the last key only, and each reference keeps the
    first 5 keys. Spark computes the column in the aggregate before the sort, so it fails;
    Sail computes it after the limit, as it does when the CTE is evaluated for each reference."""
    query = (
        "WITH c AS (SELECT k, 10 DIV (k - 199999) AS q FROM (SELECT id AS k FROM range(0, 200000, 1, 1)) GROUP BY k) "
        "SELECT count(*), count(x.q) FROM (SELECT * FROM c ORDER BY k LIMIT 5) x "
        "JOIN (SELECT * FROM c ORDER BY k LIMIT 5) y ON x.k = y.k"
    )
    if is_jvm_spark():
        with pytest.raises(Exception, match="DIVIDE_BY_ZERO"):
            spark.sql(query).collect()
    else:
        assert rows(spark, query) == [(5, 5)]


def test_failing_volatile_cte_whose_references_are_not_read(spark, ansi):  # noqa: ARG001
    """Every reference is under a filter on a scalar subquery that is false at runtime."""
    query = (
        f"WITH {VOLATILE_FALLIBLE} SELECT count(*), sum(a.f) FROM c a JOIN c b ON a.k = b.k "  # noqa: S608
        "WHERE (SELECT count(*) FROM range(0, 5, 1, 1) WHERE rand() < -1) > 0"
    )
    if is_jvm_spark():
        with pytest.raises(Exception, match="DIVIDE_BY_ZERO"):
            spark.sql(query).collect()
    else:
        assert rows(spark, query) == [(0, None)]


def test_failing_deterministic_cte_whose_references_are_not_read(spark, ansi):  # noqa: ARG001
    """Every reference is the probe side of a join whose build side is empty at runtime."""
    query = (
        "WITH c AS (SELECT k, sum(id) AS s, 10 DIV (k - 5) AS q FROM (SELECT id % 10 AS k, id FROM range(0, 1000, 1, 4)) "  # noqa: S608
        f"GROUP BY k) SELECT count(*) FROM {EMPTY} e JOIN c a ON e.k = a.k JOIN c b ON a.k = b.k AND a.q = b.q"
    )
    assert rows(spark, query) == [(0,)]


@pytest.mark.parametrize(
    "query",
    [
        f"WITH {VOLATILE_FALLIBLE} SELECT count(*), sum(a.f + b.f) FROM c a JOIN c b ON a.k = b.k",  # noqa: S608
        f"WITH {DETERMINISTIC_FALLIBLE} SELECT count(*), sum(a.f) FROM c a JOIN c b ON a.k = b.k",  # noqa: S608
        f"WITH {DETERMINISTIC_FALLIBLE} SELECT count(*), sum(CASE WHEN a.k <> 3 THEN a.f END) FROM c a JOIN c b ON a.k = b.k",  # noqa: S608
        f"WITH {DETERMINISTIC_FALLIBLE} SELECT (SELECT sum(CASE WHEN k <> 3 THEN f END) FROM c), (SELECT count(*) FROM c)",  # noqa: S608
        f"WITH {VOLATILE_FALLIBLE} SELECT (SELECT sum(CASE WHEN k <> 3 THEN f END) FROM c), (SELECT count(*) FROM c)",  # noqa: S608
    ],
    ids=["volatile", "deterministic", "guarded-join", "guarded-aggregate", "guarded-volatile"],
)
def test_failing_row_read_by_a_reference(spark, ansi, query):  # noqa: ARG001
    """The failing row reaches a reference. The CASE guards do not help: the CTE computes the
    failing column before the reference evaluates the guard."""
    with pytest.raises(Exception, match=DIVISION_BY_ZERO):
        spark.sql(query).collect()
