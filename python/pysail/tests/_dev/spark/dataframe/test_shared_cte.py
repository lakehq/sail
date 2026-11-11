"""CTEs that are referenced more than once.

Spark inlines a deterministic CTE at every reference. Sail evaluates the CTE once for
the references that read the same rows of it, and inlines it for the other references.
These tests check the results on every engine, and on Sail they also check the plans.
"""

import pytest
from pyspark.errors import PySparkException

from pysail.testing.spark.utils.common import is_jvm_spark

CTE = "c AS (SELECT id % 7 AS k, count(*) AS n, max(id) AS m FROM range(1000) GROUP BY 1)"


def plan(spark, query):
    return spark.sql(f"EXPLAIN {query}").collect()[0][0]


def check(spark, query, expected, *, shared, scans):
    """Checks the result, and on Sail whether a CTE is shared and how often the
    range is scanned."""
    assert sorted(tuple(row) for row in spark.sql(query).collect()) == sorted(expected)
    if not is_jvm_spark():
        text = plan(spark, query)
        assert ("SharedCteExec" in text) == shared, text
        assert text.count("RangeExec") == scans, text


def nested(depth):
    ctes = ["c0 AS (SELECT id FROM range(10))"]
    ctes.extend(
        f"c{i} AS (SELECT a.id FROM c{i - 1} a JOIN c{i - 1} b ON a.id = b.id)" for i in range(1, depth + 1)
    )
    return f"WITH {', '.join(ctes)} SELECT count(*) FROM c{depth}"


@pytest.mark.parametrize("depth", [12, 16, 24])
def test_nested_cte_plan_is_linear_in_depth(spark, depth):
    query = nested(depth)
    if is_jvm_spark():
        # Spark plans 2^depth copies of the first CTE.
        if depth > 8:  # noqa: PLR2004
            pytest.skip("the plan is exponential in the depth")
    else:
        text = plan(spark, query)
        assert len(text.splitlines()) < 10 * depth + 20  # noqa: PLR2004
        assert text.count("RangeExec") == 1
    assert spark.sql(query).collect() == [(10,)]


@pytest.mark.parametrize("references", [2, 3, 4])
def test_join_references_share_one_evaluation(spark, references):
    joins = " ".join(f"JOIN c t{i} ON t0.k = t{i}.k" for i in range(1, references))
    total = " + ".join(f"t{i}.n" for i in range(references))
    query = f"WITH {CTE} SELECT sum({total}) FROM c t0 {joins}"
    check(spark, query, [(1000 * references,)], shared=True, scans=1)


@pytest.mark.parametrize("references", [2, 3])
def test_union_references_share_one_evaluation(spark, references):
    branches = " UNION ALL ".join("SELECT n FROM c" for _ in range(references))
    query = f"WITH {CTE} SELECT sum(n) FROM ({branches})"
    check(spark, query, [(1000 * references,)], shared=True, scans=1)


def test_references_with_different_columns_share_one_evaluation(spark):
    query = f"WITH {CTE} SELECT a.k, a.n, b.m FROM c a JOIN c b ON a.k = b.k WHERE a.k = 0"
    check(spark, query, [(0, 143, 994)], shared=True, scans=1)
    query = f"WITH {CTE} SELECT sum(a.n), max(b.m) FROM c a JOIN c b ON a.k = b.k"
    check(spark, query, [(1000, 999)], shared=True, scans=1)


def test_references_with_the_same_filter_share_one_evaluation(spark):
    query = f"WITH {CTE} SELECT k, n FROM c WHERE k < 2 UNION ALL SELECT k, m FROM c WHERE k < 2"
    check(spark, query, [(0, 143), (1, 143), (0, 994), (1, 995)], shared=True, scans=1)


def test_references_with_different_filters_are_inlined(spark):
    query = f"WITH {CTE} SELECT k, n FROM c WHERE k < 2 UNION ALL SELECT k, n FROM c WHERE k > 5"
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
    query = f"WITH {CTE} (SELECT max(n) FROM c) UNION ALL (SELECT max(m) FROM c) LIMIT 2"
    check(spark, query, [(143,), (999,)], shared=True, scans=1)


def test_filter_columns_are_evaluated_by_the_filter_only(spark):
    """The second operand of the filter fails for `id = 3`, where the first one holds."""
    cte = "c AS (SELECT id, 1 / (id - 3) AS r FROM range(0, 6, 1, 2))"
    branch = "(SELECT id FROM c WHERE id >= 0 OR r > 0)"
    query = f"WITH {cte} {branch} UNION ALL {branch}"
    assert len(spark.sql(query).collect()) == 12  # noqa: PLR2004


def test_parameters_in_shared_cte(spark):
    cte = "c AS (SELECT id, id + {n} AS v FROM range(10) WHERE id > {lo})"
    query = f"WITH {cte} SELECT sum(a.v + b.v) FROM c a JOIN c b ON a.id = b.id"
    named = query.format(n=":n", lo=":lo")
    assert spark.sql(named, args={"lo": 3, "n": 100}).collect() == [(1278,)]
    positional = query.format(n="?", lo="?")
    assert spark.sql(positional, args=[100, 3]).collect() == [(1278,)]
    if not is_jvm_spark():
        assert "SharedCteExec" in spark.sql(positional, args=[100, 3])._explain_string()  # noqa: SLF001


def test_input_files_of_shared_cte(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.range(30).repartition(3).write.parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("shared_cte_input_files")
    try:
        df = spark.sql(
            "WITH c AS (SELECT id FROM shared_cte_input_files) SELECT a.id FROM c a JOIN c b ON a.id = b.id"
        )
        assert len(df.inputFiles()) == 3  # noqa: PLR2004
        assert df.count() == 30  # noqa: PLR2004
    finally:
        spark.catalog.dropTempView("shared_cte_input_files")


def test_filtered_rows_are_not_evaluated(spark):
    """The division fails for `id = 2`, which every reference filters out."""
    cte = "c AS (SELECT id, 10 / (id - 2) AS r FROM range(5))"
    query = f"WITH {cte} SELECT a.r, b.r FROM c a JOIN c b ON a.id = b.id WHERE a.id > 2 AND b.id > 2"
    check(spark, query, [(10.0, 10.0), (5.0, 5.0)], shared=True, scans=1)
    # A scalar subquery in the filter is not part of what the references read.
    query = (
        f"WITH {cte} SELECT a.r, b.r FROM c a JOIN c b ON a.id = b.id "
        "WHERE a.id > (SELECT 2) AND b.id > (SELECT 2)"
    )
    assert sorted(tuple(row) for row in spark.sql(query).collect()) == [(5.0, 5.0), (10.0, 10.0)]
    query = f"WITH {cte} SELECT a.r FROM c a JOIN c b ON a.id = b.id"
    with pytest.raises(PySparkException, match="(?i)divi"):
        spark.sql(query).collect()


def test_volatile_cte_is_inlined(spark):
    query = "WITH c AS (SELECT id, rand() AS r FROM range(10)) SELECT count(*) FROM c a JOIN c b ON a.id = b.id"
    check(spark, query, [(10,)], shared=False, scans=2)


def test_sorted_cte_is_inlined(spark):
    query = """
        WITH c AS (SELECT id FROM range(5) ORDER BY id DESC)
        SELECT first(id) FROM c UNION ALL SELECT last(id) FROM c
    """
    check(spark, query, [(4,), (0,)], shared=False, scans=2)


def test_from_and_scalar_subquery_references_share_one_evaluation(spark):
    query = f"WITH {CTE} SELECT k, n FROM c WHERE n = (SELECT max(n) FROM c) AND k > 4"
    check(spark, query, [(5, 143)], shared=False, scans=2)
    query = f"WITH {CTE} SELECT sum(n), (SELECT max(m) FROM c) FROM c"
    check(spark, query, [(1000, 999)], shared=True, scans=1)


def test_nested_cte_references_share_one_evaluation(spark):
    query = f"""
        WITH {CTE},
        d AS (SELECT a.k, a.n + b.n AS t FROM c a JOIN c b ON a.k = b.k)
        SELECT sum(x.t), sum(y.t) FROM d x JOIN d y ON x.k = y.k
    """
    check(spark, query, [(2000, 2000)], shared=True, scans=1)


def test_single_reference_is_inlined(spark):
    query = f"WITH {CTE} SELECT sum(n) FROM c"
    check(spark, query, [(1000,)], shared=False, scans=1)


def test_predicate_subquery_references_share_one_evaluation(spark):
    query = f"WITH {CTE} SELECT sum(n) FROM c a WHERE EXISTS (SELECT 1 FROM c b WHERE a.k = b.k AND b.n > 142)"
    check(spark, query, [(858,)], shared=False, scans=2)
    query = f"WITH {CTE} SELECT sum(n) FROM c a WHERE k IN (SELECT k FROM c)"
    check(spark, query, [(1000,)], shared=True, scans=1)
