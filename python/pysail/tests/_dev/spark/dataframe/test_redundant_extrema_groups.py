import re

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark


def _optimized_plan(spark, query):
    plan = spark.sql(f"EXPLAIN EXTENDED {query}").first()[0]
    return plan.split("== Optimized Logical Plan ==", 1)[1].split("== Physical Plan ==", 1)[0]


@pytest.mark.parametrize(
    ("inner", "outer", "expected"),
    [
        (
            "SELECT g,x FROM source GROUP BY g,x",
            "SELECT g,min(x),max(x) FROM ({inner}) d GROUP BY g ORDER BY g",
            [(1, -2, 3), (2, None, None), (3, 9, 9)],
        ),
        (
            "SELECT g,x+1 AS y FROM source GROUP BY g,x+1",
            "SELECT g,min(y),max(y) FROM ({inner}) d GROUP BY g ORDER BY g",
            [(1, -1, 4), (2, None, None), (3, 10, 10)],
        ),
        ("SELECT g,x FROM source GROUP BY g,x", "SELECT min(x),max(x) FROM ({inner}) d", [(-2, 9)]),
        ("SELECT x FROM source WHERE false GROUP BY x", "SELECT min(x),max(x) FROM ({inner}) d", [(None, None)]),
        (
            "SELECT x FROM source WHERE g=2 GROUP BY x",
            "SELECT min(DISTINCT x),max(DISTINCT x) FROM ({inner}) d",
            [(None, None)],
        ),
    ],
)
def test_min_max_remove_duplicate_only_inner_groups(spark, inner, outer, expected):
    query = "WITH source AS (SELECT * FROM VALUES (1,3),(1,3),(1,-2),(1,NULL),(2,NULL),(2,NULL),(3,9) AS t(g,x)) "
    query += outer.format(inner=inner)
    assert [tuple(row) for row in spark.sql(query).collect()] == expected
    if not is_jvm_spark():
        plan = _optimized_plan(spark, query)
        # Empty input may simplify the entire lower plan; no redundant group remains.
        assert len(re.findall(r"^\s*Aggregate:", plan, re.MULTILINE)) <= 1


@pytest.mark.parametrize(
    ("inner", "outer", "expected"),
    [
        ("SELECT x FROM source GROUP BY x", "SELECT min(x),sum(x) FROM ({inner}) d", [(-2, 10)]),
        ("SELECT x FROM source GROUP BY x", "SELECT min(x),count(*) FROM ({inner}) d", [(-2, 4)]),
        ("SELECT x FROM source GROUP BY x HAVING count(*)>1", "SELECT min(x),max(x) FROM ({inner}) d", [(3, 3)]),
        ("SELECT count(*) AS n FROM source WHERE false", "SELECT min(n),max(n) FROM ({inner}) d", [(0, 0)]),
    ],
)
def test_min_max_keep_semantically_required_inner_groups(spark, inner, outer, expected):
    query = "WITH source AS (SELECT * FROM VALUES (3),(3),(-2),(NULL),(NULL),(9) AS t(x)) "
    query += outer.format(inner=inner)
    assert [tuple(row) for row in spark.sql(query).collect()] == expected
    if not is_jvm_spark() and "WHERE false" not in inner:
        assert len(re.findall(r"^\s*Aggregate:", _optimized_plan(spark, query), re.MULTILINE)) == 2  # noqa: PLR2004


@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific conservative float-group representation guard")
def test_min_max_keep_float_grouping_zero_representation(spark):
    query = """
        SELECT CAST(min(k) AS STRING),CAST(max(k) AS STRING)
        FROM (SELECT k FROM VALUES (CAST('-0.0' AS FLOAT)),(CAST('0.0' AS FLOAT)) AS t(k) GROUP BY k) d
    """
    # The lower group normalizes zero keys; dropping it could expose a negative raw zero.
    assert [tuple(row) for row in spark.sql(query).collect()] == [("0.0", "0.0")]
    assert len(re.findall(r"^\s*Aggregate:", _optimized_plan(spark, query), re.MULTILINE)) == 2  # noqa: PLR2004


@pytest.mark.skipif(
    is_jvm_spark(), reason="Preserve Sail's existing evaluation of unused DISTINCT grouping expressions"
)
def test_min_max_keep_errors_from_unused_group_columns(spark):
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    query = """
        SELECT min(a) FROM (
          SELECT DISTINCT a,CAST(b AS INT) AS bad
          FROM VALUES (1,'bad'),(1,'2') AS t(a,b) {where}
        ) d
    """
    try:
        with pytest.raises(Exception, match=r"(?i)(cast|invalid)"):
            spark.sql(query.format(where="")).collect()
        assert [tuple(row) for row in spark.sql(query.format(where="WHERE b='2'")).collect()] == [(1,)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)


def test_min_max_prune_only_unused_csv_columns(spark, tmp_path):
    path = tmp_path / "malformed.csv"
    path.write_text("a,b\n1,bad\n1,2\n")
    spark.read.option("header", "true").option("mode", "FAILFAST").schema("a INT,b INT").csv(
        str(path)
    ).createOrReplaceTempView("optimizer_csv_keys")
    query = "SELECT {projection} FROM (SELECT DISTINCT a,b FROM optimizer_csv_keys) d"
    try:
        minimum = query.format(projection="min(a)")
        assert [tuple(row) for row in spark.sql(minimum).collect()] == [(1,)]
        if not is_jvm_spark():
            assert len(re.findall(r"^\s*Aggregate:", _optimized_plan(spark, minimum), re.MULTILINE)) == 1
        for projection, groups in [("min(b)", 1), ("min(a),count(*)", 2)]:
            required = query.format(projection=projection)
            with pytest.raises(Exception, match=r"(?i)(pars|malformed)"):
                spark.sql(required).collect()
            if not is_jvm_spark():
                assert len(re.findall(r"^\s*Aggregate:", _optimized_plan(spark, required), re.MULTILINE)) == groups
    finally:
        spark.catalog.dropTempView("optimizer_csv_keys")
