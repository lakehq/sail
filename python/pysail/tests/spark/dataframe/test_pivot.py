import pandas as pd
import pytest
from pandas.testing import assert_frame_equal


def test_pivot_infers_values_sorted(spark):
    # `pivot(column)` without an explicit value list infers the distinct pivot
    # values and sorts them ascending (Spark semantics). This path is only
    # reachable via the DataFrame API; SQL `PIVOT` always requires `IN (...)`.
    actual = (
        spark.createDataFrame(
            [
                (2012, "dotNET", 10000),
                (2012, "Java", 20000),
                (2013, "dotNET", 48000),
                (2013, "Java", 30000),
            ],
            schema="year INT, course STRING, earnings INT",
        )
        .groupBy("year")
        .pivot("course")
        .sum("earnings")
        .orderBy("year")
        .toPandas()
    )
    # Inferred values sorted ascending: "Java" before "dotNET".
    assert list(actual.columns) == ["year", "Java", "dotNET"]
    expected = pd.DataFrame(
        {"year": [2012, 2013], "Java": [20000, 30000], "dotNET": [10000, 48000]},
    ).astype({"year": "int32", "Java": "int64", "dotNET": "int64"})
    assert_frame_equal(actual, expected)


def test_pivot_infers_float_values_with_nan_last(spark):
    # Edge case: pivoting on a floating-point column whose inferred values include NaN.
    # NaN is incomparable under `partial_cmp`, so the sort falls back to a deterministic
    # tiebreaker instead of treating NaN as equal to its neighbour (which left the column
    # order dependent on the non-deterministic aggregate row order). This pins that guarantee:
    # finite values come first in ascending order, with their `.0` preserved, and NaN sorts last.
    #
    # One unrelated Spark-parity gap is intentionally NOT asserted here: the `NaN` pivot column
    # counts the `k=NaN` row whereas Spark yields NULL (see the dedicated xfail below).
    actual = (
        spark.createDataFrame(
            [
                ("a", 1.0, 10),
                ("a", 2.0, 20),
                ("a", float("nan"), 30),
                ("b", 1.0, 5),
            ],
            schema="g STRING, k DOUBLE, v INT",
        )
        .groupBy("g")
        .pivot("k")
        .sum("v")
        .orderBy("g")
        .toPandas()
    )
    assert list(actual.columns) == ["g", "1.0", "2.0", "NaN"]
    # Sanity: the first finite column (k=1.0) holds its sums for both groups.
    assert actual.set_index("g")["1.0"].to_dict() == {"a": 10, "b": 5}


def test_pivot_allows_duplicate_values(spark):
    # Spark permits duplicate pivot values, producing duplicate output column names
    # without an analysis error (the error only surfaces if such an ambiguous column
    # is later referenced). Sail matches this behavior.
    actual = spark.sql("""
        SELECT * FROM (
          SELECT year, course, earnings FROM VALUES
            (2012, 'dotNET', 10000),
            (2012, 'Java', 20000),
            (2013, 'dotNET', 48000),
            (2013, 'Java', 30000)
          AS s(year, course, earnings)
        ) PIVOT (sum(earnings) FOR (course) IN ('Java', 'Java'))
        ORDER BY year
    """).toPandas()
    assert list(actual.columns) == ["year", "Java", "Java"]
    assert actual.values.tolist() == [[2012, 20000, 20000], [2013, 30000, 30000]]


def test_pivot_allows_duplicate_aliases(spark):
    # Duplicate pivot value aliases also produce duplicate output columns (no error).
    actual = spark.sql("""
        SELECT * FROM (
          SELECT year, course, earnings FROM VALUES
            (2012, 'dotNET', 10000),
            (2012, 'Java', 20000),
            (2013, 'dotNET', 48000),
            (2013, 'Java', 30000)
          AS s(year, course, earnings)
        ) PIVOT (sum(earnings) FOR (course) IN ('Java' AS x, 'dotNET' AS x))
        ORDER BY year
    """).toPandas()
    assert list(actual.columns) == ["year", "x", "x"]
    assert actual.values.tolist() == [[2012, 20000, 10000], [2013, 30000, 48000]]


def test_pivot_multi_column_struct(spark):
    actual = spark.sql("""
        SELECT * FROM (
          SELECT year, course, training, earnings FROM VALUES
            (2012, 'Java', 'Dummies', 20000),
            (2012, 'dotNET', 'Experts', 10000),
            (2013, 'Java', 'Dummies', 30000),
            (2013, 'dotNET', 'Experts', 48000)
          AS s(year, course, training, earnings)
        ) PIVOT (sum(earnings) FOR (course, training)
                 IN (('Java', 'Dummies'), ('dotNET', 'Experts')))
        ORDER BY year
    """).toPandas()
    assert list(actual.columns) == ["year", "{Java, Dummies}", "{dotNET, Experts}"]
    assert actual.values.tolist() == [[2012, 20000, 10000], [2013, 30000, 48000]]


def test_pivot_multi_column_struct_alias(spark):
    actual = spark.sql("""
        SELECT * FROM (
          SELECT year, course, training, earnings FROM VALUES
            (2012, 'Java', 'Dummies', 20000),
            (2012, 'dotNET', 'Experts', 10000),
            (2013, 'Java', 'Dummies', 30000),
            (2013, 'dotNET', 'Experts', 48000)
          AS s(year, course, training, earnings)
        ) PIVOT (sum(earnings) FOR (course, training)
                 IN (('Java', 'Dummies') AS java_dummies))
        ORDER BY year
    """).toPandas()
    assert list(actual.columns) == ["year", "java_dummies"]
    assert actual.values.tolist() == [[2012, 20000], [2013, 30000]]


# SQL for a pivot whose value alias collides with a grouping column, producing two
# output columns both named `year` (Spark allows this). Shared by the pair of tests below.
_PIVOT_GROUPING_COLLISION_SQL = """
    SELECT * FROM (
      SELECT year, course, earnings FROM VALUES
        (2012, 'Java', 20000),
        (2013, 'Java', 30000)
      AS s(year, course, earnings)
    ) PIVOT (sum(earnings) FOR (course) IN ('Java' AS year))
"""


def test_pivot_value_colliding_with_grouping_column(spark):
    # A pivot value whose output name collides with a grouping column is allowed: Spark
    # produces two `year` columns. The pivot resolves and executes correctly (verified via
    # collect, which matches Spark); duplicate-named output columns are handled positionally.
    df = spark.sql(_PIVOT_GROUPING_COLLISION_SQL)
    assert df.columns == ["year", "year"]
    rows = sorted((tuple(r) for r in df.collect()), key=lambda r: r[0])
    assert rows == [(2012, 20000), (2013, 30000)]


def test_pivot_value_colliding_with_grouping_column_to_pandas(spark):
    # Same query as the collect-based test above, but materialized via toPandas. This is the
    # path that exercises duplicate output names through Arrow conversion
    actual = spark.sql(_PIVOT_GROUPING_COLLISION_SQL).toPandas()
    assert list(actual.columns) == ["year", "year"]
    assert sorted(actual.values.tolist()) == [[2012, 20000], [2013, 30000]]


def test_pivot_inference_rejects_too_many_distinct_values(spark):
    # Spark caps inferred pivot values at `spark.sql.pivotMaxValues` (default 10000) and
    # errors beyond it. The distinct-value scan is bounded by a LIMIT of one past the cap,
    # so a high-cardinality pivot column never collects an unbounded result into the planner.
    df = spark.range(10001).selectExpr("CAST(0 AS INT) AS g", "id AS k", "CAST(1 AS INT) AS v")
    with pytest.raises(Exception, match="10000"):
        df.groupBy("g").pivot("k").sum("v").collect()


def test_pivot_max_values_config_is_honored(spark):
    # `spark.sql.pivotMaxValues` is configurable: lowering it makes inference reject a column
    # with more distinct values than the cap, and the message reflects the configured limit.
    original = spark.conf.get("spark.sql.pivotMaxValues")
    spark.conf.set("spark.sql.pivotMaxValues", "2")
    try:
        df = spark.createDataFrame(
            [("a", 1, 10), ("a", 2, 20), ("a", 3, 30)],
            schema="g STRING, k INT, v INT",
        )
        with pytest.raises(Exception, match="more than 2 distinct values"):
            df.groupBy("g").pivot("k").sum("v").collect()
    finally:
        spark.conf.set("spark.sql.pivotMaxValues", original)


def test_pivot_float_column_naming_matches_spark(spark):
    # Whole-number double pivot values keep their trailing `.0` in the column name (Spark
    # formats the value with its CAST-to-string; Sail mirrors this with the arrow formatter).
    actual = (
        spark.createDataFrame(
            [("a", 1.0, 10), ("a", 2.0, 20), ("b", 1.0, 5)],
            schema="g STRING, k DOUBLE, v INT",
        )
        .groupBy("g")
        .pivot("k")
        .sum("v")
        .orderBy("g")
        .toPandas()
    )
    assert list(actual.columns) == ["g", "1.0", "2.0"]


def test_pivot_decimal_value_naming_matches_spark(spark):
    # Decimal pivot values are named with their plain decimal string (e.g. `1.50`), matching
    # Spark — not `ScalarValue`'s debug-like rendering. Two groups exercise row distribution.
    actual = spark.sql("""
        SELECT * FROM (
          SELECT g, p, v FROM VALUES
            ('a', CAST(1.50 AS DECIMAL(5, 2)), 10),
            ('a', CAST(2.00 AS DECIMAL(5, 2)), 20),
            ('b', CAST(1.50 AS DECIMAL(5, 2)), 5),
            ('b', CAST(2.00 AS DECIMAL(5, 2)), 8)
          AS t(g, p, v)
        ) PIVOT (sum(v) FOR (p) IN (1.50, 2.00))
        ORDER BY g
    """).toPandas()
    assert list(actual.columns) == ["g", "1.50", "2.00"]
    assert actual.values.tolist() == [["a", 10, 20], ["b", 5, 8]]


def test_pivot_value_cast_to_int_column(spark):
    actual = spark.sql("""
        SELECT * FROM (
          SELECT g, k, v FROM VALUES
            ('a', 1, 10), ('a', 2, 20), ('b', 1, 5)
          AS t(g, k, v)
        ) PIVOT (sum(v) FOR (k) IN (1.2))
        ORDER BY g
    """).toPandas()
    assert list(actual.columns) == ["g", "1.2"]
    assert actual.values.tolist() == [["a", 10], ["b", 5]]


def test_pivot_value_cast_to_string_column(spark):
    actual = spark.sql("""
        SELECT * FROM (
          SELECT g, k, v FROM VALUES
            ('a', '1', 10), ('a', '01', 99),
            ('a', '2', 20), ('a', '02', 99),
            ('b', '1', 5),  ('b', '01', 99),
            ('b', '2', 8),  ('b', '02', 99)
          AS t(g, k, v)
        ) PIVOT (sum(v) FOR (k) IN (1, 2))
        ORDER BY g
    """).toPandas()
    assert list(actual.columns) == ["g", "1", "2"]
    assert actual.values.tolist() == [["a", 10, 20], ["b", 5, 8]]


@pytest.mark.xfail(
    strict=True,
    reason="A NaN pivot value matches the NaN row in Sail (the pivot predicate `k = NaN` "
    "evaluates true, consistent with Sail's `=` operator), so the NaN column is populated. "
    "Spark leaves it NULL: although Spark SQL's `NaN = NaN` is also true, its pivot uses raw "
    "IEEE `==` in codegen (NaN != NaN) and so contradicts its own `=`. Matching that would make "
    "Sail's pivot inconsistent with its own equality, so this extreme edge is left as-is.",
)
def test_pivot_nan_value_yields_null_column_like_spark(spark):
    # In Spark the NaN row matches no pivot value, so the `NaN` column is entirely NULL.
    actual = (
        spark.createDataFrame(
            [("a", 1.0, 10), ("a", float("nan"), 30), ("b", 1.0, 5)],
            schema="g STRING, k DOUBLE, v INT",
        )
        .groupBy("g")
        .pivot("k")
        .sum("v")
        .orderBy("g")
        .toPandas()
    )
    assert actual["NaN"].isna().all()


def test_pivot_date_value_supported(spark):
    # A DATE typed literal is a valid (foldable) pivot value; the resolver evaluates it to a
    # scalar and names the column with its date string, matching Spark.
    actual = spark.sql("""
        SELECT * FROM (
          SELECT g, d, v FROM VALUES
            ('a', DATE'2023-01-15', 10),
            ('a', DATE'2024-02-20', 20),
            ('b', DATE'2023-01-15', 5),
            ('b', DATE'2024-02-20', 8)
          AS t(g, d, v)
        ) PIVOT (sum(v) FOR (d) IN (DATE'2023-01-15', DATE'2024-02-20'))
        ORDER BY g
    """).toPandas()
    assert list(actual.columns) == ["g", "2023-01-15", "2024-02-20"]
    assert actual.values.tolist() == [["a", 10, 20], ["b", 5, 8]]


def test_pivot_timestamp_value_supported(spark):
    # A TIMESTAMP typed literal is likewise a valid pivot value.
    actual = spark.sql("""
        SELECT * FROM (
          SELECT g, t, v FROM VALUES
            ('a', TIMESTAMP'2023-01-15 10:30:00', 10),
            ('a', TIMESTAMP'2023-06-20 14:00:00', 20),
            ('b', TIMESTAMP'2023-01-15 10:30:00', 5),
            ('b', TIMESTAMP'2023-06-20 14:00:00', 8)
          AS x(g, t, v)
        ) PIVOT (sum(v) FOR (t) IN (TIMESTAMP'2023-01-15 10:30:00', TIMESTAMP'2023-06-20 14:00:00'))
        ORDER BY g
    """).toPandas()
    assert list(actual.columns) == ["g", "2023-01-15 10:30:00", "2023-06-20 14:00:00"]
    assert actual.values.tolist() == [["a", 10, 20], ["b", 5, 8]]


def test_pivot_infinity_values_named_and_matched(spark):
    # Infinity / -Infinity are valid double pivot values: named `Infinity` / `-Infinity` (like
    # Spark) and matched correctly. collect() is used so the assertion is exact for all cells.
    df = spark.sql("""
        SELECT * FROM (
          SELECT g, k, v FROM VALUES
            ('a', CAST('Infinity' AS DOUBLE), 10),
            ('a', CAST('-Infinity' AS DOUBLE), 20),
            ('a', CAST(1.0 AS DOUBLE), 5),
            ('b', CAST('Infinity' AS DOUBLE), 7),
            ('b', CAST('-Infinity' AS DOUBLE), 8),
            ('b', CAST(1.0 AS DOUBLE), 9)
          AS t(g, k, v)
        ) PIVOT (sum(v) FOR (k) IN (
          CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE), CAST(1.0 AS DOUBLE)))
        ORDER BY g
    """)
    assert df.columns == ["g", "Infinity", "-Infinity", "1.0"]
    assert [tuple(r) for r in df.collect()] == [("a", 10, 20, 5), ("b", 7, 8, 9)]


def test_pivot_inference_includes_null_pivot_value(spark):
    # A NULL in the pivot column becomes its own inferred `null` column, and the NULL-keyed rows
    # are summed into it (matching Spark). A group without a NULL row gets NULL in that column.
    df = (
        spark.createDataFrame(
            [("a", None, 5), ("a", 1, 10), ("b", 1, 7)],
            schema="g STRING, k INT, v INT",
        )
        .groupBy("g")
        .pivot("k")
        .sum("v")
        .orderBy("g")
    )
    assert df.columns == ["g", "null", "1"]
    assert [tuple(r) for r in df.collect()] == [("a", 5, 10), ("b", None, 7)]
