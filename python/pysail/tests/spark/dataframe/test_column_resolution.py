"""DataFrame column resolution, subquery, and set operation regressions."""

# ruff: noqa: PLR2004

from collections import Counter

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import functions as f

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

SUBQUERY_API = pytest.mark.skipif(
    pyspark_version() < (4,), reason="DataFrame subqueries and lateral joins require PySpark 4+"
)


@pytest.fixture(autouse=True)
def case_sensitivity(spark):
    original = spark.conf.get("spark.sql.caseSensitive")
    try:
        spark.conf.set("spark.sql.caseSensitive", "false")
        yield
    finally:
        spark.conf.set("spark.sql.caseSensitive", original)


@pytest.fixture
def source(spark):
    return spark.sql(
        """
        SELECT * FROM VALUES
          (1L, 'a', 10, 100),
          (2L, 'a', 20, 200),
          (3L, 'b', CAST(NULL AS INT), 300),
          (4L, 'b', 40, CAST(NULL AS INT)),
          (5L, CAST(NULL AS STRING), 50, 500),
          (6L, 'a', 10, 600),
          (7L, 'c', 70, 700)
        AS t(id, g, v, w)
        """
    )


@pytest.fixture
def other(spark):
    return spark.sql(
        """
        SELECT * FROM VALUES
          (1L, 'a', 100), (2L, 'a', 200), (4L, 'b', 400), (8L, 'z', 800),
          (CAST(NULL AS BIGINT), 'n', 900)
        AS u(id, g, z)
        """
    )


@pytest.fixture
def case_columns(spark):
    return spark.sql("SELECT * FROM VALUES (1L, 10, 100), (2L, 20, 200) AS c(id, A, a)")


def test_full_using_join_preserves_bound_input_keys(source, other):
    result = source.join(other, "id", "full").select(source.id.alias("l"), other.id.alias("r"), f.col("id"))
    expected = [
        (1, 1, 1),
        (2, 2, 2),
        (3, None, 3),
        (4, 4, 4),
        (5, None, 5),
        (6, None, 6),
        (7, None, 7),
        (None, 8, 8),
        (None, None, None),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("l", "bigint"), ("r", "bigint"), ("id", "bigint")]


@pytest.mark.parametrize(
    ("how", "expected"),
    [
        ("left", [(1, "a", 1, "x"), (2, "b", None, None)]),
        ("inner", [(1, "a", 1, "x")]),
        ("full", [(1, "a", 1, "x"), (2, "b", None, None), (None, None, 3, "y")]),
        ("left_anti", [(2, "b", None, None)]),
    ],
)
def test_using_join_preserves_qualified_stars_and_keys(spark, how, expected):
    left = spark.createDataFrame([(1, "a"), (2, "b")], "id int, l string").alias("t")
    right = spark.createDataFrame([(1, "x"), (3, "y")], "id int, r string").alias("s")
    joined = left.join(right, on="id", how=how)

    result = joined.select("t.*")
    assert result.columns == ["id", "l"]
    assert Counter(result.collect()) == Counter((row[0], row[1]) for row in expected)
    assert Counter(joined.select("t.id").collect()) == Counter((row[0],) for row in expected)

    if how != "left_anti":
        result = joined.select("t.*", "s.r")
        assert result.columns == ["id", "l", "r"]
        assert Counter(result.collect()) == Counter((row[0], row[1], row[3]) for row in expected)
        assert Counter(joined.select("s.id").collect()) == Counter((row[2],) for row in expected)

    assert Counter(joined.select("id").collect()) == Counter(
        (row[0] if row[0] is not None else row[2],) for row in expected
    )


def test_distinct_filter_recovers_hidden_column(source):
    result = source.select("id", "g").distinct().filter(f.col("v") > 15)
    expected = [
        (2, "a"),
        (4, "b"),
        (5, None),
        (7, "c"),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("id", "bigint"), ("g", "string")]


def test_drop_duplicates_filter_recovers_hidden_column(source):
    result = source.select("id", "g").dropDuplicates().filter(f.col("v") > 15)
    expected = [
        (2, "a"),
        (4, "b"),
        (5, None),
        (7, "c"),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("id", "bigint"), ("g", "string")]


def test_drop_duplicates_filter_recovers_column_outside_subset(source):
    result = source.select("id", "g", "v").dropDuplicates(["id", "g", "v"]).filter(f.col("w") > 100)
    expected = [
        (2, "a", 20),
        (3, "b", None),
        (5, None, 50),
        (6, "a", 10),
        (7, "c", 70),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("id", "bigint"), ("g", "string"), ("v", "int")]


def test_union_by_name_fills_missing_columns(source, other):
    result = source.select("id", "v").unionByName(other.select("id", "z"), allowMissingColumns=True)
    expected = [
        (1, 10, None),
        (1, None, 100),
        (2, 20, None),
        (2, None, 200),
        (3, None, None),
        (4, 40, None),
        (4, None, 400),
        (5, 50, None),
        (6, 10, None),
        (7, 70, None),
        (8, None, 800),
        (None, None, 900),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("id", "bigint"), ("v", "int"), ("z", "int")]


def test_distinct_filter_recovers_hidden_column_beside_alias(source):
    result = source.selectExpr("g AS x", "id").distinct().filter(f.col("v") > 15)
    expected = [
        ("a", 2),
        ("b", 4),
        ("c", 7),
        (None, 5),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("x", "string"), ("id", "bigint")]


def test_distinct_filter_combines_visible_alias_and_hidden_column(source):
    result = source.selectExpr("g AS x", "id").distinct().filter((f.col("x") == "a") & (f.col("v") > 15))
    expected = [
        ("a", 2),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("x", "string"), ("id", "bigint")]


def test_distinct_filter_recovers_multiple_hidden_columns(source):
    result = source.selectExpr("g AS x", "id").distinct().filter((f.col("v") > 15) & (f.col("w") > 100))
    expected = [
        ("a", 2),
        ("c", 7),
        (None, 5),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("x", "string"), ("id", "bigint")]


def test_distinct_filter_recovers_bound_hidden_column(source):
    result = source.selectExpr("g AS x", "id").distinct().filter(source.v > 15)
    expected = [
        ("a", 2),
        ("b", 4),
        ("c", 7),
        (None, 5),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("x", "string"), ("id", "bigint")]


def test_distinct_filter_combines_bound_hidden_column_and_alias(source):
    result = source.selectExpr("g AS x", "id").distinct().filter((source.v > 15) & (f.col("x") == "a"))
    expected = [
        ("a", 2),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("x", "string"), ("id", "bigint")]


def test_distinct_sort_recovers_hidden_column(source):
    result = source.selectExpr("g AS x", "id").distinct().orderBy(f.col("w").asc_nulls_first(), f.col("id"))
    expected = [
        ("b", 4),
        ("a", 1),
        ("a", 2),
        ("b", 3),
        (None, 5),
        ("a", 6),
        ("c", 7),
    ]
    assert result.collect() == expected
    assert result.dtypes == [("x", "string"), ("id", "bigint")]


def test_distinct_sort_combines_visible_alias_and_hidden_column(source):
    result = (
        source.selectExpr("g AS x", "id")
        .distinct()
        .orderBy(f.col("x").asc_nulls_first(), f.col("w").asc_nulls_first(), f.col("id"))
    )
    expected = [
        (None, 5),
        ("a", 1),
        ("a", 2),
        ("a", 6),
        ("b", 4),
        ("b", 3),
        ("c", 7),
    ]
    assert result.collect() == expected
    assert result.dtypes == [("x", "string"), ("id", "bigint")]


def test_distinct_sort_recovers_bound_hidden_column(source):
    result = source.selectExpr("g AS x", "id").distinct().orderBy(source.v.asc_nulls_first(), f.col("id"))
    expected = [
        ("b", 3),
        ("a", 1),
        ("a", 6),
        ("a", 2),
        ("b", 4),
        (None, 5),
        ("c", 7),
    ]
    assert result.collect() == expected
    assert result.dtypes == [("x", "string"), ("id", "bigint")]


def test_rename_column_ignores_case(spark, source):
    original = spark.conf.get("spark.sql.caseSensitive")
    try:
        spark.conf.set("spark.sql.caseSensitive", "false")
        result = source.withColumnRenamed("V", "x")
        expected = [
            (1, "a", 10, 100),
            (2, "a", 20, 200),
            (3, "b", None, 300),
            (4, "b", 40, None),
            (5, None, 50, 500),
            (6, "a", 10, 600),
            (7, "c", 70, 700),
        ]
        assert Counter(result.collect()) == Counter(expected)
        assert result.dtypes == [("id", "bigint"), ("g", "string"), ("x", "int"), ("w", "int")]
    finally:
        spark.conf.set("spark.sql.caseSensitive", original)


def test_rename_columns_ignores_case(spark, source):
    original = spark.conf.get("spark.sql.caseSensitive")
    try:
        spark.conf.set("spark.sql.caseSensitive", "false")
        result = source.withColumnsRenamed({"V": "x"})
        expected = [
            (1, "a", 10, 100),
            (2, "a", 20, 200),
            (3, "b", None, 300),
            (4, "b", 40, None),
            (5, None, 50, 500),
            (6, "a", 10, 600),
            (7, "c", 70, 700),
        ]
        assert Counter(result.collect()) == Counter(expected)
        assert result.dtypes == [("id", "bigint"), ("g", "string"), ("x", "int"), ("w", "int")]
    finally:
        spark.conf.set("spark.sql.caseSensitive", original)


def test_rename_column_updates_all_case_insensitive_matches(spark, case_columns):
    original = spark.conf.get("spark.sql.caseSensitive")
    try:
        spark.conf.set("spark.sql.caseSensitive", "false")
        result = case_columns.withColumnRenamed("a", "x")
        expected = [
            (1, 10, 100),
            (2, 20, 200),
        ]
        assert Counter(result.collect()) == Counter(expected)
        assert result.dtypes == [("id", "bigint"), ("x", "int"), ("x", "int")]
    finally:
        spark.conf.set("spark.sql.caseSensitive", original)


def test_case_sensitive_drop_preserves_differently_cased_column(spark, source):
    original = spark.conf.get("spark.sql.caseSensitive")
    try:
        spark.conf.set("spark.sql.caseSensitive", "true")
        result = source.drop("V")
        expected = [
            (1, "a", 10, 100),
            (2, "a", 20, 200),
            (3, "b", None, 300),
            (4, "b", 40, None),
            (5, None, 50, 500),
            (6, "a", 10, 600),
            (7, "c", 70, 700),
        ]
        assert Counter(result.collect()) == Counter(expected)
        assert result.dtypes == [("id", "bigint"), ("g", "string"), ("v", "int"), ("w", "int")]
    finally:
        spark.conf.set("spark.sql.caseSensitive", original)


def test_case_sensitive_drop_removes_only_exact_match(spark, case_columns):
    original = spark.conf.get("spark.sql.caseSensitive")
    try:
        spark.conf.set("spark.sql.caseSensitive", "true")
        result = case_columns.drop("a")
        expected = [
            (1, 10),
            (2, 20),
        ]
        assert Counter(result.collect()) == Counter(expected)
        assert result.dtypes == [("id", "bigint"), ("A", "int")]
    finally:
        spark.conf.set("spark.sql.caseSensitive", original)


def test_case_sensitive_union_by_name_rejects_differently_cased_columns(spark, source):
    original = spark.conf.get("spark.sql.caseSensitive")
    try:
        spark.conf.set("spark.sql.caseSensitive", "true")
        with pytest.raises(PySparkException, match=r"(?i)(cannot resolve|unresolved|not found)"):
            source.selectExpr("v AS V").unionByName(source.select("v")).collect()
    finally:
        spark.conf.set("spark.sql.caseSensitive", original)


def test_sort_hidden_column_after_offset(source):
    result = source.orderBy("id").select("id", "g").offset(2).orderBy(f.col("w").asc_nulls_first(), "id")
    expected = [
        (4, "b"),
        (3, "b"),
        (5, None),
        (6, "a"),
        (7, "c"),
    ]
    assert result.collect() == expected
    assert result.dtypes == [("id", "bigint"), ("g", "string")]


@SUBQUERY_API
def test_lateral_filter_recovers_hidden_column_with_unqualified_outer_reference(source, other):
    result = (
        source.select("id", "v")
        .lateralJoin(other.select("z").where(other.id == f.col("id").outer()), how="left")
        .select("z")
        .filter(f.col("v") > 15)
    )
    expected = [
        (100,),
        (100,),
        (100,),
        (100,),
        (200,),
        (200,),
        (200,),
        (200,),
        (400,),
        (400,),
        (400,),
        (400,),
        (800,),
        (800,),
        (800,),
        (800,),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("z", "int")]


def test_with_columns_resolves_alias_from_same_call(source):
    result = source.withColumns({"x": source.v + 1, "y": f.col("x") + 1})
    expected = [
        (1, "a", 10, 100, 11, 12),
        (2, "a", 20, 200, 21, 22),
        (3, "b", None, 300, None, None),
        (4, "b", 40, None, 41, 42),
        (5, None, 50, 500, 51, 52),
        (6, "a", 10, 600, 11, 12),
        (7, "c", 70, 700, 71, 72),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("id", "bigint"), ("g", "string"), ("v", "int"), ("w", "int"), ("x", "int"), ("y", "int")]


def test_using_join_coalesces_repeated_bound_key(source, other):
    result = source.select("id", "id", "v").join(other, "id")
    expected = [
        (1, 10, "a", 100),
        (2, 20, "a", 200),
        (4, 40, "b", 400),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("id", "bigint"), ("v", "int"), ("g", "string"), ("z", "int")]


@SUBQUERY_API
@pytest.mark.skipif(is_jvm_spark(), reason="Sail supports bound outer columns rejected by JVM Spark")
def test_scalar_subquery_supports_bound_outer_column(source, other):
    result = source.select(
        "id", other.where(other.id == source.id.outer()).agg(f.max("z").alias("z")).scalar().alias("z")
    )
    expected = [
        (1, 100),
        (2, 200),
        (3, None),
        (4, 400),
        (5, None),
        (6, None),
        (7, None),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("id", "bigint"), ("z", "int")]


@SUBQUERY_API
@pytest.mark.skipif(is_jvm_spark(), reason="Sail supports bound outer columns rejected by JVM Spark")
def test_exists_subquery_supports_bound_outer_column(source, other):
    result = source.where(other.where(other.id == source.id.outer()).exists()).select("id")
    expected = [
        (1,),
        (2,),
        (4,),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("id", "bigint")]


@pytest.mark.skipif(pyspark_version() < (4, 1), reason="Column.isin with a DataFrame requires PySpark 4.1+")
def test_isin_accepts_dataframe_subquery(source, other):
    result = source.select("id", source.id.isin(other.select("id")).alias("b"))
    expected = [
        (1, True),
        (2, True),
        (3, False),
        (4, True),
        (5, False),
        (6, False),
        (7, False),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("id", "bigint"), ("b", "boolean")]


def test_union_by_name_fills_missing_struct_fields(source):
    result = source.select(f.struct(f.col("v").alias("a")).alias("s")).unionByName(
        source.select(f.struct(f.col("w").alias("b")).alias("s")), allowMissingColumns=True
    )
    expected = [
        ((10, None),),
        ((10, None),),
        ((20, None),),
        ((40, None),),
        ((50, None),),
        ((70, None),),
        ((None, 100),),
        ((None, 200),),
        ((None, 300),),
        ((None, 500),),
        ((None, 600),),
        ((None, 700),),
        ((None, None),),
        ((None, None),),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("s", "struct<a:int,b:int>")]


def test_intersect_all_preserves_minimum_row_multiplicity(source):
    result = source.select("g", "v").intersectAll(source.where("id<=3").select("g", "v"))
    expected = [
        ("a", 10),
        ("a", 20),
        ("b", None),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("g", "string"), ("v", "int")]


def test_distinct_filter_recovers_hidden_column_outside_output(source):
    result = source.select("g").distinct().filter(f.col("id") > 0)
    expected = [
        ("a",),
        ("b",),
        ("c",),
        (None,),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("g", "string")]


def test_drop_duplicates_filter_recovers_column_outside_key(source):
    result = source.select("id", "g").dropDuplicates(["id"]).filter(f.col("v") > 15)
    expected = [
        (2, "a"),
        (4, "b"),
        (5, None),
        (7, "c"),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("id", "bigint"), ("g", "string")]


def test_later_join_resolves_bound_key_from_using_join(source, other):
    joined = source.join(other, "id")
    result = joined.join(other.alias("r"), source.id == f.col("r.id")).select(source.id)
    expected = [
        (1,),
        (2,),
        (4,),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("id", "bigint")]


@SUBQUERY_API
def test_lateral_join_correlates_qualified_outer_reference(source, other):
    result = (
        source.alias("l")
        .lateralJoin(other.alias("r").where(f.col("r.id") == f.col("l.id").outer()).select("z"), how="left")
        .select("l.id", "z")
    )
    expected = [
        (1, 100),
        (2, 200),
        (3, None),
        (4, 400),
        (5, None),
        (6, None),
        (7, None),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("id", "bigint"), ("z", "int")]


@SUBQUERY_API
def test_lateral_filter_recovers_hidden_column_with_qualified_outer_reference(source, other):
    result = (
        source.alias("l")
        .select("id", "v")
        .lateralJoin(other.alias("r").where(f.col("r.id") == f.col("l.id").outer()).select("z"), how="left")
        .select("z")
        .filter(f.col("v") > 15)
    )
    expected = [
        (200,),
        (400,),
        (None,),
        (None,),
    ]
    assert Counter(result.collect()) == Counter(expected)
    assert result.dtypes == [("z", "int")]


def test_sort_visible_column_after_offset(source):
    result = source.orderBy("id").select("id", "g", "w").offset(2).orderBy(f.col("w").asc_nulls_first(), "id")
    expected = [
        (4, "b", None),
        (3, "b", 300),
        (5, None, 500),
        (6, "a", 600),
        (7, "c", 700),
    ]
    assert result.collect() == expected
    assert result.dtypes == [("id", "bigint"), ("g", "string"), ("w", "int")]


def test_hidden_sort_does_not_move_below_offset(spark):
    result = (
        spark.sql("SELECT * FROM VALUES (1,20),(2,30),(3,10) t(id,v)")
        .orderBy("id")
        .select("id")
        .offset(1)
        .orderBy("v", "id")
    )
    expected = [
        (3,),
        (2,),
    ]
    assert result.collect() == expected
    assert result.dtypes == [("id", "int")]


def test_lateral_alias_evaluates_random_once(spark):
    rows = spark.sql("SELECT rand() AS x, x AS y FROM range(10)").collect()
    assert all(row.x == row.y for row in rows)


def test_lateral_alias_does_not_capture_unrelated_dataframe_column(spark):
    left = spark.range(1)
    right = spark.range(1).withColumn("x", f.lit(9))
    with pytest.raises(PySparkException):
        left.select(f.lit(7).alias("x"), right.x.alias("y")).collect()


@pytest.mark.parametrize("aggregate", [False, True])
def test_lateral_alias_chain_uses_materialized_values(spark, aggregate):
    expressions = ["count(*) AS n"] if aggregate else []
    expressions.append("1L AS a0")
    expressions.extend(f"a{i - 1} + a{i - 1} AS a{i}" for i in range(1, 31))
    result = spark.range(1).selectExpr(*expressions).collect()
    assert len(result) == 1
    assert result[0].a30 == 2**30


def test_alias_over_using_join_keeps_hidden_key_provenance(spark):
    left = spark.sql("SELECT 1 AS id, 'left' AS a").alias("l")
    right = spark.sql("SELECT 1 AS id, 'right' AS b").alias("r")
    joined = left.join(right, "id").alias("q")
    assert joined.select("q.*").columns == ["id", "a", "b"]
    assert [tuple(row) for row in joined.select("q.id", "l.id", "r.id").collect()] == [(1, 1, 1)]
