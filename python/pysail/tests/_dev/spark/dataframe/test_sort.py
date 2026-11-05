import pandas as pd
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pandas.testing import assert_frame_equal
from pyspark.errors import AnalysisException
from pyspark.sql import Row

from pysail.testing.spark.utils.common import is_jvm_spark


def test_sort(spark):
    # Reference: pyspark.sql.tests.connect.test_connect_basic.SparkConnectBasicTests.test_sort
    # We need to make sure sorting works since we have patched PySpark tests to ignore row order.
    query = """
        SELECT * FROM VALUES
        (false, 1, NULL), (false, NULL, 2.0), (NULL, 3, 3.0)
        AS tab(a, b, c)
    """
    # +-----+----+----+
    # |    a|   b|   c|
    # +-----+----+----+
    # |false|   1|NULL|
    # |false|NULL| 2.0|
    # | NULL|   3| 3.0|
    # +-----+----+----+

    df = spark.sql(query)
    assert_frame_equal(
        df.sort("a").toPandas()[["a"]],
        pd.DataFrame({"a": [None, False, False]}),
    )
    assert_frame_equal(
        df.sort("a").toPandas().sort_values(by=["a", "b"], ignore_index=True),
        pd.DataFrame(
            {"a": [False, False, None], "b": [1, None, 3], "c": [None, 2.0, 3.0]},
        ).astype({"c": object}),
    )
    assert_frame_equal(
        df.sort("c").toPandas(),
        pd.DataFrame(
            {"a": [False, False, None], "b": [1, None, 3], "c": [None, 2.0, 3.0]},
        ).astype({"c": object}),
    )
    assert_frame_equal(
        df.sort("b").toPandas(),
        pd.DataFrame(
            {"a": [False, False, None], "b": [None, 1, 3], "c": [2.0, None, 3.0]},
        ).astype({"c": object}),
    )
    assert_frame_equal(
        df.sort(df.c, "b").toPandas(),
        pd.DataFrame(
            {"a": [False, False, None], "b": [1, None, 3], "c": [None, 2.0, 3.0]},
        ).astype({"c": object}),
    )
    assert_frame_equal(
        df.sort(df.c.desc(), "b").toPandas(),
        pd.DataFrame(
            {"a": [None, False, False], "b": [3, None, 1], "c": [3.0, 2.0, None]},
        ).astype({"c": object}),
    )
    assert_frame_equal(
        df.sort(df.c.desc(), df.a.asc()).toPandas(),
        pd.DataFrame(
            {"a": [None, False, False], "b": [3, None, 1], "c": [3.0, 2.0, None]},
        ).astype({"c": object}),
    )


@pytest.fixture
def sort_source(spark):
    return spark.createDataFrame([(1, 10, 3), (2, 20, 1), (3, 30, 2)], "a int, b int, c int")


@pytest.mark.parametrize(
    ("operation", "expected"),
    [
        (lambda df: df.select("a").where("c > 1").orderBy(F.col("b").desc()), [Row(a=3), Row(a=1)]),
        (lambda df: df.select("a").where("c > 1").where("b > 5").orderBy(F.col("b").desc()), [Row(a=3), Row(a=1)]),
        (lambda df: df.select("a").where("c > 1").orderBy(F.col("b").desc()).limit(1), [Row(a=3)]),
        (lambda df: df.select("a", "c").select("a").orderBy("b"), [Row(a=1), Row(a=2), Row(a=3)]),
        (
            lambda df: df.select("a", "b").where("c > 1").select("a").orderBy(F.col("b") + F.col("c")),
            [Row(a=1), Row(a=3)],
        ),
    ],
    ids=["after-filter", "after-filters", "with-limit", "nested-projections", "multiple-levels"],
)
def test_sort_by_attribute_removed_by_projections(sort_source, operation, expected):
    result = operation(sort_source)
    assert result.columns == ["a"]
    assert result.collect() == expected


@pytest.mark.parametrize(
    ("operation", "expected"),
    [
        (lambda df: df.select("a"), [Row(a=3), Row(a=2), Row(a=1)]),
        (lambda df: df.select("a").where("c > 1"), [Row(a=3), Row(a=1)]),
    ],
    ids=["after-projection", "after-filter"],
)
def test_sort_within_partitions_by_attribute_removed_by_projections(sort_source, operation, expected):
    result = operation(sort_source.coalesce(1)).sortWithinPartitions(F.col("b").desc())
    assert result.columns == ["a"]
    assert result.collect() == expected


@pytest.mark.parametrize("operation", ["orderBy", "sortWithinPartitions"])
def test_sort_recovered_key_preserves_visible_alias(sort_source, operation):
    projected = sort_source.coalesce(1).select((-F.col("b")).alias("b"), "a").select("b")
    result = getattr(projected, operation)(F.col("b") + F.col("c"))
    assert result.collect() == [Row(b=-30), Row(b=-20), Row(b=-10)]
    assert result.schema == projected.schema


@pytest.mark.parametrize("operation", ["orderBy", "sortWithinPartitions"])
def test_sort_recovered_key_preserves_renamed_alias(sort_source, operation):
    projected = sort_source.coalesce(1).select((-F.col("b")).alias("x"), "a").select("x")
    result = getattr(projected, operation)(F.col("x") + F.col("c"))
    assert result.collect() == [Row(x=-30), Row(x=-20), Row(x=-10)]
    assert result.schema == projected.schema


@pytest.mark.parametrize("operation", ["orderBy", "sortWithinPartitions"])
def test_sort_many_visible_keys_with_one_recovered_key(spark, operation):
    columns = [f"c{i}" for i in range(128)]
    source = spark.createDataFrame([(1, 20), (2, 10), (3, None)], "id int, hidden int").coalesce(1)
    source = source.select("*", *(F.lit(i).alias(name) for i, name in enumerate(columns)))
    projected = source.select("id", *columns)
    result = getattr(projected, operation)(*columns, F.col("hidden").asc_nulls_last())

    assert result.schema == projected.schema
    assert result.collect() == [Row(id=id_, **{name: i for i, name in enumerate(columns)}) for id_ in (2, 1, 3)]


@pytest.mark.parametrize("operation", ["orderBy", "sortWithinPartitions"])
def test_sort_recovered_replacement_preserves_reference_scope(spark, operation):
    source = spark.createDataFrame([(1, 30), (2, 10), (3, 20)], "a int, b int").coalesce(1)
    projected = source.withColumn("b", F.col("a")).select("a")
    original = getattr(projected, operation)(source.b)
    replacement = getattr(projected, operation)("b")
    assert original.collect() == [Row(a=2), Row(a=3), Row(a=1)]
    assert replacement.collect() == [Row(a=1), Row(a=2), Row(a=3)]
    assert original.schema == replacement.schema == projected.schema


@pytest.mark.parametrize(
    "operation",
    [
        lambda df: df.withColumn("id", F.monotonically_increasing_id()).orderBy(F.col("b").desc()).select("id"),
        lambda df: df.withColumn("id", F.monotonically_increasing_id()).select("id").orderBy(F.col("b").desc()),
    ],
    ids=["visible-attribute", "removed-attribute"],
)
@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="The physical optimizer pushes sorts below the monotonically increasing ID generation",
    strict=True,
)
def test_sort_keeps_monotonically_increasing_ids(sort_source, operation):
    assert operation(sort_source.coalesce(1)).collect() == [Row(id=2), Row(id=1), Row(id=0)]


@pytest.mark.parametrize(
    ("operation", "expected"),
    [
        (
            lambda df: df.withColumn("id", F.monotonically_increasing_id())
            .select("id")
            .sortWithinPartitions(F.col("b").desc()),
            [Row(id=2), Row(id=1), Row(id=0)],
        ),
        (lambda df: df.select("a").limit(2).sortWithinPartitions(F.col("b").desc()), [Row(a=2), Row(a=1)]),
    ],
    ids=["monotonically-increasing-id", "limit"],
)
@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="Sail cannot recover sort keys above operators that the physical optimizer reorders",
    strict=True,
)
def test_sort_within_partitions_above_order_sensitive_operator(sort_source, operation, expected):
    # TODO: Preserve the order of ID generation and limits before recovering their sort keys.
    result = operation(sort_source.coalesce(1))
    assert result.collect() == expected


@pytest.mark.parametrize(
    "operation",
    [lambda df: df.orderBy("v"), lambda df: df.coalesce(1).sortWithinPartitions("v")],
    ids=["orderBy", "sortWithinPartitions"],
)
def test_sort_recovers_key_beside_order_sensitive_join_input(spark, operation):
    source = spark.createDataFrame(
        [(1, "a", 30), (2, "b", 10), (3, "a", 20), (4, "c", 40), (5, "b", 50)], "k int, g string, v int"
    )
    other = spark.createDataFrame([(1, 100), (2, 90), (3, 80), (6, 70)], "k int, w int")
    # The limit is in the other join input, so recovering `v` does not cross it.
    projected = source.join(other.orderBy("k").limit(3), "k").select("g", "w").select("g")
    assert operation(projected).collect() == [Row(g="b"), Row(g="a"), Row(g="a")]


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("SELECT id FROM sort_user_source ORDER BY id DESC, user", [Row(id=3), Row(id=2), Row(id=1)]),
        ("SELECT count(*) AS c FROM sort_user_source GROUP BY user ORDER BY user", [Row(c=1), Row(c=1), Row(c=1)]),
    ],
    ids=["secondary-key", "grouping-key"],
)
def test_sort_by_column_named_like_literal_function(spark, query, expected):
    source = spark.createDataFrame([(1, "bob"), (2, "alice"), (3, "carl")], "id int, user string")
    source.coalesce(1).createOrReplaceTempView("sort_user_source")
    try:
        assert spark.sql(query).collect() == expected
    finally:
        spark.catalog.dropTempView("sort_user_source")


def test_sql_sort_by_attribute_removed_by_projection(spark, sort_source):
    sort_source.coalesce(1).createOrReplaceTempView("sort_by_source")
    try:
        result = spark.sql("SELECT a FROM sort_by_source SORT BY b DESC")
        assert result.collect() == [Row(a=3), Row(a=2), Row(a=1)]
    finally:
        spark.catalog.dropTempView("sort_by_source")


@pytest.mark.parametrize("operation", ["orderBy", "sortWithinPartitions"])
@pytest.mark.parametrize("replacement", ["missing-field", "ambiguous-root", "scalar"])
@pytest.mark.parametrize("selector", ["dotted", "literal"])
def test_sort_recovers_key_after_failed_output_resolution(spark, operation, replacement, selector):
    source = spark.createDataFrame([(1, (20,)), (2, (10,))], "key int, payload struct<x:int>").coalesce(1)
    value = F.lit(9) if replacement == "scalar" else F.struct(F.lit(9).alias("y"))
    projected = source.select("key", value.alias("payload"))
    if replacement == "ambiguous-root":
        projected = source.select("key", "payload", F.struct(F.lit(9).alias("x")).alias("payload"))
    key = F.col("payload.x") if selector == "dotted" else F.col("payload")["x"]
    result = getattr(projected, operation)(key)
    assert [row.key for row in result.collect()] == [2, 1]
    assert result.schema == projected.schema
    # Spark's filter and repartition initial resolution throws on the same
    # invalid output instead of recovering a descendant, unlike sort resolution.
    for invalid in (projected.where(key > 0), projected.repartition(1, key), projected.alias("t").orderBy(key)):
        with pytest.raises(AnalysisException):
            invalid.collect()


@pytest.mark.parametrize("columns", [["a"], ["x.a", "b"]])
def test_sort_distinct_by_qualified_select_list_column(spark, columns):
    df = spark.createDataFrame([(1, 30), (2, 10), (1, 30), (3, 40)], "a int, b int").alias("x")
    result = df.select(*columns).distinct().orderBy(F.col("x.a").desc())
    assert [row.a for row in result.collect()] == [3, 2, 1]
