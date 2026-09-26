import pandas as pd
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pandas.testing import assert_frame_equal
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
