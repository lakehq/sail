import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql import Row
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.window import Window


def test_dataframe_drop(spark):
    df = spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
    df2 = spark.createDataFrame([Row(height=80, name="Tom"), Row(height=85, name="Bob")])

    assert_frame_equal(
        df.drop("age").sort("name").toPandas(),
        pd.DataFrame({"name": ["Alice", "Bob", "Tom"]}),
    )
    assert_frame_equal(
        df.drop(df.age).sort("name").toPandas(),
        pd.DataFrame({"name": ["Alice", "Bob", "Tom"]}),
    )

    assert_frame_equal(
        df.join(df2, df.name == df2.name, "inner").drop("name").sort("age").toPandas(),
        pd.DataFrame({"age": [14, 16], "height": [80, 85]}),
    )

    df3 = df.join(df2)
    assert_frame_equal(
        df3.select(
            df["age"],
            df["name"].alias("name_left"),
            df2["height"],
            df2["name"].alias("name_right"),
        )
        .sort("name_left", "name_right")
        .toPandas(),
        pd.DataFrame(
            {
                "age": [23, 23, 16, 16, 14, 14],
                "name_left": ["Alice", "Alice", "Bob", "Bob", "Tom", "Tom"],
                "height": [85, 80, 85, 80, 85, 80],
                "name_right": ["Bob", "Tom", "Bob", "Tom", "Bob", "Tom"],
            }
        ),
    )

    assert_frame_equal(
        df3.drop("name").sort("age", "height").toPandas(),
        pd.DataFrame({"age": [14, 14, 16, 16, 23, 23], "height": [80, 85, 80, 85, 80, 85]}),
    )

    with pytest.raises(Exception, match="AMBIGUOUS_REFERENCE"):
        df3.drop(col("name")).toPandas()

    df4 = df.withColumn("a.b.c", lit(1))
    assert_frame_equal(
        df4.sort("age").toPandas(),
        pd.DataFrame({"age": [14, 16, 23], "name": ["Tom", "Bob", "Alice"], "a.b.c": [1, 1, 1]}).astype(
            {"a.b.c": "int32"}
        ),
    )

    assert_frame_equal(
        df4.drop("a.b.c").sort("age").toPandas(),
        pd.DataFrame({"age": [14, 16, 23], "name": ["Tom", "Bob", "Alice"]}),
    )

    assert_frame_equal(
        df4.drop(col("a.b.c")).sort("age").toPandas(),
        pd.DataFrame({"age": [14, 16, 23], "name": ["Tom", "Bob", "Alice"], "a.b.c": [1, 1, 1]}).astype(
            {"a.b.c": "int32"}
        ),
    )


def test_dataframe_describe_and_summary_string_column(spark):
    """Compute Spark-compatible statistics for a string column."""
    df = spark.createDataFrame([("Alice",), ("Bob",), (None,)], "name STRING")

    assert {row.summary: row.name for row in df.describe("name").collect()} == {
        "count": "2",
        "mean": None,
        "stddev": None,
        "min": "Alice",
        "max": "Bob",
    }
    assert {row.summary: row.name for row in df.summary().collect()} == {
        "count": "2",
        "mean": None,
        "stddev": None,
        "min": "Alice",
        "25%": None,
        "50%": None,
        "75%": None,
        "max": "Bob",
    }


def test_dataframe_describe_and_summary_numeric_string_column(spark):
    """Include numeric strings in numeric statistics while counting all strings."""
    df = spark.createDataFrame([("10",), ("20",), ("not-a-number",), (None,)], "value STRING")

    assert {row.summary: row.value for row in df.describe("value").collect()} == {
        "count": "3",
        "mean": "15.0",
        "stddev": "7.0710678118654755",
        "min": "10",
        "max": "not-a-number",
    }
    summary = df.summary("count", "mean", "stddev", "min", "max").collect()
    assert {row.summary: row.value for row in summary} == {
        "count": "3",
        "mean": "15.0",
        "stddev": "7.0710678118654755",
        "min": "10",
        "max": "not-a-number",
    }


def test_dataframe_describe_and_summary_ignore_unsupported_columns(spark):
    """Exclude unsupported columns from statistics on a mixed schema."""
    df = spark.createDataFrame(
        [(1, "10", True), (2, "not-a-number", False)],
        "id LONG, value STRING, flag BOOLEAN",
    )

    describe = df.describe()
    assert describe.columns == ["summary", "id", "value"]
    assert {row.summary for row in describe.collect()} == {"count", "mean", "stddev", "min", "max"}

    summary = df.summary()
    assert summary.columns == ["summary", "id", "value"]
    assert {row.summary for row in summary.collect()} == {
        "count",
        "mean",
        "stddev",
        "min",
        "25%",
        "50%",
        "75%",
        "max",
    }


def test_dataframe_describe_and_summary_unsupported_column_only(spark):
    """Return only statistic labels when every selected column is unsupported."""
    df = spark.createDataFrame([(True,), (False,)], "flag BOOLEAN")

    describe = df.describe("flag")
    assert describe.columns == ["summary"]
    assert {row.summary for row in describe.collect()} == {"count", "mean", "stddev", "min", "max"}

    summary = df.summary()
    assert summary.columns == ["summary"]
    assert {row.summary for row in summary.collect()} == {
        "count",
        "mean",
        "stddev",
        "min",
        "25%",
        "50%",
        "75%",
        "max",
    }


def test_dataframe_with_column_alias(spark):
    df = spark.createDataFrame(
        schema="id INTEGER, value STRING",
        data=[(1, "bar"), (2, "foo")],
    )

    # Using alias and referencing a single column works
    assert_frame_equal(
        df.alias("a").withColumn("col1", col("a.id")).sort("id").toPandas(),
        pd.DataFrame({"id": [1, 2], "value": ["bar", "foo"], "col1": [1, 2]}).astype({"id": "int32", "col1": "int32"}),
    )

    # Using alias and referencing multiple columns in chained withColumn calls
    assert_frame_equal(
        df.alias("a").withColumn("col1", col("a.id")).withColumn("col2", col("a.value")).sort("id").toPandas(),
        pd.DataFrame({"id": [1, 2], "value": ["bar", "foo"], "col1": [1, 2], "col2": ["bar", "foo"]}).astype(
            {"id": "int32", "col1": "int32"}
        ),
    )

    # More than two chained withColumn calls with alias
    assert_frame_equal(
        df.alias("a")
        .withColumn("col1", col("a.id"))
        .withColumn("col2", col("a.value"))
        .withColumn("col3", col("a.id"))
        .sort("id")
        .toPandas(),
        pd.DataFrame(
            {
                "id": [1, 2],
                "value": ["bar", "foo"],
                "col1": [1, 2],
                "col2": ["bar", "foo"],
                "col3": [1, 2],
            }
        ).astype({"id": "int32", "col1": "int32", "col3": "int32"}),
    )


def test_with_metadata(spark):
    df = spark.sql("SELECT 1 AS a")
    assert df.schema["a"].metadata == {}
    assert df.withMetadata("a", {"m": "x"}).schema["a"].metadata == {"m": "x"}
    assert df.withMetadata("a", {"m": "x"}).withMetadata("a", {"n": "y"}).schema["a"].metadata == {"n": "y"}
    assert df.withMetadata("a", {"m": "x"}).withMetadata("a", {}).schema["a"].metadata == {}


def reverse_sorted_map_in_pandas(df):
    def reverse_batches(iterator):
        for pdf in iterator:
            yield pd.DataFrame({"id": pdf["id"].iloc[::-1].to_numpy()})

    return df.orderBy(col("id")).mapInPandas(reverse_batches, schema="id long")


def test_map_in_pandas_reordered_rows_can_be_sorted_again(spark):
    actual = reverse_sorted_map_in_pandas(spark.range(0, 4, 1, 1)).orderBy(col("id")).toPandas()
    expected = pd.DataFrame({"id": [0, 1, 2, 3]}, dtype="int64")

    assert_frame_equal(actual, expected)


def test_map_in_pandas_reordering_does_not_satisfy_window_ordering(spark):
    window = Window.orderBy(col("id"))

    actual = (
        reverse_sorted_map_in_pandas(spark.range(0, 4, 1, 1))
        .select("id", row_number().over(window).alias("rn"))
        .orderBy(col("id"))
        .toPandas()
    )
    expected = pd.DataFrame({"id": [0, 1, 2, 3], "rn": [1, 2, 3, 4]}).astype({"rn": "int32"})

    assert_frame_equal(actual, expected)
