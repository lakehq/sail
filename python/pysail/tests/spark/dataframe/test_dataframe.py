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


def test_with_column_matches_name_case_insensitively(spark):
    df = spark.createDataFrame([(1, 10), (2, 20)], ["a", "b"])

    # The existing column is replaced in place, and it takes the new name.
    replaced = df.withColumn("A", col("a") + 1)
    assert replaced.columns == ["A", "b"]
    assert [r.asDict() for r in replaced.orderBy("A").collect()] == [{"A": 2, "b": 10}, {"A": 3, "b": 20}]

    assert df.withColumn("a", col("a") + 1).columns == ["a", "b"]
    assert df.withColumn("zz", lit(1)).columns == ["a", "b", "zz"]
    assert df.withColumns({"A": lit(1), "B": lit(2)}).columns == ["A", "B"]

    with pytest.raises(Exception, match="COLUMN_ALREADY_EXISTS"):
        _ = df.withColumns({"a": lit(1), "A": lit(2)}).columns

    # The first duplicate in alphabetical order is the one reported.
    with pytest.raises(Exception, match="The column `a` already exists"):
        _ = df.withColumns({"z": lit(1), "a": lit(2), "Z": lit(3), "A": lit(4)}).columns


def test_with_columns_renamed_matches_name_case_insensitively(spark):
    df = spark.createDataFrame([(1, 10)], ["a", "b"])

    assert df.withColumnRenamed("A", "z").columns == ["z", "b"]
    assert df.withColumnRenamed("a", "z").columns == ["z", "b"]
    # A name that matches no column is ignored.
    assert df.withColumnRenamed("nope", "z").columns == ["a", "b"]
    assert df.withColumnsRenamed({"A": "z", "B": "y"}).columns == ["z", "y"]

    # The renames are applied in order to the output of the previous one, so the second
    # entry no longer matches the column that the first one renamed.
    assert df.withColumnsRenamed({"A": "z", "a": "y"}).columns == ["z", "b"]
    # Spark 3.5 rejected the resulting duplicate name with COLUMN_ALREADY_EXISTS;
    # Spark 4 allows it, and we follow the latest behavior.
    assert df.withColumnsRenamed({"a": "b", "b": "c"}).columns == ["c", "c"]


def test_with_column_case_sensitive(spark):
    spark.conf.set("spark.sql.caseSensitive", "true")
    try:
        df = spark.createDataFrame([(1, 10)], ["a", "b"])
        # The names no longer match, so the column is appended instead of replaced.
        assert df.withColumn("A", lit(1)).columns == ["a", "b", "A"]
        assert df.withColumns({"a": lit(1), "A": lit(2)}).columns == ["a", "b", "A"]
        # A rename that matches no column is ignored.
        assert df.withColumnRenamed("A", "z").columns == ["a", "b"]
        assert df.withColumnRenamed("a", "z").columns == ["z", "b"]
    finally:
        spark.conf.unset("spark.sql.caseSensitive")


def test_with_column_matches_non_ascii_names(spark):
    assert spark.sql("SELECT 1 AS `Ä`").withColumn("ä", lit(2)).columns == ["ä"]
    assert spark.sql("SELECT 1 AS `ä`").withColumnsRenamed({"Ä": "z"}).columns == ["z"]

    with pytest.raises(Exception, match="COLUMN_ALREADY_EXISTS"):
        _ = spark.range(1).withColumns({"Ä": lit(1), "ä": lit(2)}).columns


def test_with_metadata_matches_name_case_insensitively(spark):
    df = spark.createDataFrame([(1, 10)], ["a", "b"])

    annotated = df.withMetadata("A", {"m": "x"})
    assert annotated.columns == ["A", "b"]
    assert annotated.schema["A"].metadata == {"m": "x"}


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
