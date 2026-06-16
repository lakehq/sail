import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql import Row
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
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


def test_with_metadata(spark):
    df = spark.sql("SELECT 1 AS a")
    assert df.schema["a"].metadata == {}
    assert df.withMetadata("a", {"m": "x"}).schema["a"].metadata == {"m": "x"}
    assert df.withMetadata("a", {"m": "x"}).withMetadata("a", {"n": "y"}).schema["a"].metadata == {"n": "y"}
    assert df.withMetadata("a", {"m": "x"}).withMetadata("a", {}).schema["a"].metadata == {}


def test_struct_field_from_null_struct_preserves_metadata(spark):
    schema = StructType(
        [
            StructField(
                "s",
                StructType(
                    [
                        StructField("id", IntegerType(), True, metadata={"comment": "the id"}),
                        StructField("name", StringType(), True),
                        StructField(
                            "inner",
                            StructType([StructField("val", IntegerType(), True, metadata={"m": "v"})]),
                            True,
                        ),
                    ]
                ),
                True,
            ),
        ]
    )
    df = spark.createDataFrame(
        [({"id": 1, "name": "alice", "inner": {"val": 10}},), (None,)],
        schema,
    )
    result = df.select(
        df.s.id.alias("id"),
        df.s["name"].alias("name"),
        df.s["id"].alias("id_bracket"),
        df.s.inner.val.alias("val"),
    )
    assert result.collect() == [
        Row(id=1, name="alice", id_bracket=1, val=10),
        Row(id=None, name=None, id_bracket=None, val=None),
    ]
    # Metadata must survive for dot access (`s.id`), bracket access (`s['id']`),
    # and nested access (`s.inner.val`) — the cases Spark preserves.
    assert result.schema["id"].metadata == {"comment": "the id"}
    assert result.schema["id_bracket"].metadata == {"comment": "the id"}
    assert result.schema["val"].metadata == {"m": "v"}


def test_struct_wildcard_from_null_struct_preserves_metadata(spark):
    schema = StructType(
        [
            StructField(
                "s",
                StructType(
                    [
                        StructField("id", IntegerType(), True, metadata={"comment": "the id"}),
                        StructField("name", StringType(), True),
                        StructField(
                            "inner",
                            StructType([StructField("val", IntegerType(), True, metadata={"m": "v"})]),
                            True,
                        ),
                    ]
                ),
                True,
            ),
        ]
    )
    df = spark.createDataFrame(
        [({"id": 1, "name": "alice", "inner": {"val": 10}},), (None,)],
        schema,
    )
    # Top-level wildcard `s.*` expands to the leaf field columns, preserving metadata.
    top = df.select("s.*")
    assert [(r["id"], r["name"]) for r in top.collect()] == [(1, "alice"), (None, None)]
    assert top.schema["id"].metadata == {"comment": "the id"}
    # Nested wildcard `s.inner.*` expands the inner struct; NULL parent yields NULL.
    nested = df.select("s.inner.*")
    assert nested.collect() == [Row(val=10), Row(val=None)]
    assert nested.schema["val"].metadata == {"m": "v"}


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
