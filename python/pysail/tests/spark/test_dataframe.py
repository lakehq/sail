import math
from datetime import date

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import col, lit


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


def test_fillna_replaces_nan_in_double_column(spark):
    """fillna(0) should replace NaN values in DoubleType columns (Spark parity)."""
    schema = T.StructType([T.StructField("value", T.DoubleType(), True)])
    df = spark.createDataFrame([(float("nan"),), (2.5,), (None,)], schema)
    result = df.fillna(0).collect()
    # NaN and NULL should both be replaced with 0.0
    assert result[0]["value"] == 0.0, f"NaN should be replaced with 0.0, got {result[0]['value']}"
    assert result[1]["value"] == 2.5
    assert result[2]["value"] == 0.0, f"None should be replaced with 0.0, got {result[2]['value']}"


def test_fillna_replaces_nan_in_float_column(spark):
    """fillna(0) should replace NaN values in FloatType columns (Spark parity)."""
    schema = T.StructType([T.StructField("value", T.FloatType(), True)])
    df = spark.createDataFrame([(float("nan"),), (1.5,)], schema)
    result = df.fillna(0).collect()
    assert result[0]["value"] == 0.0, f"NaN should be replaced with 0.0, got {result[0]['value']}"
    assert result[1]["value"] == pytest.approx(1.5)


def test_fillna_does_not_replace_nan_in_non_float_column(spark):
    """fillna(0) should not affect non-NaN values."""
    schema = T.StructType([T.StructField("value", T.DoubleType(), True)])
    df = spark.createDataFrame([(1.5,), (None,)], schema)
    result = df.fillna(0).collect()
    assert result[0]["value"] == 1.5
    assert result[1]["value"] == 0.0


def test_withcolumn_dotted_name_creates_top_level_column(spark):
    """withColumn with a dotted name should create a new top-level column (Spark parity)."""
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), False),
            T.StructField(
                "struct_col",
                T.StructType([T.StructField("nested_field", T.StringType(), True)]),
                True,
            ),
        ]
    )
    df = spark.createDataFrame([(1, ("x",))], schema)
    result = df.withColumn("struct_col.nested_field", lit("y")).collect()
    # The original struct column should be unchanged
    assert result[0]["struct_col"]["nested_field"] == "x"
    # A new top-level column with the dotted name should be added
    assert result[0]["struct_col.nested_field"] == "y"


def test_concat_ws_coerces_non_string_columns(spark):
    """concat_ws should coerce non-string column types to string (Spark parity)."""
    schema = T.StructType(
        [
            T.StructField("string_col", T.StringType(), True),
            T.StructField("int_col", T.IntegerType(), True),
            T.StructField("date_col", T.DateType(), True),
        ]
    )
    df = spark.createDataFrame([("a", 1, date(2024, 1, 15))], schema)
    result = df.select(F.concat_ws("-", F.col("string_col"), F.col("int_col"), F.col("date_col"))).collect()
    assert result[0][0] == "a-1-2024-01-15"


def test_concat_ws_coerces_null_non_string_columns(spark):
    """concat_ws should skip null non-string values (Spark parity)."""
    schema = T.StructType(
        [
            T.StructField("string_col", T.StringType(), True),
            T.StructField("int_col", T.IntegerType(), True),
        ]
    )
    df = spark.createDataFrame([("a", None)], schema)
    result = df.select(F.concat_ws("-", F.col("string_col"), F.col("int_col"))).collect()
    assert result[0][0] == "a"
