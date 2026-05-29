import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark import StorageLevel
from pyspark.sql import Row
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


def test_with_metadata(spark):
    df = spark.sql("SELECT 1 AS a")
    assert df.schema["a"].metadata == {}
    assert df.withMetadata("a", {"m": "x"}).schema["a"].metadata == {"m": "x"}
    assert df.withMetadata("a", {"m": "x"}).withMetadata("a", {"n": "y"}).schema["a"].metadata == {"n": "y"}
    assert df.withMetadata("a", {"m": "x"}).withMetadata("a", {}).schema["a"].metadata == {}


def test_dataframe_semantic_analysis(spark):
    df1 = spark.range(10)
    df2 = spark.range(10)

    assert df1.withColumn("col1", df1.id * 2).sameSemantics(df2.withColumn("col1", df2.id * 2))
    assert not df1.withColumn("col1", df1.id * 2).sameSemantics(df2.withColumn("col1", df2.id + 2))
    assert df1.withColumn("col1", df1.id * 2).sameSemantics(df2.withColumn("col0", df2.id * 2))
    assert df1.selectExpr("id as col0").semanticHash() == df2.selectExpr("id as col1").semanticHash()


def test_dataframe_extended_explain_sections(spark):
    explain = spark.sql("SELECT 1")._explain_string(extended=True)  # noqa: SLF001

    assert "Parsed Logical Plan" in explain
    assert "Analyzed Logical Plan" in explain
    assert "Optimized Logical Plan" in explain
    assert "Physical Plan" in explain


def test_dataframe_drop_duplicates_within_watermark_batch_error(spark):
    with pytest.raises(
        Exception,
        match="dropDuplicatesWithinWatermark is not supported with batch DataFrames/DataSets",
    ):
        spark.range(1).dropDuplicatesWithinWatermark(["id"]).collect()


def test_dataframe_storage_level_state(spark):
    df = spark.range(1)
    try:
        assert df.storageLevel == StorageLevel.NONE

        df.cache()
        assert df.storageLevel == StorageLevel.MEMORY_AND_DISK_DESER

        df.unpersist()
        assert df.storageLevel == StorageLevel.NONE

        df.persist(StorageLevel.DISK_ONLY)
        assert df.storageLevel == StorageLevel.DISK_ONLY
    finally:
        df.unpersist()


def test_dataframe_stat_approx_quantile_shape(spark):
    df = spark.createDataFrame([(1, 2), (2, 4), (3, 6)], ["a", "b"])

    result = df.stat.approxQuantile(["a", "b"], [0.1, 0.5, 0.9], 0.1)

    assert len(result) == 2
    assert len(result[0]) == 3
    assert len(result[1]) == 3
