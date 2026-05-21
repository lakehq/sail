import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark import StorageLevel
from pyspark.sql import Row
from pyspark.sql.functions import col, lit

from pysail.testing.spark.utils.common import is_jvm_spark


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


def test_dataframe_local_checkpoint(spark):
    df = spark.createDataFrame(
        schema="id INT, value STRING",
        data=[(1, "a"), (2, "b")],
    )

    checkpointed = df.where(col("id") >= 1).localCheckpoint()

    assert_frame_equal(
        checkpointed.sort("id").toPandas(),
        pd.DataFrame({"id": [1, 2], "value": ["a", "b"]}).astype({"id": "int32"}),
    )
    target_id = 2
    assert_frame_equal(
        checkpointed.where(col("id") == target_id).select("value").toPandas(),
        pd.DataFrame({"value": ["b"]}),
    )


@pytest.mark.skipif(is_jvm_spark(), reason="JVM Spark Connect requires checkpoint dir at session startup")
def test_dataframe_checkpoint(spark, tmp_path):
    df = spark.createDataFrame(
        schema="id INT, value STRING",
        data=[(1, "a"), (2, "b")],
    )
    spark.conf.set("spark.checkpoint.dir", str(tmp_path / "checkpoints"))
    try:
        checkpointed = df.where(col("id") >= 1).checkpoint()

        assert_frame_equal(
            checkpointed.sort("id").toPandas(),
            pd.DataFrame({"id": [1, 2], "value": ["a", "b"]}).astype({"id": "int32"}),
        )
        assert_frame_equal(
            checkpointed.where(col("id") == 1).select("value").toPandas(),
            pd.DataFrame({"value": ["a"]}),
        )
    finally:
        spark.conf.unset("spark.checkpoint.dir")


@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific missing checkpoint dir coverage")
def test_dataframe_checkpoint_requires_directory(spark_session_factory):
    spark = spark_session_factory()
    df = spark.createDataFrame(
        schema="id INT",
        data=[(1,)],
    )

    with pytest.raises(Exception, match=r"spark\.checkpoint\.dir"):
        df.checkpoint()


@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific checkpoint path validation")
def test_dataframe_checkpoint_rejects_parent_path_components(spark, tmp_path):
    df = spark.createDataFrame(
        schema="id INT",
        data=[(1,)],
    )
    spark.conf.set("spark.checkpoint.dir", str(tmp_path / ".." / "checkpoints"))
    try:
        with pytest.raises(Exception, match="parent path"):
            df.checkpoint()
    finally:
        spark.conf.unset("spark.checkpoint.dir")


@pytest.mark.skipif(is_jvm_spark(), reason="Sail MVP limitation")
def test_dataframe_checkpoint_lazy_is_not_supported(spark):
    df = spark.createDataFrame(
        schema="id INT",
        data=[(1,)],
    )

    with pytest.raises(Exception, match="eager=false"):
        df.localCheckpoint(eager=False)


def test_dataframe_local_checkpoint_with_memory_storage_level(spark):
    df = spark.createDataFrame(
        schema="id INT, value STRING",
        data=[(1, "a"), (2, "b")],
    )

    checkpointed = df.localCheckpoint(storageLevel=StorageLevel.MEMORY_ONLY)

    assert_frame_equal(
        checkpointed.sort("id").toPandas(),
        pd.DataFrame({"id": [1, 2], "value": ["a", "b"]}).astype({"id": "int32"}),
    )


@pytest.mark.skipif(is_jvm_spark(), reason="Sail cache storage tier limitation")
def test_dataframe_local_checkpoint_disk_only_storage_level_is_not_supported(spark):
    df = spark.createDataFrame(
        schema="id INT",
        data=[(1,)],
    )

    with pytest.raises(Exception, match="without memory"):
        df.localCheckpoint(storageLevel=StorageLevel.DISK_ONLY)


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
