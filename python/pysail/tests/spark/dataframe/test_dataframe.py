import shutil

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark import StorageLevel
from pyspark.sql import Row
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.window import Window

from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark

_CHECKPOINT_SOURCE_MAX_ID = 2
_CHECKPOINT_SOURCE_ROW_COUNT = 2


def _read_checkpoint_source(spark, path):
    spark.createDataFrame(
        schema="id INT, value STRING",
        data=[(1, "a"), (2, "b"), (3, "c")],
    ).write.mode("overwrite").parquet(str(path))
    return spark.read.parquet(str(path)).where(col("id") <= _CHECKPOINT_SOURCE_MAX_ID)


def _assert_checkpoint_source_result(df):
    assert_frame_equal(
        df.sort("id").toPandas(),
        pd.DataFrame({"id": [1, 2], "value": ["a", "b"]}).astype({"id": "int32"}),
    )
    assert_frame_equal(
        df.where(col("id") == _CHECKPOINT_SOURCE_MAX_ID).select("value").toPandas(),
        pd.DataFrame({"value": ["b"]}),
    )


def _checkpoint_arrow_files(path):
    if not path.exists():
        return []
    return list(path.rglob("*.arrow"))


def _uuid_values(df):
    return [row.value for row in df.sort("id").collect()]


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


def test_dataframe_local_checkpoint_survives_source_removal(spark, tmp_path):
    source_path = tmp_path / "source"
    df = _read_checkpoint_source(spark, source_path)

    checkpointed = df.localCheckpoint()
    shutil.rmtree(source_path)

    _assert_checkpoint_source_result(checkpointed)


@pytest.mark.parametrize("eager", [True, False], ids=["eager", "lazy"])
def test_dataframe_local_checkpoint_freezes_nondeterministic_values(spark, eager):
    df = spark.sql("SELECT id, uuid() AS value FROM range(3)")
    assert _uuid_values(df) != _uuid_values(df)

    checkpointed = df.localCheckpoint(eager=eager)

    assert _uuid_values(checkpointed) == _uuid_values(checkpointed)


@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific physical plan names")
def test_dataframe_local_checkpoint_explain_truncates_plan(spark):
    df = spark.range(0, 3).withColumn("value", lit(1)).filter("id >= 0")
    original = normalize_plan_text(df._explain_string())  # noqa: SLF001
    checkpointed = df.localCheckpoint()
    checkpointed_plan = normalize_plan_text(checkpointed._explain_string())  # noqa: SLF001

    assert "RangeExec" in original
    assert "RangeExec" not in checkpointed_plan
    assert "DataSourceExec" in checkpointed_plan


@pytest.mark.skipif(is_jvm_spark(), reason="JVM Spark Connect requires checkpoint dir at session startup")
def test_dataframe_checkpoint(spark, tmp_path):
    source_path = tmp_path / "source"
    checkpoint_path = tmp_path / "checkpoints"
    df = _read_checkpoint_source(spark, source_path)
    spark.conf.set("spark.checkpoint.dir", str(checkpoint_path))
    try:
        checkpointed = df.checkpoint()
        assert _checkpoint_arrow_files(checkpoint_path)
        shutil.rmtree(source_path)

        _assert_checkpoint_source_result(checkpointed)
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


def test_dataframe_local_checkpoint_lazy_survives_source_removal_after_first_action(spark, tmp_path):
    source_path = tmp_path / "source"
    df = _read_checkpoint_source(spark, source_path)

    checkpointed = df.localCheckpoint(eager=False)
    assert checkpointed.count() == _CHECKPOINT_SOURCE_ROW_COUNT
    shutil.rmtree(source_path)

    _assert_checkpoint_source_result(checkpointed)


@pytest.mark.skipif(is_jvm_spark(), reason="JVM Spark Connect requires checkpoint dir at session startup")
def test_dataframe_checkpoint_lazy(spark, tmp_path):
    source_path = tmp_path / "source"
    checkpoint_path = tmp_path / "checkpoints"
    df = _read_checkpoint_source(spark, source_path)
    spark.conf.set("spark.checkpoint.dir", str(checkpoint_path))
    try:
        checkpointed = df.checkpoint(eager=False)
        assert not _checkpoint_arrow_files(checkpoint_path)
        assert checkpointed.count() == _CHECKPOINT_SOURCE_ROW_COUNT
        assert _checkpoint_arrow_files(checkpoint_path)
        shutil.rmtree(source_path)

        _assert_checkpoint_source_result(checkpointed)
    finally:
        spark.conf.unset("spark.checkpoint.dir")


@pytest.mark.parametrize(
    ("storage_level", "source_name"),
    [(StorageLevel.MEMORY_ONLY, "memory-only"), (StorageLevel.DISK_ONLY, "disk-only")],
    ids=["memory-only", "disk-only"],
)
def test_dataframe_local_checkpoint_storage_level_survives_source_removal(spark, tmp_path, storage_level, source_name):
    source_path = tmp_path / f"source-{source_name}"
    df = _read_checkpoint_source(spark, source_path)

    checkpointed = df.localCheckpoint(storageLevel=storage_level)
    shutil.rmtree(source_path)

    _assert_checkpoint_source_result(checkpointed)


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
