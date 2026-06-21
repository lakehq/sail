import shutil

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark import StorageLevel
from pyspark.sql.functions import col, lit

from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark


def test_dataframe_local_checkpoint_survives_source_removal(spark, tmp_path):
    source_path = tmp_path / "source"
    spark.createDataFrame(
        schema="id INT, value STRING",
        data=[(1, "a"), (2, "b"), (3, "c")],
    ).write.mode("overwrite").parquet(str(source_path))
    df = spark.read.parquet(str(source_path)).where(col("id") <= 2)  # noqa: PLR2004

    checkpointed = df.localCheckpoint()
    shutil.rmtree(source_path)

    assert_frame_equal(
        checkpointed.sort("id").toPandas(),
        pd.DataFrame({"id": [1, 2], "value": ["a", "b"]}).astype({"id": "int32"}),
    )
    assert_frame_equal(
        checkpointed.where(col("id") == 2).select("value").toPandas(),  # noqa: PLR2004
        pd.DataFrame({"value": ["b"]}),
    )


@pytest.mark.parametrize("eager", [True, False], ids=["eager", "lazy"])
def test_dataframe_local_checkpoint_freezes_nondeterministic_values(spark, eager):
    df = spark.sql("SELECT id, uuid() AS value FROM range(3)")
    assert [row.value for row in df.sort("id").collect()] != [row.value for row in df.sort("id").collect()]

    checkpointed = df.localCheckpoint(eager=eager)

    assert [row.value for row in checkpointed.sort("id").collect()] == [
        row.value for row in checkpointed.sort("id").collect()
    ]


@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific physical plan names")
def test_dataframe_local_checkpoint_explain_truncates_plan(spark):
    df = spark.range(0, 3).withColumn("value", lit(1)).filter("id >= 0")
    original = normalize_plan_text(df._explain_string())  # noqa: SLF001
    checkpointed = df.localCheckpoint()
    checkpointed_plan = normalize_plan_text(checkpointed._explain_string())  # noqa: SLF001

    assert "RangeExec" in original
    assert original.count("RangeExec") >= 1
    assert "RangeExec" not in checkpointed_plan
    assert checkpointed_plan.count("RangeExec") == 0
    assert checkpointed_plan.count("FilterExec") < 1
    assert checkpointed_plan.count("ProjectionExec") < 3  # noqa: PLR2004
    assert "DataSourceExec" in checkpointed_plan
    assert checkpointed_plan.count("DataSourceExec") <= 2  # noqa: PLR2004


@pytest.mark.skipif(is_jvm_spark(), reason="JVM Spark Connect requires checkpoint dir at session startup")
def test_dataframe_checkpoint(spark, tmp_path):
    source_path = tmp_path / "source"
    checkpoint_path = tmp_path / "checkpoints"
    spark.createDataFrame(
        schema="id INT, value STRING",
        data=[(1, "a"), (2, "b"), (3, "c")],
    ).write.mode("overwrite").parquet(str(source_path))
    df = spark.read.parquet(str(source_path)).where(col("id") <= 2)  # noqa: PLR2004
    spark.conf.set("spark.checkpoint.dir", str(checkpoint_path))
    try:
        checkpointed = df.checkpoint()
        assert list(checkpoint_path.rglob("*.arrow"))
        shutil.rmtree(source_path)

        assert_frame_equal(
            checkpointed.sort("id").toPandas(),
            pd.DataFrame({"id": [1, 2], "value": ["a", "b"]}).astype({"id": "int32"}),
        )
        assert_frame_equal(
            checkpointed.where(col("id") == 2).select("value").toPandas(),  # noqa: PLR2004
            pd.DataFrame({"value": ["b"]}),
        )
    finally:
        spark.conf.unset("spark.checkpoint.dir")


@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific object-store checkpoint URL")
def test_dataframe_checkpoint_with_memory_object_store(spark, tmp_path):
    source_path = tmp_path / "source"
    spark.createDataFrame(
        schema="id INT, value STRING",
        data=[(1, "a"), (2, "b"), (3, "c")],
    ).write.mode("overwrite").parquet(str(source_path))
    df = spark.read.parquet(str(source_path)).where(col("id") <= 2)  # noqa: PLR2004
    spark.conf.set("spark.checkpoint.dir", "memory:///dataframe-checkpoint-test")
    try:
        checkpointed = df.checkpoint()
        checkpointed_plan = normalize_plan_text(checkpointed._explain_string())  # noqa: SLF001
        assert "DataSourceExec" in checkpointed_plan

        shutil.rmtree(source_path)
        assert_frame_equal(
            checkpointed.sort("id").toPandas(),
            pd.DataFrame({"id": [1, 2], "value": ["a", "b"]}).astype({"id": "int32"}),
        )
        assert_frame_equal(
            checkpointed.where(col("id") == 2).select("value").toPandas(),  # noqa: PLR2004
            pd.DataFrame({"value": ["b"]}),
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


def test_dataframe_local_checkpoint_lazy_survives_source_removal_after_first_action(spark, tmp_path):
    source_path = tmp_path / "source"
    spark.createDataFrame(
        schema="id INT, value STRING",
        data=[(1, "a"), (2, "b"), (3, "c")],
    ).write.mode("overwrite").parquet(str(source_path))
    df = spark.read.parquet(str(source_path)).where(col("id") <= 2)  # noqa: PLR2004

    checkpointed = df.localCheckpoint(eager=False)
    assert checkpointed.count() == 2  # noqa: PLR2004
    shutil.rmtree(source_path)

    assert_frame_equal(
        checkpointed.sort("id").toPandas(),
        pd.DataFrame({"id": [1, 2], "value": ["a", "b"]}).astype({"id": "int32"}),
    )
    assert_frame_equal(
        checkpointed.where(col("id") == 2).select("value").toPandas(),  # noqa: PLR2004
        pd.DataFrame({"value": ["b"]}),
    )


@pytest.mark.skipif(is_jvm_spark(), reason="JVM Spark Connect requires checkpoint dir at session startup")
def test_dataframe_checkpoint_lazy(spark, tmp_path):
    source_path = tmp_path / "source"
    checkpoint_path = tmp_path / "checkpoints"
    spark.createDataFrame(
        schema="id INT, value STRING",
        data=[(1, "a"), (2, "b"), (3, "c")],
    ).write.mode("overwrite").parquet(str(source_path))
    df = spark.read.parquet(str(source_path)).where(col("id") <= 2)  # noqa: PLR2004
    spark.conf.set("spark.checkpoint.dir", str(checkpoint_path))
    try:
        checkpointed = df.checkpoint(eager=False)
        assert not list(checkpoint_path.rglob("*.arrow"))
        assert checkpointed.count() == 2  # noqa: PLR2004
        assert list(checkpoint_path.rglob("*.arrow"))
        shutil.rmtree(source_path)

        assert_frame_equal(
            checkpointed.sort("id").toPandas(),
            pd.DataFrame({"id": [1, 2], "value": ["a", "b"]}).astype({"id": "int32"}),
        )
        assert_frame_equal(
            checkpointed.where(col("id") == 2).select("value").toPandas(),  # noqa: PLR2004
            pd.DataFrame({"value": ["b"]}),
        )
    finally:
        spark.conf.unset("spark.checkpoint.dir")


@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific physical plan names and stack configuration")
def test_dataframe_local_checkpoint_deep_plan_explain_truncates(spark):
    df = spark.range(0, 10)
    for i in range(20):
        df = df.withColumn(f"value_{i}", col("id") + lit(i)).filter(col("id") >= 0)

    checkpointed = df.localCheckpoint()
    checkpointed_plan = normalize_plan_text(checkpointed._explain_string())  # noqa: SLF001

    assert checkpointed.count() == 10  # noqa: PLR2004
    assert "RangeExec" not in checkpointed_plan
    assert checkpointed_plan.count("RangeExec") == 0
    assert checkpointed_plan.count("FilterExec") < 1
    assert checkpointed_plan.count("ProjectionExec") < 3  # noqa: PLR2004
    assert "DataSourceExec" in checkpointed_plan
    assert checkpointed_plan.count("DataSourceExec") <= 2  # noqa: PLR2004


@pytest.mark.parametrize(
    ("storage_level", "source_name"),
    [(StorageLevel.MEMORY_ONLY, "memory-only"), (StorageLevel.DISK_ONLY, "disk-only")],
    ids=["memory-only", "disk-only"],
)
def test_dataframe_local_checkpoint_storage_level_survives_source_removal(spark, tmp_path, storage_level, source_name):
    source_path = tmp_path / f"source-{source_name}"
    spark.createDataFrame(
        schema="id INT, value STRING",
        data=[(1, "a"), (2, "b"), (3, "c")],
    ).write.mode("overwrite").parquet(str(source_path))
    df = spark.read.parquet(str(source_path)).where(col("id") <= 2)  # noqa: PLR2004

    checkpointed = df.localCheckpoint(storageLevel=storage_level)
    shutil.rmtree(source_path)

    assert_frame_equal(
        checkpointed.sort("id").toPandas(),
        pd.DataFrame({"id": [1, 2], "value": ["a", "b"]}).astype({"id": "int32"}),
    )
    assert_frame_equal(
        checkpointed.where(col("id") == 2).select("value").toPandas(),  # noqa: PLR2004
        pd.DataFrame({"value": ["b"]}),
    )
