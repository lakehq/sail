import gc
import os
import shutil
import time

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark import StorageLevel
from pyspark.errors import PySparkException
from pyspark.sql.connect import plan as connect_plan
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.functions import col, lit

from pysail.testing.spark.session import spark_session_factory
from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

pytestmark = pytest.mark.skipif(
    pyspark_version() < (4,),
    reason="checkpoint and localCheckpoint require PySpark Connect 4+",
)


@pytest.fixture(name="spark_session_factory")
def spark_session_factory_fixture(remote):
    with spark_session_factory(remote) as sessions:
        yield sessions.create


def _wait_for_checkpoint_files(checkpoint_path, *, present):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        files_present = any(checkpoint_path.rglob("*.arrow"))
        if files_present == present:
            return
        time.sleep(0.1)
    assert any(checkpoint_path.rglob("*.arrow")) == present


def _cached_relation_id(dataframe):
    relation_id = dataframe._plan._relation_id  # noqa: SLF001
    assert isinstance(relation_id, str)
    assert relation_id
    return relation_id


def _wait_for_cached_relation_removal(spark, relation_id):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        plan = connect_plan.CachedRemoteRelation(relation_id, spark)
        probe = DataFrame(plan, spark)
        try:
            probe.count()
        except PySparkException as error:
            if f"No DataFrame with id {relation_id} is found" in str(error):
                return
            raise
        finally:
            plan._relation_id = None  # noqa: SLF001
        time.sleep(0.1)
    pytest.fail(f"cached relation {relation_id} was not removed")


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
        assert any(checkpoint_path.rglob("*.arrow"))
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
@pytest.mark.skipif(
    os.environ.get("SAIL_MODE") == "local-cluster",
    reason="memory object stores are scoped to one runtime and are not shared with workers",
)
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


@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific checkpoint materialization plan")
def test_dataframe_local_checkpoint_lazy_storage_materialization_state(spark):
    checkpointed = spark.range(0, 10, numPartitions=2).localCheckpoint(
        eager=False,
        storageLevel=StorageLevel.DISK_ONLY,
    )

    pending_plan = normalize_plan_text(checkpointed._explain_string())  # noqa: SLF001
    assert "PendingCachedRelationExec" in pending_plan

    assert checkpointed.count() == 10  # noqa: PLR2004
    materialized_plan = normalize_plan_text(checkpointed._explain_string())  # noqa: SLF001
    assert "PendingCachedRelationExec" not in materialized_plan
    assert "DataSourceExec" in materialized_plan


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
        assert not any(checkpoint_path.rglob("*.arrow"))
        assert checkpointed.count() == 2  # noqa: PLR2004
        assert any(checkpoint_path.rglob("*.arrow"))
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


@pytest.mark.skipif(is_jvm_spark(), reason="JVM Spark Connect requires checkpoint dir at session startup")
def test_dataframe_checkpoint_lazy_retains_checkpoint_dependency(spark, tmp_path):
    checkpoint_path = tmp_path / "checkpoints"
    spark.conf.set("spark.checkpoint.dir", str(checkpoint_path))
    try:
        first = spark.range(0, 10).checkpoint()
        second = first.checkpoint(eager=False)

        del first
        gc.collect()

        assert second.count() == 10  # noqa: PLR2004
        assert [row.id for row in second.sort("id").collect()] == list(range(10))
    finally:
        spark.conf.unset("spark.checkpoint.dir")


@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific lazy checkpoint explain behavior")
def test_dataframe_checkpoint_lazy_explain_does_not_materialize(spark, tmp_path):
    checkpoint_path = tmp_path / "checkpoints"
    df = spark.range(0, 5).where(col("id") <= 2)  # noqa: PLR2004
    spark.conf.set("spark.checkpoint.dir", str(checkpoint_path))
    try:
        checkpointed = df.checkpoint(eager=False)
        checkpointed_plan = normalize_plan_text(checkpointed._explain_string())  # noqa: SLF001

        assert "PendingCachedRelationExec" in checkpointed_plan
        assert not any(checkpoint_path.rglob("*.arrow"))

        assert checkpointed.count() == 3  # noqa: PLR2004
        assert any(checkpoint_path.rglob("*.arrow"))

        materialized_plan = normalize_plan_text(checkpointed._explain_string())  # noqa: SLF001
        assert "PendingCachedRelationExec" not in materialized_plan
        assert "DataSourceExec" in materialized_plan
    finally:
        spark.conf.unset("spark.checkpoint.dir")


@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific checkpoint session cleanup")
def test_dataframe_checkpoint_cleanup_on_session_stop(spark_session_factory, tmp_path):
    spark = spark_session_factory()
    checkpoint_path = tmp_path / "checkpoints"
    spark.conf.set("spark.checkpoint.dir", str(checkpoint_path))

    _checkpointed = spark.range(0, 5).checkpoint()
    assert any(checkpoint_path.rglob("*.arrow"))

    spark.stop()
    _wait_for_checkpoint_files(checkpoint_path, present=False)


@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific checkpoint DataFrame cleanup")
def test_dataframe_checkpoint_cleanup_on_dataframe_gc(spark, tmp_path):
    checkpoint_path = tmp_path / "checkpoints"
    spark.conf.set("spark.checkpoint.dir", str(checkpoint_path))
    try:
        checkpointed = spark.range(0, 5).checkpoint()
        assert any(checkpoint_path.rglob("*.arrow"))

        del checkpointed
        gc.collect()
        _wait_for_checkpoint_files(checkpoint_path, present=False)
    finally:
        spark.conf.unset("spark.checkpoint.dir")


def test_dataframe_local_checkpoint_gc_removes_cached_relation(spark):
    checkpointed = spark.range(0, 10).localCheckpoint()
    relation_id = _cached_relation_id(checkpointed)

    del checkpointed
    gc.collect()

    _wait_for_cached_relation_removal(spark, relation_id)


def test_dataframe_local_checkpoint_explicit_cleanup_removes_cached_relation(spark):
    checkpointed = spark.range(0, 10).localCheckpoint()
    relation_id = _cached_relation_id(checkpointed)
    relation = checkpointed._plan  # noqa: SLF001
    client = spark._client  # noqa: SLF001

    client.execute_command(connect_plan.RemoveRemoteCachedRelation(relation).command(client))
    relation._relation_id = None  # noqa: SLF001

    _wait_for_cached_relation_removal(spark, relation_id)


def test_dataframe_local_checkpoint_cleanup_waits_for_derived_dataframe(spark):
    checkpointed = spark.range(0, 10).localCheckpoint()
    relation_id = _cached_relation_id(checkpointed)
    derived = checkpointed.repartition(2)

    del checkpointed
    gc.collect()
    assert derived.count() == 10  # noqa: PLR2004

    del derived
    gc.collect()
    _wait_for_cached_relation_removal(spark, relation_id)


@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific derived checkpoint cleanup")
def test_dataframe_checkpoint_cleanup_waits_for_derived_dataframe(spark, tmp_path):
    checkpoint_path = tmp_path / "checkpoints"
    spark.conf.set("spark.checkpoint.dir", str(checkpoint_path))
    try:
        checkpointed = spark.range(0, 5).checkpoint()
        derived = checkpointed.repartition(2)
        assert any(checkpoint_path.rglob("*.arrow"))

        del checkpointed
        gc.collect()
        _wait_for_checkpoint_files(checkpoint_path, present=True)

        del derived
        gc.collect()
        _wait_for_checkpoint_files(checkpoint_path, present=False)
    finally:
        spark.conf.unset("spark.checkpoint.dir")


@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific checkpoint partition preservation")
@pytest.mark.parametrize("local", [True, False], ids=["local", "reliable"])
def test_dataframe_checkpoint_preserves_multiple_output_partitions(spark, tmp_path, local):
    df = spark.range(0, 1000, numPartitions=4).repartition(4)
    if local:
        checkpointed = df.localCheckpoint()
    else:
        checkpoint_path = tmp_path / "checkpoints"
        spark.conf.set("spark.checkpoint.dir", str(checkpoint_path))
        try:
            checkpointed = df.checkpoint()
        finally:
            spark.conf.unset("spark.checkpoint.dir")

    partition_ids = {row.pid for row in checkpointed.selectExpr("spark_partition_id() AS pid").distinct().collect()}
    assert partition_ids == set(range(4))


@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific checkpoint partition preservation")
@pytest.mark.parametrize("local", [True, False], ids=["local", "reliable"])
def test_dataframe_checkpoint_preserves_empty_partition_ids(spark, tmp_path, local):
    df = spark.range(0, 1, numPartitions=4)
    expected = df.selectExpr("id", "spark_partition_id() AS pid").collect()
    assert expected[0].pid != 0

    if local:
        checkpointed = df.localCheckpoint()
    else:
        checkpoint_path = tmp_path / "checkpoints"
        spark.conf.set("spark.checkpoint.dir", str(checkpoint_path))
        try:
            checkpointed = df.checkpoint()
        finally:
            spark.conf.unset("spark.checkpoint.dir")

    actual = checkpointed.selectExpr("id", "spark_partition_id() AS pid").collect()
    assert actual == expected


@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific checkpoint physical properties")
@pytest.mark.parametrize("eager", [True, False], ids=["eager", "lazy"])
@pytest.mark.parametrize("local", [True, False], ids=["local", "reliable"])
def test_dataframe_checkpoint_preserves_partitioning_and_ordering(spark, tmp_path, local, eager):
    df = (
        spark.range(0, 100, numPartitions=4)
        .withColumn("key", col("id") % 4)
        .repartition(4, "key")
        .sortWithinPartitions("key", "id")
    )
    if local:
        checkpointed = df.localCheckpoint(eager=eager)
    else:
        checkpoint_path = tmp_path / f"checkpoints-{eager}"
        spark.conf.set("spark.checkpoint.dir", str(checkpoint_path))
        try:
            checkpointed = df.checkpoint(eager=eager)
        finally:
            spark.conf.unset("spark.checkpoint.dir")

    assert checkpointed.count() == 100  # noqa: PLR2004
    aggregate_plan = normalize_plan_text(checkpointed.groupBy("key").count()._explain_string())  # noqa: SLF001
    ordered_plan = normalize_plan_text(checkpointed.sortWithinPartitions("key", "id")._explain_string())  # noqa: SLF001
    rows = checkpointed.selectExpr("id", "key", "spark_partition_id() AS pid").collect()

    assert "RepartitionExec" not in aggregate_plan
    assert "SortExec" not in ordered_plan
    for partition_id in range(4):
        partition_rows = [(row.key, row.id) for row in rows if row.pid == partition_id]
        assert partition_rows == sorted(partition_rows)


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


@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific local checkpoint validation")
def test_dataframe_local_checkpoint_rejects_excessive_replication(spark):
    storage_level = StorageLevel(True, False, False, False, 40)

    with pytest.raises(Exception, match="replication must be less than 40"):
        spark.range(0, 1).localCheckpoint(storageLevel=storage_level)
