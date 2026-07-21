import shutil

import pytest
from pyspark import StorageLevel
from pyspark.sql import functions as sf

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import pyspark_version

pytestmark = pytest.mark.skipif(
    pyspark_version() < (4,),
    reason="checkpoint and localCheckpoint require PySpark Connect 4+",
)


@pytest.fixture(scope="module")
def checkpoint_root(tmp_path_factory):
    return tmp_path_factory.mktemp("object-store-checkpoints")


@pytest.fixture(scope="module")
def remote(checkpoint_root):
    with spark_connect_server(envs={"SAIL_CHECKPOINT__ROOT": checkpoint_root.as_uri()}) as server:
        yield server.remote


@pytest.mark.parametrize("local", [False, True], ids=["checkpoint", "local-checkpoint"])
def test_eager_checkpoint_is_an_object_store_snapshot(spark, checkpoint_root, tmp_path, local):
    source_path = tmp_path / "source"
    spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "id INT, value STRING").write.parquet(str(source_path))
    source = spark.read.parquet(str(source_path)).where("id <= 2")
    files_before = {path for path in checkpoint_root.rglob("*") if path.is_file()}

    checkpointed = source.localCheckpoint() if local else source.checkpoint()

    checkpoint_files = {path for path in checkpoint_root.rglob("*") if path.is_file()} - files_before
    assert checkpoint_files
    assert all(path.suffix == ".parquet" for path in checkpoint_files)

    shutil.rmtree(source_path)
    assert checkpointed.orderBy("id").collect() == [(1, "a"), (2, "b")]
    assert checkpointed.where("id = 2").select("value").collect() == [("b",)]


def test_checkpoint_preserves_duplicate_column_names(spark):
    checkpointed = (
        spark.range(3, numPartitions=2)
        .selectExpr(
            "id AS value",
            "id + 10 AS value",
        )
        .checkpoint()
    )

    assert [field.name for field in checkpointed.schema.fields] == ["value", "value"]
    assert sorted(tuple(row) for row in checkpointed.collect()) == [
        (0, 10),
        (1, 11),
        (2, 12),
    ]


def test_checkpoint_preserves_rows_with_no_columns(spark):
    checkpointed = spark.range(3, numPartitions=2).select().checkpoint()

    assert checkpointed.schema.simpleString() == "struct<>"
    assert checkpointed.collect() == [(), (), ()]


def test_checkpoint_preserves_field_metadata(spark):
    checkpointed = (
        spark.range(1).select(sf.col("id").alias("value", metadata={"source": "checkpoint-test"})).checkpoint()
    )

    assert checkpointed.schema["value"].metadata == {"source": "checkpoint-test"}


@pytest.mark.parametrize("local", [False, True], ids=["checkpoint", "local-checkpoint"])
def test_checkpoint_preserves_partitioning_and_ordering(spark, local):
    source = (
        spark.range(100, numPartitions=4)
        .withColumn("key", sf.col("id") % 4)
        .repartition(4, "key")
        .sortWithinPartitions("key", "id")
    )

    checkpointed = source.localCheckpoint() if local else source.checkpoint()
    aggregate_plan = normalize_plan_text(checkpointed.groupBy("key").count()._explain_string())  # noqa: SLF001
    ordered_plan = normalize_plan_text(checkpointed.sortWithinPartitions("key", "id")._explain_string())  # noqa: SLF001

    assert "RepartitionExec" not in aggregate_plan
    assert "SortExec" not in ordered_plan


def test_checkpoint_rejects_unimplemented_fallback_semantics(spark):
    source = spark.range(3)

    with pytest.raises(Exception, match="lazy DataFrame checkpoint"):
        source.checkpoint(eager=False)
    with pytest.raises(Exception, match="StorageLevel"):
        source.localCheckpoint(storageLevel=StorageLevel.DISK_ONLY)
