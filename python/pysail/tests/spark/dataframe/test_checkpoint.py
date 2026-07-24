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
    with spark_connect_server(
        envs={"SAIL_EXECUTION__CHECKPOINT__PATH": checkpoint_root.as_uri()}
    ) as server:
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


def _weakly_connected_components(nodes, pairs, *, checkpoint):
    forward = pairs.select(sf.col("a").alias("source"), sf.col("b").alias("target"))
    reverse = pairs.select(sf.col("b").alias("source"), sf.col("a").alias("target"))
    edges = forward.unionByName(reverse)
    labels = nodes.select(sf.col("id").alias("node")).withColumn("label", sf.col("node"))

    for _ in range(100):
        neighbor_labels = labels.select(
            sf.col("node").alias("target"),
            sf.col("label").alias("target_label"),
        )
        neighbor_minimum = (
            edges.join(neighbor_labels, on="target")
            .groupBy("source")
            .agg(sf.min("target_label").alias("neighbor_minimum"))
            .select(sf.col("source").alias("node"), "neighbor_minimum")
        )
        next_labels = labels.join(neighbor_minimum, on="node", how="left").select(
            "node",
            sf.least(
                "label",
                sf.coalesce("neighbor_minimum", "label"),
            ).alias("label"),
        )
        previous_labels = labels.select("node", sf.col("label").alias("previous_label"))
        changed = (
            next_labels.join(previous_labels, on="node")
            .where(sf.col("label") != sf.col("previous_label"))
            .limit(1)
            .count()
        )
        labels = next_labels
        if changed == 0:
            break
        if checkpoint:
            labels = labels.checkpoint()
    else:
        pytest.fail("weakly connected components did not converge")

    return labels


def _component_partition(labels):
    components = {}
    for row in labels.collect():
        components.setdefault(row.label, set()).add(row.node)
    return {frozenset(members) for members in components.values()}


@pytest.mark.yamlsnapshot(group="plan")
def test_checkpoint_supports_iterative_weakly_connected_components(spark, snapshot):
    nodes = spark.range(7)
    pairs = spark.createDataFrame(
        [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)],
        "a LONG, b LONG",
    )

    expected = {frozenset(range(6)), frozenset({6})}
    baseline = _weakly_connected_components(nodes, pairs, checkpoint=False)
    checkpointed = _weakly_connected_components(nodes, pairs, checkpoint=True)
    checkpointed_plan = normalize_plan_text(checkpointed._explain_string())  # noqa: SLF001

    assert _component_partition(checkpointed) == _component_partition(baseline) == expected
    assert checkpointed_plan == snapshot


@pytest.mark.parametrize("local", [False, True], ids=["checkpoint", "local-checkpoint"])
@pytest.mark.yamlsnapshot(group="plan")
def test_checkpoint_preserves_partitioning_and_ordering(spark, local, snapshot):
    source = (
        spark.range(100, numPartitions=4)
        .withColumn("key", sf.col("id") % 4)
        .repartition(4, "key")
        .sortWithinPartitions("key", "id")
    )

    checkpointed = source.localCheckpoint() if local else source.checkpoint()
    aggregate_plan = normalize_plan_text(checkpointed.groupBy("key").count()._explain_string())  # noqa: SLF001
    ordered_plan = normalize_plan_text(checkpointed.sortWithinPartitions("key", "id")._explain_string())  # noqa: SLF001
    consumer_plans = "\n".join(
        [
            f"== Grouped Aggregate From Checkpoint ==\n{aggregate_plan}",
            f"== Sort Within Partitions From Checkpoint ==\n{ordered_plan}",
        ]
    )

    assert consumer_plans == snapshot


def test_checkpoint_rejects_unimplemented_fallback_semantics(spark):
    source = spark.range(3)

    with pytest.raises(Exception, match="lazy DataFrame checkpoint"):
        source.checkpoint(eager=False)
    with pytest.raises(Exception, match="StorageLevel"):
        source.localCheckpoint(storageLevel=StorageLevel.DISK_ONLY)
