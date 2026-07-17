import contextlib
import gc
import shutil
import time
import uuid

import pytest
from pyspark import StorageLevel
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession
from pyspark.sql.connect import proto
from pyspark.sql.connect.plan import CachedRemoteRelation, Checkpoint, RemoveRemoteCachedRelation

from pysail.testing.spark.session import (
    configure_spark_session,
    patch_spark_connect_session,
    spark_connect_server,
)
from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

pytestmark = [
    pytest.mark.skipif(
        pyspark_version() < (4,),
        reason="checkpoint and localCheckpoint require PySpark Connect 4+",
    ),
]

SAIL_XFAIL = pytest.mark.xfail(
    not is_jvm_spark(),
    reason="Known Sail checkpoint parity bug",
    strict=True,
)


@contextlib.contextmanager
def _checkpoint_spark_session(remote, checkpoint_path):
    builder = (
        SparkSession.builder.appName(f"test-{uuid.uuid4().hex}")
        .remote(remote)
        .config("spark.checkpoint.dir", str(checkpoint_path))
    )
    session = builder.getOrCreate() if remote.startswith("local") else builder.create()
    configure_spark_session(session)
    patch_spark_connect_session(session)
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture(scope="module")
def remote():
    envs = None if is_jvm_spark() else {"SAIL_MODE": "local-cluster"}
    with spark_connect_server(envs=envs) as server:
        yield server.remote


@pytest.fixture(scope="module")
def checkpoint_path(tmp_path_factory):
    return tmp_path_factory.mktemp("execution-checkpoints")


@pytest.fixture(scope="module")
def spark(remote, checkpoint_path):
    with _checkpoint_spark_session(remote, checkpoint_path) as session:
        yield session


def _payload_dataframe(spark, rows, partitions=4):
    return spark.range(rows, numPartitions=partitions).selectExpr(
        "id",
        "repeat('x', 1024) AS payload",
    )


def _assert_payload(checkpointed, rows):
    assert checkpointed.count() == rows
    actual = checkpointed.selectExpr("sum(length(payload)) AS payload_bytes").first()
    assert actual.payload_bytes == rows * 1024


def _assert_large_payload(checkpointed, rows):
    actual = checkpointed.selectExpr(
        "count(*) AS row_count",
        "sum(length(payload)) AS payload_bytes",
    ).first()
    assert actual.row_count == rows
    assert actual.payload_bytes == rows * 1024


def _checkpoint(dataframe, kind, *, eager=True):
    if kind == "local":
        return dataframe.localCheckpoint(eager=eager)
    return dataframe.checkpoint(eager=eager)


def _remove_cached_relation(spark, dataframe):
    relation = dataframe._plan  # noqa: SLF001
    client = spark._client  # noqa: SLF001
    client.execute_command(RemoveRemoteCachedRelation(relation).command(client))
    relation._relation_id = None  # noqa: SLF001


def _hash_shuffle_count(plan):
    if is_jvm_spark():
        return plan.count("Exchange hashpartitioning")
    return plan.count("RepartitionExec: partitioning=Hash")


@pytest.fixture(scope="module")
def retry_spark(spark, checkpoint_path):
    if is_jvm_spark():
        yield spark
        return

    # The retry state is embedded-only, so close the cluster session before switching servers.
    gc.collect()
    spark.stop()
    with (
        spark_connect_server(envs={"SAIL_MODE": "local"}) as server,
        _checkpoint_spark_session(server.remote, checkpoint_path / "retry") as session,
    ):
        yield session


def _assert_checkpoint_preserves_duplicate_column_names(spark, eager, kind):
    source = spark.range(3, numPartitions=2).selectExpr(
        "id AS value",
        "id + 10 AS value",
    )

    checkpointed = source.localCheckpoint(eager=eager) if kind == "local" else source.checkpoint(eager=eager)

    assert [field.name for field in checkpointed.schema.fields] == ["value", "value"]
    assert checkpointed.count() == 3  # noqa: PLR2004


@pytest.mark.parametrize("eager", [True, False], ids=["eager", "lazy"])
@pytest.mark.parametrize("kind", ["local", "reliable"])
def test_checkpoint_preserves_duplicate_column_names(spark, eager, kind):
    _assert_checkpoint_preserves_duplicate_column_names(spark, eager, kind)


@pytest.mark.parametrize("eager", [True, False], ids=["eager", "lazy"])
@pytest.mark.parametrize("partitions", [1, 2, 4, 8, 10])
@pytest.mark.parametrize(
    "storage_level",
    [
        pytest.param(None, id="default"),
        pytest.param(StorageLevel.MEMORY_ONLY, id="memory-only"),
        pytest.param(StorageLevel.DISK_ONLY, id="disk-only"),
        pytest.param(StorageLevel.MEMORY_AND_DISK, id="memory-and-disk"),
    ],
)
def test_local_checkpoint_preserves_rows_after_cache_repartition(spark, eager, partitions, storage_level):
    source = _payload_dataframe(spark, 16 * 1024, partitions=partitions)
    assert source.count() == 16 * 1024

    if storage_level is None:
        checkpointed = source.localCheckpoint(eager=eager)
    else:
        checkpointed = source.localCheckpoint(eager=eager, storageLevel=storage_level)

    try:
        _assert_payload(checkpointed, 16 * 1024)
    finally:
        _remove_cached_relation(spark, checkpointed)


# FIXME: In local-cluster, this 64 MiB local checkpoint is embedded in every worker task definition and
# serializes to 135,869,682 bytes, exceeding the 128 MiB gRPC decoding limit. A 132 MiB checkpoint
# serialized to 280,225,527 bytes and raised peak process RSS to about 3.29 GB.
@SAIL_XFAIL
def test_local_checkpoint_large_payload_remains_executable_in_cluster(spark):
    checkpointed = _payload_dataframe(spark, 64 * 1024).localCheckpoint()

    _assert_large_payload(checkpointed, 64 * 1024)


def test_checkpoint_cleanup_is_scoped_to_one_relation(spark, tmp_path):
    if is_jvm_spark():
        # JVM Spark fixes the checkpoint root at server startup.
        safe = spark.range(32, numPartitions=4).checkpoint()
        assert safe.count() == 32  # noqa: PLR2004

        other = spark.range(1).checkpoint(eager=False)
        del other
        gc.collect()

        assert safe.count() == 32  # noqa: PLR2004
        return

    original_checkpoint_path = spark.conf.get("spark.checkpoint.dir")
    root = tmp_path / "checkpoint-scope"
    safe_root = f"{root.as_uri()}/safe"
    overlapping_root = f"{root.as_uri()}?scope=other"

    try:
        spark.conf.set("spark.checkpoint.dir", safe_root)
        safe = spark.range(32, numPartitions=4).checkpoint()
        assert safe.count() == 32  # noqa: PLR2004
        safe_files = list(root.rglob("*.arrow"))

        spark.conf.set("spark.checkpoint.dir", overlapping_root)
        with pytest.raises(PySparkException, match="checkpoint directory cannot contain"):
            spark.range(1).checkpoint(eager=False)

        assert all(path.exists() for path in safe_files)
        assert safe.count() == 32  # noqa: PLR2004
    finally:
        spark.conf.set("spark.checkpoint.dir", original_checkpoint_path)


# FIXME: In local-cluster, a completed job retains the checkpoint relation lease after DataFrame GC. All
# four Arrow files remain after the five-second cleanup window and are released only when the session stops.
@SAIL_XFAIL
def test_checkpoint_gc_releases_completed_cluster_job_lease(spark, tmp_path):
    if is_jvm_spark():
        checkpointed = spark.range(32, numPartitions=4).checkpoint()
        assert checkpointed.count() == 32  # noqa: PLR2004
        del checkpointed
        gc.collect()
        return

    original_checkpoint_path = spark.conf.get("spark.checkpoint.dir")
    checkpoint_root = tmp_path / "completed-job"
    try:
        spark.conf.set("spark.checkpoint.dir", checkpoint_root.as_uri())
        checkpointed = spark.range(32, numPartitions=4).checkpoint()
        assert checkpointed.count() == 32  # noqa: PLR2004
        checkpoint_files = list(checkpoint_root.rglob("*.arrow"))
        assert len(checkpoint_files) == 4  # noqa: PLR2004

        del checkpointed
        gc.collect()
        deadline = time.monotonic() + 5
        while any(path.exists() for path in checkpoint_files) and time.monotonic() < deadline:
            time.sleep(0.1)

        assert all(not path.exists() for path in checkpoint_files)
    finally:
        spark.conf.set("spark.checkpoint.dir", original_checkpoint_path)


@SAIL_XFAIL
def test_checkpoint_command_can_reattach(spark):
    client = spark._client  # noqa: SLF001
    command = Checkpoint(spark.range(3)._plan, local=True, eager=False).command(client)  # noqa: SLF001
    request = client._execute_plan_request_with_metadata()  # noqa: SLF001
    request.operation_id = str(uuid.uuid4())
    request.plan.command.CopyFrom(command)
    request.request_options.append(
        proto.ExecutePlanRequest.RequestOption(
            reattach_options=proto.ReattachOptions(reattachable=True),
        )
    )

    call = client._stub.ExecutePlan(request, metadata=client._builder.metadata())  # noqa: SLF001
    first = next(iter(call))
    assert first.HasField("checkpoint_command_result")
    call.cancel()

    reattach = proto.ReattachExecuteRequest(
        session_id=request.session_id,
        client_observed_server_side_session_id=first.server_side_session_id,
        user_context=request.user_context,
        operation_id=request.operation_id,
        client_type=request.client_type,
        last_response_id=first.response_id,
    )
    responses = list(
        client._stub.ReattachExecute(  # noqa: SLF001
            reattach,
            metadata=client._builder.metadata(),  # noqa: SLF001
        )
    )

    assert any(response.HasField("result_complete") for response in responses)


def test_checkpoint_command_preserves_operation_id(spark):
    client = spark._client  # noqa: SLF001
    command = Checkpoint(spark.range(3)._plan, local=True, eager=True).command(client)  # noqa: SLF001
    operation_id = str(uuid.uuid4())
    request = client._execute_plan_request_with_metadata()  # noqa: SLF001
    request.operation_id = operation_id
    request.plan.command.CopyFrom(command)

    responses = list(client._stub.ExecutePlan(request, metadata=client._builder.metadata()))  # noqa: SLF001

    assert responses
    assert all(response.operation_id == operation_id for response in responses)
    result = next(
        response.checkpoint_command_result for response in responses if response.HasField("checkpoint_command_result")
    )
    relation = CachedRemoteRelation(result.relation.relation_id, spark)
    try:
        client.execute_command(RemoveRemoteCachedRelation(relation).command(client))
    finally:
        relation._relation_id = None  # noqa: SLF001


def test_local_checkpoint_does_not_write_reliable_checkpoint_directory(spark, checkpoint_path):
    files_before = {path.relative_to(checkpoint_path) for path in checkpoint_path.rglob("*") if path.is_file()}

    checkpointed = spark.range(0, 32, numPartitions=4).localCheckpoint(
        eager=True,
        storageLevel=StorageLevel.DISK_ONLY,
    )

    assert checkpointed.count() == 32  # noqa: PLR2004
    files_after = {path.relative_to(checkpoint_path) for path in checkpoint_path.rglob("*") if path.is_file()}
    assert not files_after.difference(files_before)


@pytest.mark.parametrize("eager", [True, False], ids=["eager", "lazy"])
@pytest.mark.parametrize("kind", ["local", "reliable"])
def test_checkpoint_preserves_exact_partition_count(spark, eager, kind):
    source = spark.range(0, 1000, numPartitions=4).repartition(4)

    checkpointed = _checkpoint(source, kind, eager=eager)

    partition_ids = {row.pid for row in checkpointed.selectExpr("spark_partition_id() AS pid").distinct().collect()}
    assert partition_ids == set(range(4))


@pytest.mark.parametrize("eager", [True, False], ids=["eager", "lazy"])
@pytest.mark.parametrize("kind", ["local", "reliable"])
def test_checkpoint_preserves_rows_and_order_within_each_partition(spark, eager, kind):
    source = (
        spark.range(0, 100, numPartitions=4)
        .selectExpr("id", "id % 4 AS key")
        .repartition(4)
        .sortWithinPartitions("key", "id")
    )

    checkpointed = _checkpoint(source, kind, eager=eager)
    rows = checkpointed.selectExpr("id", "key", "spark_partition_id() AS pid").collect()

    assert len(rows) == 100  # noqa: PLR2004
    assert {row.pid for row in rows} == set(range(4))
    for partition_id in range(4):
        partition_rows = [(row.key, row.id) for row in rows if row.pid == partition_id]
        assert partition_rows == sorted(partition_rows)


@pytest.mark.parametrize("eager", [True, False], ids=["eager", "lazy"])
@pytest.mark.parametrize("kind", ["local", "reliable"])
def test_checkpoint_scalar_subquery_does_not_survive_plan_truncation(spark, eager, kind):
    source = spark.sql("SELECT id FROM range(1000) WHERE id = (SELECT max(id) FROM range(10))")

    checkpointed = _checkpoint(source, kind, eager=eager)
    assert [row.id for row in checkpointed.collect()] == [9]
    checkpoint_plan = normalize_plan_text(checkpointed._explain_string())  # noqa: SLF001

    assert "Subquery" not in checkpoint_plan
    assert [row.id for row in checkpointed.collect()] == [9]


@pytest.mark.parametrize("eager", [True, False], ids=["eager", "lazy"])
@pytest.mark.parametrize("kind", ["local", "reliable"])
def test_checkpointed_aggregate_uses_preserved_hash_partitioning(spark, eager, kind):
    source = spark.range(0, 128, numPartitions=4).selectExpr("id", "id % 16 AS key").repartition(4, "key")

    checkpointed = _checkpoint(source, kind, eager=eager)
    assert checkpointed.count() == 128  # noqa: PLR2004
    aggregate = checkpointed.groupBy("key").count()
    rows = aggregate.sort("key").collect()

    assert [(row.key, row["count"]) for row in rows] == [(key, 8) for key in range(16)]
    if not is_jvm_spark():
        plan = normalize_plan_text(aggregate._explain_string())  # noqa: SLF001
        assert "RepartitionExec" not in plan


@pytest.mark.parametrize(
    ("right_kind", "right_partitions", "sail_hash_shuffles"),
    [("checkpoint", 4, 0), ("checkpoint", 3, 1), ("plain", None, 1)],
    ids=["compatible", "incompatible", "unpartitioned-sibling"],
)
def test_checkpointed_join_inputs_keep_independent_partitioning(
    spark, right_kind, right_partitions, sail_hash_shuffles
):
    original_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    try:
        left = (
            spark.range(0, 64, numPartitions=4)
            .selectExpr("id AS key", "id AS left_value")
            .repartition(4, "key")
            .checkpoint()
        )
        right = spark.range(0, 64, numPartitions=4).selectExpr("id AS key", "id AS right_value")
        if right_kind == "checkpoint":
            right = right.repartition(right_partitions, "key").checkpoint()

        joined = left.join(right, "key").select("key", "left_value", "right_value")
        plan = normalize_plan_text(joined._explain_string())  # noqa: SLF001

        assert joined.count() == 64  # noqa: PLR2004
        expected_hash_shuffles = 2 if is_jvm_spark() else sail_hash_shuffles
        assert _hash_shuffle_count(plan) == expected_hash_shuffles
    finally:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", original_threshold)


@pytest.mark.parametrize("eager", [True, False], ids=["eager", "lazy"])
@pytest.mark.parametrize("kind", ["local", "reliable"])
def test_embedded_checkpoint_preserves_duplicate_column_names(retry_spark, eager, kind):
    _assert_checkpoint_preserves_duplicate_column_names(retry_spark, eager, kind)


@pytest.mark.parametrize("kind", ["local", "reliable"])
def test_lazy_checkpoint_can_retry_after_source_recovers(retry_spark, tmp_path, kind):
    row_count = 50_000
    source_path = tmp_path / f"source-{kind}"
    retry_spark.range(row_count, numPartitions=8).write.parquet(str(source_path))
    source = retry_spark.read.parquet(str(source_path))

    checkpointed = source.localCheckpoint(eager=False) if kind == "local" else source.checkpoint(eager=False)

    source_files = sorted(source_path.glob("*.parquet"))
    assert len(source_files) > 1
    missing = source_files[len(source_files) // 2]
    hidden = tmp_path / f"{kind}-{missing.name}.missing"
    shutil.move(missing, hidden)
    try:
        with pytest.raises(PySparkException):
            checkpointed.count()
    finally:
        shutil.move(hidden, missing)

    assert source.count() == row_count
    assert checkpointed.count() == row_count
