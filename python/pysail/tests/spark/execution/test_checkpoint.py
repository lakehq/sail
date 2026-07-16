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
from pyspark.sql.connect.plan import Checkpoint

from pysail.testing.spark.session import (
    configure_spark_session,
    patch_spark_connect_session,
    spark_connect_server,
)
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


SMALL_PAYLOAD_ROWS = 16 * 1024
LARGE_PAYLOAD_ROWS = 64 * 1024
PAYLOAD_BYTES = 1024
DUPLICATE_COLUMN_ROWS = 3
CLEANUP_ROWS = 32
COMPLETED_JOB_PARTITIONS = 4


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
        f"repeat('x', {PAYLOAD_BYTES}) AS payload",
    )


def _assert_payload(checkpointed, rows):
    assert checkpointed.count() == rows
    actual = checkpointed.selectExpr("sum(length(payload)) AS payload_bytes").first()
    assert actual.payload_bytes == rows * PAYLOAD_BYTES


def _assert_large_payload(checkpointed, rows):
    actual = checkpointed.selectExpr(
        "count(*) AS row_count",
        "sum(length(payload)) AS payload_bytes",
    ).first()
    assert actual.row_count == rows
    assert actual.payload_bytes == rows * PAYLOAD_BYTES


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
    source = spark.range(DUPLICATE_COLUMN_ROWS, numPartitions=2).selectExpr(
        "id AS value",
        "id + 10 AS value",
    )

    checkpointed = source.localCheckpoint(eager=eager) if kind == "local" else source.checkpoint(eager=eager)

    assert [field.name for field in checkpointed.schema.fields] == ["value", "value"]
    assert checkpointed.count() == DUPLICATE_COLUMN_ROWS


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
    source = _payload_dataframe(spark, SMALL_PAYLOAD_ROWS, partitions=partitions)
    assert source.count() == SMALL_PAYLOAD_ROWS

    if storage_level is None:
        checkpointed = source.localCheckpoint(eager=eager)
    else:
        checkpointed = source.localCheckpoint(eager=eager, storageLevel=storage_level)

    _assert_payload(checkpointed, SMALL_PAYLOAD_ROWS)


# FIXME: Local checkpoint payloads are embedded in every worker task definition.
@SAIL_XFAIL
def test_local_checkpoint_large_payload_remains_executable_in_cluster(spark):
    checkpointed = _payload_dataframe(spark, LARGE_PAYLOAD_ROWS).localCheckpoint()

    _assert_large_payload(checkpointed, LARGE_PAYLOAD_ROWS)


def test_checkpoint_cleanup_is_scoped_to_one_relation(spark, tmp_path):
    if is_jvm_spark():
        # JVM Spark fixes the checkpoint root at server startup.
        safe = spark.range(CLEANUP_ROWS, numPartitions=4).checkpoint()
        assert safe.count() == CLEANUP_ROWS

        other = spark.range(1).checkpoint(eager=False)
        del other
        gc.collect()

        assert safe.count() == CLEANUP_ROWS
        return

    original_checkpoint_path = spark.conf.get("spark.checkpoint.dir")
    root = tmp_path / "checkpoint-scope"
    safe_root = f"{root.as_uri()}/safe"
    overlapping_root = f"{root.as_uri()}?scope=other"

    try:
        spark.conf.set("spark.checkpoint.dir", safe_root)
        safe = spark.range(CLEANUP_ROWS, numPartitions=4).checkpoint()
        assert safe.count() == CLEANUP_ROWS
        safe_files = list(root.rglob("*.arrow"))

        spark.conf.set("spark.checkpoint.dir", overlapping_root)
        with pytest.raises(PySparkException, match="checkpoint directory cannot contain"):
            spark.range(1).checkpoint(eager=False)

        assert all(path.exists() for path in safe_files)
        assert safe.count() == CLEANUP_ROWS
    finally:
        spark.conf.set("spark.checkpoint.dir", original_checkpoint_path)


# FIXME: Completed local-cluster jobs retain checkpoint relation leases after DataFrame GC.
@SAIL_XFAIL
def test_checkpoint_gc_releases_completed_cluster_job_lease(spark, tmp_path):
    if is_jvm_spark():
        checkpointed = spark.range(CLEANUP_ROWS, numPartitions=4).checkpoint()
        assert checkpointed.count() == CLEANUP_ROWS
        del checkpointed
        gc.collect()
        return

    original_checkpoint_path = spark.conf.get("spark.checkpoint.dir")
    checkpoint_root = tmp_path / "completed-job"
    try:
        spark.conf.set("spark.checkpoint.dir", checkpoint_root.as_uri())
        checkpointed = spark.range(
            CLEANUP_ROWS,
            numPartitions=COMPLETED_JOB_PARTITIONS,
        ).checkpoint()
        assert checkpointed.count() == CLEANUP_ROWS
        checkpoint_files = list(checkpoint_root.rglob("*.arrow"))
        assert len(checkpoint_files) == COMPLETED_JOB_PARTITIONS

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
