import os
import time

import pandas as pd
import pyarrow as pa
import pytest
from pyspark.sql import Row

from pysail.testing.spark.session import spark_connect_server

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail local-cluster mode only")


@pytest.fixture(
    scope="module",
    params=[
        (None, None, None, None),
        ("none", 1, 65535, None),
        ("lz4", 3, None, 1048576),
        ("zstd", 4, 4194304, 16777216),
    ],
    ids=["default", "none-one-connection", "lz4-three-connections", "zstd-four-connections"],
)
def remote(request):
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__TASK_STREAM_BUFFER": "1",
        "SAIL_EXECUTION__BATCH_SIZE": "256",
    }
    compression, connection_count, stream_window_size, connection_window_size = request.param
    if compression is not None:
        envs["SAIL_CLUSTER__SHUFFLE_BACKEND__FLIGHT__COMPRESSION"] = compression
    if connection_count is not None:
        envs["SAIL_CLUSTER__SHUFFLE_BACKEND__FLIGHT__CONNECTION_COUNT"] = str(connection_count)
    if stream_window_size is not None:
        envs["SAIL_CLUSTER__SHUFFLE_BACKEND__FLIGHT__INITIAL_STREAM_WINDOW_SIZE"] = str(stream_window_size)
    if connection_window_size is not None:
        envs["SAIL_CLUSTER__SHUFFLE_BACKEND__FLIGHT__INITIAL_CONNECTION_WINDOW_SIZE"] = str(connection_window_size)
    with spark_connect_server(envs=envs) as server:
        yield server.remote


@pytest.mark.timeout(15)
@pytest.mark.parametrize("partitions", [2, 4])
@pytest.mark.parametrize("join_type", ["left_semi", "left_anti"])
def test_shuffle_finishes_when_join_leaves_probe_partitions_unread(spark, partitions, join_type):
    # Only one build partition contains a row. The other join partitions finish
    # without opening their probe inputs. Multiple range batches overflow the
    # one-batch shuffle buffers, including those belonging to unread channels.
    build = spark.range(1, numPartitions=1).repartition(partitions, "id")
    probe = spark.range(10_000, numPartitions=1).repartition(partitions, "id")

    rows = build.join(probe, "id", join_type).collect()

    assert rows == ([Row(id=0)] if join_type == "left_semi" else [])


def _task_stop_times(spark):
    """Returns the stages of the latest join job of this session and, for each stage,
    when its last task stopped."""
    session = spark.sql(
        "SELECT session_id FROM system.session.sessions WHERE client_session_id = :id",
        args={"id": spark.session_id},
    ).collect()
    assert len(session) == 1
    args = {"id": session[0].session_id}
    stages = (
        spark.sql("SELECT job_id, stage, inputs FROM system.execution.stages WHERE session_id = :id", args=args)
        .toArrow()
        .to_pylist()
    )
    tasks = (
        spark.sql(
            "SELECT job_id, stage, status, created_at, stopped_at FROM system.execution.tasks WHERE session_id = :id",
            args=args,
        )
        .toArrow()
        .to_pylist()
    )
    job = max(stage["job_id"] for stage in stages if len(stage["inputs"]) >= 2)  # noqa: PLR2004
    stages = {stage["stage"]: stage for stage in stages if stage["job_id"] == job}
    tasks = [task for task in tasks if task["job_id"] == job]
    assert all(task["status"] == "SUCCEEDED" for task in tasks)
    started = min(task["created_at"] for task in tasks)
    stopped = {
        stage: max((task["stopped_at"] - started).total_seconds() for task in tasks if task["stage"] == stage)
        for stage in stages
    }
    return stages, stopped


@pytest.mark.timeout(30)
def test_shuffle_producers_finish_without_waiting_for_unread_channels(spark):
    # Only one build partition contains a row, so the other join partitions finish
    # without opening their probe inputs, while the remaining one is still busy.
    # The tasks that wrote the probe side must not stay around until the join stage ends.
    delay = 2.0

    def slow(batches):
        for batch in batches:
            if batch.num_rows:
                time.sleep(delay)
            yield batch

    build = spark.range(1, numPartitions=1).repartition(4, "id")
    probe = spark.range(10_000, numPartitions=1).repartition(4, "id")

    rows = build.join(probe, "id", "left_semi").mapInArrow(slow, "id long").collect()

    assert rows == [Row(id=0)]
    stages, stopped = _task_stop_times(spark)
    (join,) = [stage for stage in stages.values() if len(stage["inputs"]) == 2]  # noqa: PLR2004
    assert stopped[join["stage"]] >= delay
    for producer in join["inputs"]:
        assert stopped[producer["stage"]] < stopped[join["stage"]] - delay / 2


@pytest.mark.timeout(30)
def test_shuffle_producer_stops_after_all_readers_leave_without_opening(spark, tmp_path):
    # Every join partition has an empty build side and finishes without opening its
    # probe input, while another branch keeps the stage alive. The probe side must
    # notice that nobody will read it instead of being produced in full.
    total, delay = 60, 0.02

    def produce(batches):
        for _ in batches:
            for i in range(total):
                (tmp_path / f"{i:04d}").touch()
                time.sleep(delay)
                yield pa.record_batch([pa.array(range(i * 256, (i + 1) * 256), type=pa.int64())], names=["id"])

    def slow(batches):
        time.sleep(3 * total * delay)
        yield from batches

    build = spark.range(1, numPartitions=1).where("id < 0").repartition(4, "id")
    # The rows are distributed without a key so that every output channel of every
    # probe task receives data and observes that its reader has left.
    probe = spark.range(1, numPartitions=1).mapInArrow(produce, "id long").repartition(4)
    other = spark.range(4, numPartitions=1).mapInArrow(slow, "id long")

    rows = build.join(probe, "id", "left_semi").unionAll(other).collect()

    assert sorted(row.id for row in rows) == [0, 1, 2, 3]
    assert len(list(tmp_path.iterdir())) < total // 2


@pytest.mark.timeout(30)
@pytest.mark.parametrize("join_type", ["inner", "left_semi"])
def test_shuffle_finishes_when_producer_starts_after_its_readers_are_done(spark, join_type):
    # The broadcast build side is empty, so every join task finishes at once without
    # opening its probe input. The probe side scans a wide local table whose rows travel
    # inside the task definition, so its task only starts after all its readers and the
    # rest of the job are done, when the streams of its stage have already been cleaned up.
    # It then writes more batches than its readers would have buffered.
    count = 100_000
    probe = spark.createDataFrame(pd.DataFrame({f"c{i}": range(i, count + i) for i in range(20)}))
    small = spark.createDataFrame(pd.DataFrame({"id": range(100), "tag": [i % 7 for i in range(100)]}))
    build = small.alias("s").join(small.alias("t"), "id").where("t.tag = 99").select("id")

    rows = probe.join(build, probe.c0 == build.id, join_type).groupBy().count().collect()

    assert rows == [Row(count=0)]


@pytest.mark.timeout(30)
def test_shuffle_finishes_when_region_is_retried_after_stage_cleanup(spark, tmp_path):
    # The inner join reads a broadcast side with one replica for each of the 20 partitions
    # of the union. The 10 partitions of the other branch never read theirs, and the tasks
    # of the outer join, whose own broadcast side is empty, finish without opening theirs.
    # The streams of the broadcast side are cleaned up when the union has finished.
    # The only task of the final stage then fails once, all tasks run again, and the
    # broadcast side is written again to replicas that nobody reads or removes anymore,
    # unless its streams are cleaned up once more.
    marker = tmp_path / "failed"

    def fail_once(batches):
        first = not marker.exists()
        if first:
            time.sleep(2)
        for batch in batches:
            if first and batch.num_rows and not marker.exists():
                marker.touch()
                message = "forced failure of the first attempt"
                raise RuntimeError(message)
            yield batch

    probe = spark.sql("SELECT id AS y FROM range(0, 400000, 1, 10)")
    broadcast = spark.sql("SELECT id AS k FROM range(0, 60000) GROUP BY id")
    empty = spark.sql("SELECT id AS e FROM range(0, 100) WHERE id < 0 GROUP BY id")
    inner = probe.join(broadcast, probe.y % 90000 == broadcast.k).select("y")
    outer = inner.join(empty, inner.y == empty.e).select("y")
    other = spark.sql("SELECT id AS y FROM range(0, 8, 1, 10)")

    rows = outer.unionAll(other).repartition(1).mapInArrow(fail_once, "y long").collect()

    assert sorted(row.y for row in rows) == list(range(8))
    assert marker.exists()


@pytest.mark.timeout(60)
def test_ordered_merge_finishes_when_partitions_are_read_one_after_another(spark):
    # Each partition holds a contiguous range of the sort key, so the final merge reads
    # the partitions one after another while the other partitions stay open and unread.
    # The payload does not compress well and every partition sends several megabytes,
    # which is more than the unread streams of one worker may hold back on a connection.
    count = 1_600_000
    rows = spark.sql(f"""
        SELECT id, pad FROM (
            SELECT id, sha2(CAST(id AS STRING), 512) AS pad FROM range(0, {count}, 1, 40)
        ) ORDER BY id LIMIT 5 OFFSET {count - 10}
    """).collect()  # noqa: S608

    assert [row.id for row in rows] == list(range(count - 10, count - 5))


@pytest.mark.timeout(15)
def test_hash_join_preserves_global_build_limit(spark):
    build = spark.range(16, numPartitions=4).limit(3)
    probe = spark.range(16, numPartitions=4)

    assert len(build.join(probe, "id", "left").collect()) == 3  # noqa: PLR2004


@pytest.mark.timeout(30)
def test_shuffle_preserves_rows_across_multiple_batches(spark):
    rows = [Row(id=i, payload="shuffle" * 100 if i % 3 else None) for i in range(2048)]
    actual = spark.createDataFrame(rows).repartition(8, "id").orderBy("id").collect()

    assert actual == rows


@pytest.mark.timeout(60)
def test_shuffle_remains_usable_after_repeated_limits(spark):
    row_count, limit = 100_000, 10
    shuffled = spark.range(row_count, numPartitions=8).repartition(16, "id")
    for _ in range(8):
        rows = shuffled.limit(limit).collect()
        assert len(rows) == limit
        assert len({row.id for row in rows}) == limit
        assert all(0 <= row.id < row_count for row in rows)

    # A later query must still be able to fully consume the shared Flight connections.
    assert shuffled.groupBy().sum("id").first()[0] == row_count * (row_count - 1) // 2


@pytest.mark.timeout(30)
@pytest.mark.parametrize(
    ("input_sizes", "partitions", "completed_sizes"),
    [
        ([64] * 32 + [13], 4, [256, 256]),
        ([64, 1024, 32], 2, [256, 256]),
        ([128, 128, 128, 1024], 2, [192, 512]),
        ([1024], 2, [512]),
        ([0, 13, 0], 4, []),
        ([0], 4, []),
    ],
    ids=[
        "small-batches",
        "multiple-completed-batches",
        "flush-before-bypass",
        "large-batch-bypass",
        "residual",
        "empty",
    ],
)
@pytest.mark.spark(cross_version=True)
def test_shuffle_reader_coalesces_batches_in_channel_order(spark, input_sizes, partitions, completed_sizes):
    def make_batches(batches):
        for _ in batches:
            offset = 0
            for size in input_sizes:
                yield pa.record_batch([pa.array(range(offset, offset + size), type=pa.int64())], names=["id"])
                offset += size

    def observe_batches(batches):
        # Capture boundaries and row order before another operator can batch the output.
        rows = [batch.column(0).to_pylist() for batch in batches]
        yield pa.record_batch([pa.array([rows], type=pa.list_(pa.list_(pa.int64())))], names=["batches"])

    result = (
        spark.range(1, numPartitions=1)
        .mapInArrow(make_batches, "id long")
        .repartition(partitions)
        .mapInArrow(observe_batches, "batches array<array<long>>")
        .collect()
    )

    expected = []
    for partition in range(partitions):
        ids = list(range(partition, sum(input_sizes), partitions))
        batches = []
        offset = 0
        for size in completed_sizes:
            batches.append(ids[offset : offset + size])
            offset += size
        if offset < len(ids):
            batches.append(ids[offset:])
        expected.append(batches)
    assert sorted(row.batches for row in result) == sorted(expected)


@pytest.mark.timeout(30)
@pytest.mark.spark(cross_version=True)
def test_shuffle_reader_coalesces_batches_across_producers(spark):
    def make_batches(batches):
        for batch in batches:
            for producer in batch.column(0).to_pylist():
                start = producer * 130
                yield pa.record_batch([pa.array(range(start, start + 130), type=pa.int64())], names=["id"])

    def observe_batches(batches):
        rows = [batch.column(0).to_pylist() for batch in batches]
        yield pa.record_batch([pa.array([rows], type=pa.list_(pa.list_(pa.int64())))], names=["batches"])

    result = (
        spark.range(4, numPartitions=4)
        .mapInArrow(make_batches, "id long")
        .repartition(2)
        .mapInArrow(observe_batches, "batches array<array<long>>")
        .collect()
    )

    # Each producer contributes only 65 rows per destination. A 256-row batch
    # therefore requires combining producers after the reader merges them.
    assert sorted([len(batch) for batch in row.batches] for row in result) == [[256, 4], [256, 4]]
    assert sorted(value for row in result for batch in row.batches for value in batch) == list(range(520))


@pytest.mark.timeout(30)
def test_shuffle_preserves_values_larger_than_the_grpc_default_limit(spark):
    # Random bytes do not compress, and a single value cannot be split across Flight
    # messages, so each message exceeds tonic's 4 MiB default decoding limit.
    value = os.urandom(5 * 1024 * 1024)
    result = spark.createDataFrame([(value,), (value,)], "v binary").repartition(2).collect()

    assert [bytes(row.v) for row in result] == [value, value]
