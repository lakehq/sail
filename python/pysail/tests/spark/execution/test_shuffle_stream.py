import pyarrow as pa
import pytest
from pyspark.sql import Row

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")


@pytest.fixture(scope="module", params=[None, "none", "lz4", "zstd"], ids=["default", "none", "lz4", "zstd"])
def remote(request):
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__TASK_STREAM_BUFFER": "1",
        "SAIL_EXECUTION__BATCH_SIZE": "256",
    }
    if request.param is not None:
        envs["SAIL_CLUSTER__SHUFFLE_BACKEND__FLIGHT__COMPRESSION"] = request.param
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


@pytest.mark.timeout(30)
def test_shuffle_preserves_rows_across_multiple_batches(spark):
    rows = [Row(id=i, payload="shuffle" * 100 if i % 3 else None) for i in range(2048)]
    actual = spark.createDataFrame(rows).repartition(8, "id").orderBy("id").collect()

    assert actual == rows


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
