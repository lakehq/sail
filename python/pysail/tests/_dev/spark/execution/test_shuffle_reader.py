import pyarrow as pa
import pytest

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
        # The shuffle writer passes each input batch on, so that the tests observe how
        # the shuffle reader combines batches.
        "SAIL_CLUSTER__SHUFFLE_WRITE_BUFFER_SIZE": "1",
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


@pytest.mark.timeout(30)
@pytest.mark.parametrize(
    ("input_sizes", "partitions", "completed_sizes"),
    [
        ([64] * 32 + [13], 4, [256, 256]),
        ([64, 1024, 32], 2, [256, 256]),
        ([128, 128, 128, 1024], 2, [192, 256, 256]),
        ([600], 2, [150, 150]),
        ([96, 96, 512, 32], 1, [192, 512]),
        ([0, 13, 0], 4, []),
        ([0], 4, []),
    ],
    ids=[
        "small-batches",
        "multiple-completed-batches",
        "flush-before-bypass",
        "large-batch-bypass",
        "large-batch-bypass-single-channel",
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
