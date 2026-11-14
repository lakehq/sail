import time

import pyarrow as pa
import pytest

from pysail.testing.spark.session import spark_connect_server

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail local-cluster mode only")


@pytest.fixture(scope="module")
def remote():
    # All tasks run on one worker, so the readers take the data directly from the
    # memory of the writer and no data is in transit between workers.
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "1",
        "SAIL_CLUSTER__WORKER_TASK_SLOTS": "64",
        "SAIL_CLUSTER__TASK_STREAM_BUFFER": "1",
        "SAIL_EXECUTION__BATCH_SIZE": "256",
    }
    with spark_connect_server(envs=envs) as server:
        yield server.remote


@pytest.mark.timeout(60)
def test_shuffle_writer_waits_for_slow_readers(spark, tmp_path):
    # The writer could produce everything at once, while each reader takes a while
    # for every batch. The writer must not get far ahead of the readers.
    batches, rows, readers = 200, 256, 2

    def produce(_):
        for i in range(batches):
            (tmp_path / f"{i:04d}").touch()
            yield pa.record_batch([pa.array(range(i * rows, (i + 1) * rows), type=pa.int64())], names=["id"])

    def consume(inputs):
        ahead, seen = 0, 0
        for batch in inputs:
            seen += batch.num_rows
            produced = len(list(tmp_path.iterdir())) * rows // readers
            ahead = max(ahead, produced - seen)
            time.sleep(0.02)
        yield pa.record_batch([pa.array([ahead]), pa.array([seen])], names=["ahead", "seen"])

    result = (
        spark.range(1, numPartitions=1)
        .mapInArrow(produce, "id long")
        .repartition(readers)
        .mapInArrow(consume, "ahead long, seen long")
        .collect()
    )

    assert [row.seen for row in result] == [batches * rows // readers] * readers
    assert max(row.ahead for row in result) < batches * rows // readers // 4
