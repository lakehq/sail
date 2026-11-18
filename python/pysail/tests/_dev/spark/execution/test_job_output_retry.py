# SPDX-License-Identifier: Apache-2.0

import time

import pyarrow as pa
import pytest

from pysail.testing.spark.session import spark_connect_server

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail local-cluster mode only")


@pytest.fixture(scope="module")
def remote():
    # The batches are small so that the rows of each batch travel through the stages
    # right away, instead of being combined into larger batches first.
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "1",
        "SAIL_CLUSTER__WORKER_MAX_COUNT": "1",
        "SAIL_CLUSTER__WORKER_TASK_SLOTS": "16",
        "SAIL_EXECUTION__BATCH_SIZE": "256",
    }
    with spark_connect_server(envs=envs) as server:
        yield server.remote


@pytest.mark.timeout(120)
@pytest.mark.parametrize("failure", ["writer_fails", "writer_fails_slow", "reader_fails"])
def test_job_fails_with_the_task_error_after_rows_have_reached_the_client(spark, tmp_path, failure):
    # All tasks of the query are retried together, since no stage waits for another one
    # to finish. When a task fails after the last stage has passed rows on to the client,
    # the new attempts cannot replace the rows that the client already has, so the job
    # fails. The error must be the one of the failed task in any case: whether the task
    # fails before or after the first rows have reached the client, which is open for
    # the writer that fails at once, and whichever attempt it is.
    emitted = tmp_path / "emitted"
    batches, rows = 40, 256
    poison = (batches - 1) * rows

    def fail_late(message):
        # Wait for the last stage to pass rows on, and then for them to reach the client.
        deadline = time.time() + 10
        while not emitted.exists() and time.time() < deadline:
            time.sleep(0.05)
        time.sleep(1)
        raise RuntimeError(message)

    def produce(_):
        for i in range(batches):
            if i == batches // 2:
                if failure == "writer_fails":
                    message = "forced failure: writer_fails"
                    raise RuntimeError(message)
                if failure == "writer_fails_slow":
                    fail_late("forced failure: writer_fails_slow")
            yield pa.record_batch([pa.array(range(i * rows, (i + 1) * rows), type=pa.int64())], names=["id"])

    def read(inputs):
        for batch in inputs:
            if failure == "reader_fails" and poison in batch.column(0).to_pylist():
                fail_late("forced failure: reader_fails")
            yield batch

    def emit(inputs):
        for batch in inputs:
            if batch.num_rows:
                emitted.touch()
            yield batch

    df = (
        spark.range(1, numPartitions=1)
        .mapInArrow(produce, "id long")
        .repartition(4)
        .mapInArrow(read, "id long")
        .repartition(4)
        .mapInArrow(emit, "id long")
    )

    with pytest.raises(Exception, match=f"forced failure: {failure}") as error:
        df.collect()

    assert "a different attempt has already produced job output" not in str(error.value)
