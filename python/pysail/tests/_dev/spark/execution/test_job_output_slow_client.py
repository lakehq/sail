import pytest
from pyspark.sql import Row

from pysail.testing.spark.session import spark_connect_server

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail local-cluster mode only")


@pytest.fixture(scope="module")
def remote():
    # One worker with fewer task slots than two queries need at the same time.
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "1",
        "SAIL_CLUSTER__WORKER_MAX_COUNT": "1",
        "SAIL_CLUSTER__WORKER_TASK_SLOTS": "7",
        "SAIL_CLUSTER__TASK_LAUNCH_TIMEOUT_SECS": "5",
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": "4",
    }
    with spark_connect_server(envs=envs) as server:
        yield server.remote


@pytest.mark.timeout(60)
def test_tasks_do_not_keep_their_slots_while_the_client_reads_the_result(spark):
    # The result is much larger than what is buffered on its way to the client, and the
    # client has only read a small part of it when it runs another query. The tasks of
    # the first query must have released their task slots by then, since the second
    # query needs more slots than the first one leaves.
    count = 1_600_000
    result = spark.sql(f"""
        SELECT id, sha2(CAST(id AS STRING), 512) AS pad FROM range(0, {count}, 1, 4)
    """).toLocalIterator()
    try:
        rows = 0
        for _ in result:
            rows += 1
            if rows == 2000:  # noqa: PLR2004
                break

        other = spark.sql("""
            SELECT id % 3 AS k, count(*) AS c FROM range(0, 100000, 1, 4) GROUP BY id % 3 ORDER BY k
        """).collect()

        assert other == [Row(k=0, c=33334), Row(k=1, c=33333), Row(k=2, c=33333)]
        # The rest of the result is still available.
        assert rows + sum(1 for _ in result) == count
    finally:
        # Do not leave a half-read query behind for the other tests and the teardown.
        spark.interruptAll()
