import pytest
from pyspark.sql import Row

from pysail.testing.spark.session import spark_connect_server

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail local-cluster mode only")

BATCH_SIZE = 256
PARTITIONS = 4


@pytest.fixture(scope="module", params=[True, False], ids=["hash-join", "sort-merge-join"])
def remote(request):
    # All tasks run on one worker, so the readers take the data directly from the
    # memory of the writer. Nothing is in transit between workers, where data could
    # stay for a writer that would otherwise have to wait for a reader forever.
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "1",
        "SAIL_CLUSTER__WORKER_TASK_SLOTS": "64",
        "SAIL_CLUSTER__TASK_STREAM_BUFFER": "1",
        "SAIL_CLUSTER__TASK_STREAM_MEMORY_LIMIT": "1",
        "SAIL_EXECUTION__BATCH_SIZE": str(BATCH_SIZE),
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": str(PARTITIONS),
        "SAIL_OPTIMIZER__PREFER_HASH_JOIN": str(request.param).lower(),
    }
    with spark_connect_server(envs=envs) as server:
        yield server.remote


@pytest.mark.timeout(60)
def test_ordered_merge_finishes_when_one_of_its_inputs_has_no_rows_until_the_end(spark):
    # The sorted rows of one task are distributed batch by batch to the tasks of the
    # filter, which keep the order, so that the final merge reads them in the order of
    # the rows. The filter removes all rows of one of its tasks. The merge cannot take
    # any row of the other tasks before that task has seen the end of its input, which
    # comes only after the first task has written all the rows for the other tasks.
    count, limit, offset = 400_000, 300_000, 200_000
    rows = spark.sql(f"""
        SELECT x FROM (SELECT id AS x FROM range(0, {count}) ORDER BY id LIMIT {limit}) AS t
        WHERE (x div {BATCH_SIZE}) % {PARTITIONS} <> 0
        ORDER BY x LIMIT 5 OFFSET {offset}
    """).collect()  # noqa: S608

    expected = [x for x in range(limit) if (x // BATCH_SIZE) % PARTITIONS != 0]
    assert [row.x for row in rows] == expected[offset : offset + 5]


@pytest.mark.timeout(60)
def test_join_finishes_when_both_inputs_are_sorted_before_they_are_partitioned(spark):
    # Each side is sorted by one task and partitioned by the join key afterwards. A
    # sort-merge join then relies on the order of the rows in each partition, and reads
    # either side depending on the keys that it has seen.
    count, limit = 400_000, 300_000
    rows = spark.sql(f"""
        SELECT count(*) AS n
        FROM (SELECT id AS k FROM range(0, {count}) ORDER BY id LIMIT {limit}) AS a
        JOIN (SELECT id * 7 AS k FROM range(0, {count}) ORDER BY k LIMIT {limit}) AS b ON a.k = b.k
    """).collect()  # noqa: S608

    assert rows == [Row(n=len(range(0, limit, 7)))]


@pytest.mark.timeout(60)
def test_outer_join_finishes_when_the_join_below_it_ends_without_reading_an_input(spark):
    # The inner join has a single row on one side, so most of its tasks end without
    # reading the other side, while the outer join on top still reads its other input.
    count = 200_000
    rows = spark.sql(f"""
        SELECT count(*) AS n, count(a.k) AS a, count(c.k) AS c
        FROM (SELECT id AS k FROM range(0, 1)) AS a
        JOIN (SELECT id AS k FROM range(0, {count})) AS b ON a.k = b.k
        FULL OUTER JOIN (SELECT id AS k FROM range(0, {count})) AS c ON a.k = c.k
    """).collect()  # noqa: S608

    assert rows == [Row(n=count, a=1, c=count)]


@pytest.mark.timeout(60)
@pytest.mark.parametrize("plain_branch", [False, True], ids=["two-joins", "join-and-scan"])
def test_union_finishes_when_only_one_branch_reads_a_broadcast_input(spark, plain_branch):
    # The build side of each broadcast join is written once for every task of the stage
    # that contains the union, but only the tasks of one branch ever read it. The build
    # side has more batches than a task can queue for a reader before it waits.
    build = 4000
    first = [i % 7 for i in range(200_000) if i % 6000 < build]
    if plain_branch:
        second = [i % 5 for i in range(150_000)]
        branch = "SELECT id % 5 AS v FROM range(0, 150000)"
    else:
        second = [i % 5 for i in range(150_000) if i % 8000 < build]
        branch = f"""
            SELECT b.id % 5 AS v FROM range(0, 150000) AS b
            JOIN (SELECT id AS k FROM range(0, {build}) GROUP BY id) AS t ON b.id % 8000 = t.k
        """  # noqa: S608
    rows = spark.sql(f"""
        SELECT count(*) AS n, sum(v) AS total FROM (
            SELECT a.id % 7 AS v FROM range(0, 200000) AS a
            JOIN (SELECT id AS k FROM range(0, {build}) GROUP BY id) AS s ON a.id % 6000 = s.k
            UNION ALL
            {branch}
        ) AS u
    """).collect()  # noqa: S608

    assert rows == [Row(n=len(first) + len(second), total=sum(first) + sum(second))]


@pytest.mark.timeout(60)
def test_union_finishes_when_its_branches_are_shuffled_afterwards(spark):
    build = 4000
    values = (
        [i % 11 for i in range(100_000)]
        + [i % 11 for i in range(80_000)]
        + [i % 11 for i in range(60_000) if i % 6000 < build]
    )
    rows = spark.sql(f"""
        SELECT v, count(*) AS n FROM (
            SELECT id % 11 AS v FROM range(0, 100000)
            UNION ALL
            SELECT k % 11 AS v FROM (SELECT id AS k, count(*) AS c FROM range(0, 80000) GROUP BY id) AS g
            UNION ALL
            SELECT a.id % 11 AS v FROM range(0, 60000) AS a
            JOIN (SELECT id AS k FROM range(0, {build}) GROUP BY id) AS s ON a.id % 6000 = s.k
        ) AS u
        GROUP BY v ORDER BY v
    """).collect()  # noqa: S608

    assert rows == [Row(v=v, n=values.count(v)) for v in range(11)]
