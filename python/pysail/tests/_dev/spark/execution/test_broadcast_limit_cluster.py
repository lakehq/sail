import json
import os

import pytest
from pyspark.sql.functions import udf

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.fixture(scope="module")
def remote(request):
    envs = {"SAIL_MODE": "local-cluster", **getattr(request, "param", {})}
    with spark_connect_server(envs=envs) as server:
        yield server.remote


pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail cluster mode")


def test_limit_read_by_every_task_of_a_join_keeps_the_same_rows(spark):
    # Each probe partition contains every build key, so compare the selected key sets.
    query = (
        "SELECT b.probe_partition, a.id "
        "FROM (SELECT id FROM range(0, 200, 1, 4) LIMIT 50) a "
        "JOIN (SELECT id % 200 AS id, CAST(floor(id / 200) AS INT) AS probe_partition "
        "FROM range(0, 800, 1, 4)) b ON a.id = b.id"
    )
    for _ in range(20):
        selected = [set() for _ in range(4)]
        for row in spark.sql(query).collect():
            selected[row.probe_partition].add(row.id)
        assert len(selected[0]) == 50  # noqa: PLR2004
        assert all(keys == selected[0] for keys in selected)


def _broadcast_query(build, *, values=False, input_rows=200):
    fields = "a.id, a.value" if values else "a.id"
    return (
        f"SELECT b.probe_partition, {fields} FROM ({build}) a "
        f"JOIN (SELECT id % {input_rows} AS id, "
        f"CAST(floor(id / {input_rows}) AS INT) AS probe_partition "
        f"FROM range(0, {input_rows * 4}, 1, 4)) b ON a.id = b.id"
    )


def _selected_rows(spark, query, expected_count):
    rows = spark.sql(query).collect()
    selected = [set() for _ in range(4)]
    for row in rows:
        selected[row.probe_partition].add(tuple(row[1:]))
    assert len(rows) == 4 * expected_count
    assert len(selected[0]) == expected_count
    assert all(values == selected[0] for values in selected)
    return selected[0]


@pytest.mark.parametrize(
    ("build", "plan_fragments"),
    [
        (
            "SELECT id FROM (SELECT /*+ COALESCE(1) */ id FROM range(0,200,1,4)) t ORDER BY id % 2 LIMIT 50",
            ("SortExec: TopK(fetch=50)", "CoalescePartitionsExec"),
        ),
        (
            "SELECT id FROM (SELECT /*+ REPARTITION(1) */ id FROM range(0,200,1,4)) t ORDER BY id % 2 LIMIT 50",
            ("SortExec: TopK(fetch=50)", "ExplicitRepartitionExec"),
        ),
        (
            "SELECT id FROM (SELECT /*+ COALESCE(1) */ id FROM range(0,200,1,4)) t WHERE id % 2 = 0 LIMIT 50",
            ("FilterExec:", "fetch=50", "CoalescePartitionsExec"),
        ),
        (
            "SELECT id FROM range(0,200,1,4) ORDER BY id % 2 LIMIT 50",
            ("SortPreservingMergeExec:", "fetch=50"),
        ),
    ],
    ids=["topk-coalesce", "topk-repartition", "filter-fetch", "sorted-merge"],
)
def test_shared_selection_keeps_identical_build_keys(spark, build, plan_fragments):
    query = _broadcast_query(build)
    plan = spark.sql(query)._explain_string()  # noqa: SLF001
    assert "mode=CollectLeft" in plan
    assert all(fragment in plan for fragment in plan_fragments)
    for _ in range(20):
        selected = _selected_rows(spark, query, 50)
        assert all(key % 2 == 0 for (key,) in selected)


@pytest.mark.parametrize(
    ("build", "plan_fragment"),
    [
        ("SELECT id, uuid() AS value FROM range(0,200,1,1)", "uuid()"),
        (
            "SELECT id, row_number() OVER (ORDER BY id % 2) AS value "
            "FROM (SELECT /*+ COALESCE(1) */ id FROM range(0,200,1,4)) t",
            "BoundedWindowAggExec:",
        ),
    ],
    ids=["volatile-projection", "tied-window"],
)
def test_complete_shared_build_keeps_identical_values(spark, build, plan_fragment):
    query = _broadcast_query(build, values=True)
    plan = spark.sql(query)._explain_string()  # noqa: SLF001
    assert "mode=CollectLeft" in plan
    assert plan_fragment in plan
    for _ in range(20):
        _selected_rows(spark, query, 200)


@pytest.mark.timeout(30)
@pytest.mark.parametrize(
    "remote",
    [
        {
            "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "1",
            "SAIL_CLUSTER__WORKER_MAX_COUNT": "1",
            "SAIL_CLUSTER__WORKER_TASK_SLOTS": "1",
            "SAIL_CLUSTER__TASK_STREAM_BUFFER": "1",
            "SAIL_EXECUTION__BATCH_SIZE": "256",
        }
    ],
    indirect=True,
    ids=["one-worker-one-slot"],
)
@pytest.mark.parametrize(("skip", "fetch"), [(0, 50), (7, 50), (0, 20_000)])
def test_shared_build_finishes_before_independent_single_slot_readers(spark, skip, fetch):
    # The large case exceeds both the configured batch size and the normal 8192-row
    # batch size, so the producer cannot finish by filling a single stream buffer.
    input_rows = max(200, 2 * fetch)
    build = f"SELECT id FROM range(0,{input_rows},1,1) ORDER BY id LIMIT {fetch} OFFSET {skip}"
    query = _broadcast_query(build, input_rows=input_rows)
    plan = spark.sql(query)._explain_string()  # noqa: SLF001
    assert "mode=CollectLeft" in plan
    assert "num_partitions=1" in plan
    assert _selected_rows(spark, query, fetch) == {(key,) for key in range(skip, skip + fetch)}


@pytest.mark.timeout(30)
@pytest.mark.parametrize(
    "remote",
    [
        {
            "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "1",
            "SAIL_CLUSTER__WORKER_MAX_COUNT": "1",
            "SAIL_CLUSTER__WORKER_TASK_SLOTS": "1",
            "SAIL_CLUSTER__TASK_MAX_ATTEMPTS": "3",
            "SAIL_EXECUTION__BATCH_SIZE": "256",
        }
    ],
    indirect=True,
    ids=["one-slot-retry"],
)
def test_consumer_retry_reuses_the_same_shared_build(spark, tmp_path):
    marker = str(tmp_path / "failed-once.json")

    def fail_once(partition, key, value):
        try:
            fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            return value
        with os.fdopen(fd, "w") as file:
            json.dump({"partition": partition, "key": key, "value": value}, file)
        raise RuntimeError("intentional fail-once broadcast consumer")

    # Depend on the probe partition too, so the failure cannot move into the build.
    spark.udf.register("broadcast_fail_once", udf(fail_once, "string").asNondeterministic())
    query = _broadcast_query("SELECT id, uuid() AS value FROM range(0,200,1,1)", values=True)
    query = query.replace("a.id, a.value", "a.id, broadcast_fail_once(b.probe_partition, a.id, a.value) AS value")
    plan = spark.sql(query)._explain_string()  # noqa: SLF001
    assert "mode=CollectLeft" in plan
    assert plan.index("broadcast_fail_once") < plan.index("HashJoinExec") < plan.index("uuid()")
    selected = _selected_rows(spark, query, 200)
    failed = json.loads((tmp_path / "failed-once.json").read_text())
    assert (failed["key"], failed["value"]) in selected


def test_shared_file_build_is_evaluated_once(spark, tmp_path):
    path = str(tmp_path / "build.parquet")
    spark.range(0, 200, 1, 1).write.parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("broadcast_file_build")
    query = _broadcast_query("SELECT id, uuid() AS value FROM broadcast_file_build", values=True)
    plan = spark.sql(query)._explain_string()  # noqa: SLF001
    assert "mode=CollectLeft" in plan
    assert "DataSourceExec" in plan
    _selected_rows(spark, query, 200)
