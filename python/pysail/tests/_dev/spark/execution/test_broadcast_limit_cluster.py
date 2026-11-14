import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.fixture(scope="module")
def remote():
    with spark_connect_server(envs={"SAIL_MODE": "local-cluster"}) as server:
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


def test_shared_file_build_is_evaluated_once(spark, tmp_path):
    path = str(tmp_path / "build.parquet")
    spark.range(0, 200, 1, 1).write.parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("broadcast_file_build")
    query = _broadcast_query("SELECT id, uuid() AS value FROM broadcast_file_build", values=True)
    plan = spark.sql(query)._explain_string()  # noqa: SLF001
    assert "mode=CollectLeft" in plan
    assert "DataSourceExec" in plan
    _selected_rows(spark, query, 200)
