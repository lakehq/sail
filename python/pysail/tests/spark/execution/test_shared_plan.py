from __future__ import annotations

import re

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import Row

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = [
    pytest.mark.skipif(is_jvm_spark(), reason="Sail physical subplan reuse"),
    pytest.mark.timeout(90),
]


@pytest.fixture(scope="module", params=["local", "flight", "storage"])
def remote(request, tmp_path_factory):
    path = tmp_path_factory.mktemp(f"shared_{request.param}")
    envs = {
        "SAIL_MODE": "local" if request.param == "local" else "local-cluster",
        "SAIL_CLUSTER__TASK_STREAM_BUFFER": "1",
        "SAIL_RUNTIME__MEMORY_POOL__TYPE": "greedy",
        "SAIL_RUNTIME__MEMORY_POOL__GREEDY__MAX_SIZE": str(256 * 1024 * 1024),
    }
    if request.param == "storage":
        envs.update(
            {
                "SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE": "storage",
                "SAIL_CLUSTER__SHUFFLE_BACKEND__STORAGE__PATH": path.as_uri(),
                "SAIL_CLUSTER__SHUFFLE_BACKEND__STORAGE__MAX_FILE_SIZE": "65536",
            }
        )
    with spark_connect_server(envs=envs) as server:
        yield server.remote


@pytest.mark.yamlsnapshot(group="plan")
def test_common_aggregate_has_independent_readers(spark, snapshot):
    df = spark.sql("""
        WITH totals AS (
            SELECT id % 8 AS k, SUM(id) AS v
            FROM range(0, 40000, 1, 4) GROUP BY id % 8
        )
        SELECT SUM(v) AS total FROM totals
        UNION ALL
        SELECT SUM(v) + 1 AS total FROM totals
    """)
    assert normalize_plan_text(df._explain_string()) == snapshot  # noqa: SLF001
    expected = [Row(total=799980000), Row(total=799980001)]
    assert sorted(df.collect()) == expected
    # A new execution must not keep the previous execution's streams or state.
    assert sorted(df.collect()) == expected


@pytest.mark.yamlsnapshot(group="plan")
def test_shared_producer_preserves_aliases_and_duplicates(spark, snapshot):
    df = spark.sql("""
        WITH t AS (SELECT id % 5 AS k, id * 3 AS v FROM range(0, 100, 1, 4))
        SELECT a.k AS left_key, b.k AS right_key, a.v AS left_value, b.v AS right_value
        FROM t a JOIN t b ON a.k = b.k
    """)
    assert normalize_plan_text(df._explain_string()) == snapshot  # noqa: SLF001
    expected = [
        Row(left_key=a % 5, right_key=b % 5, left_value=a * 3, right_value=b * 3)
        for a in range(100)
        for b in range(100)
        if a % 5 == b % 5
    ]
    assert sorted(df.collect()) == sorted(expected)


@pytest.mark.yamlsnapshot(group="plan")
def test_early_reader_does_not_truncate_shared_output(spark, snapshot):
    df = spark.sql("""
        WITH t AS (SELECT id, id * 7 AS v FROM range(0, 20000, 1, 4))
        SELECT 0 AS branch, SUM(v) AS n FROM (SELECT * FROM t LIMIT 1)
        UNION ALL
        SELECT 1 AS branch, SUM(v) AS n FROM t
    """)
    assert normalize_plan_text(df._explain_string()) == snapshot  # noqa: SLF001
    first, second = sorted(df.collect())
    assert first.branch == 0
    assert first.n in range(0, 140000, 7)
    assert second == Row(branch=1, n=1399930000)


@pytest.mark.yamlsnapshot(group="plan")
def test_shared_output_spills_and_replays_to_late_reader(spark, snapshot):
    # Each shared hash partition exceeds the 8 MiB replay memory budget. Each
    # consumer also uses the payload, preventing projection pushdown from
    # removing the wide column before materialization.
    query = """
        WITH t AS (
            SELECT id, repeat(CAST(id AS STRING), 2048) AS payload
            FROM range(1000, 7000, 1, 1)
        )
        SELECT SUM(length(a.payload) + length(b.payload)) AS n
        FROM t a JOIN t b ON a.id = b.id
    """
    df = spark.sql(query)
    assert normalize_plan_text(df._explain_string()) == snapshot  # noqa: SLF001
    assert df.collect() == [Row(n=98304000)]
    plan = "\n".join(str(row[0]) for row in spark.sql("EXPLAIN ANALYZE " + query).collect())
    assert normalize_plan_text(plan) == snapshot
    shared_metrics = "\n".join(line for line in plan.splitlines() if "SharedReadExec" in line)
    spills = re.findall(r"spill_count=(\d+)", shared_metrics)
    assert any(int(count) > 0 for count in spills), plan


@pytest.mark.yamlsnapshot(group="plan")
def test_shared_producer_is_executed_once(spark, snapshot):
    rows = spark.sql("""
        EXPLAIN ANALYZE
        WITH t AS (SELECT SUM(id) AS n FROM range(0, 40000, 1, 1))
        SELECT n FROM t UNION ALL SELECT n + 1 FROM t
    """).collect()
    plan = "\n".join(str(row[0]) for row in rows)
    assert normalize_plan_text(plan) == snapshot
    executions = re.findall(r"producer_executions=(\d+)", plan)
    assert executions, plan
    assert all(int(count) == 1 for count in executions), plan


@pytest.mark.yamlsnapshot(group="plan")
def test_common_join_with_unused_dynamic_filter_is_shared(spark, snapshot):
    df = spark.sql("""
        WITH t AS (
            SELECT SUM(a.id + b.id) AS n
            FROM range(100) a JOIN range(100) b
            ON a.id % 5 = b.id % 5 AND a.id < b.id
        )
        SELECT n FROM t UNION ALL SELECT n + 1 FROM t
    """)
    plan = df._explain_string()  # noqa: SLF001
    assert normalize_plan_text(plan) == snapshot
    total = sum(a + b for a in range(100) for b in range(100) if a % 5 == b % 5 and a < b)
    assert sorted(df.collect()) == [Row(n=total), Row(n=total + 1)]


@pytest.mark.yamlsnapshot(group="plan")
def test_different_sources_and_filters_are_not_merged(spark, snapshot):
    spark.createDataFrame([(1,), (2,)], ["v"]).createOrReplaceTempView("shared_left")
    spark.createDataFrame([(7,), (8,)], ["v"]).createOrReplaceTempView("shared_right")
    try:
        df = spark.sql("""
            SELECT SUM(v) AS n FROM shared_left WHERE v < 2
            UNION ALL SELECT SUM(v) AS n FROM shared_right WHERE v > 7
        """)
        assert normalize_plan_text(df._explain_string()) == snapshot  # noqa: SLF001
        assert sorted(df.collect()) == [Row(n=1), Row(n=8)]
    finally:
        spark.catalog.dropTempView("shared_left")
        spark.catalog.dropTempView("shared_right")


@pytest.mark.yamlsnapshot(group="plan")
def test_shared_output_with_nulls_and_empty_partitions(spark, snapshot):
    df = spark.sql("""
        WITH t AS (
            SELECT CASE WHEN id = 0 THEN NULL ELSE id END AS v
            FROM range(0, 2, 1, 8)
        )
        SELECT v AS a FROM t UNION ALL SELECT v AS a FROM t
    """)
    assert normalize_plan_text(df._explain_string()) == snapshot  # noqa: SLF001
    rows = df.collect()
    assert sum(row.a is None for row in rows) == 2  # noqa: PLR2004
    assert sum(row.a == 1 for row in rows) == 2  # noqa: PLR2004


@pytest.mark.yamlsnapshot(group="plan")
def test_shared_producer_error_does_not_hang_or_poison_next_query(spark, snapshot):
    df = spark.sql("""
        WITH t AS (SELECT SUM(100 DIV (id % 2)) AS n FROM range(10000))
        SELECT n FROM t UNION ALL SELECT n + 1 FROM t
    """)
    assert normalize_plan_text(df._explain_string()) == snapshot  # noqa: SLF001
    with pytest.raises(PySparkException, match=r"(?i)(divide|division).*zero"):
        df.collect()
    assert spark.range(10).count() == 10  # noqa: PLR2004


@pytest.mark.yamlsnapshot(group="plan")
def test_pivot_value_inference_with_shared_input(spark, snapshot):
    source = spark.range(10).selectExpr("id % 2 AS k", "id AS v")
    df = source.unionAll(source).groupBy().pivot("k").sum("v")
    assert normalize_plan_text(df._explain_string()) == snapshot  # noqa: SLF001
    assert df.collect() == [Row(**{"0": 40, "1": 50})]


@pytest.mark.yamlsnapshot(group="plan")
def test_common_parquet_scan(spark, tmp_path, snapshot):
    path = str(tmp_path / "input")
    spark.range(10000, numPartitions=2).write.parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("shared_parquet")
    try:
        df = spark.sql("""
            SELECT SUM(id) AS n FROM shared_parquet
            UNION ALL SELECT SUM(id) + 1 AS n FROM shared_parquet
        """)
        assert normalize_plan_text(df._explain_string()) == snapshot  # noqa: SLF001
        assert sorted(df.collect()) == [Row(n=49995000), Row(n=49995001)]
        filtered = spark.sql("""
            SELECT SUM(id) AS n FROM shared_parquet WHERE id < 10
            UNION ALL SELECT SUM(id) AS n FROM shared_parquet WHERE id > 9990
        """)
        assert normalize_plan_text(filtered._explain_string()) == snapshot  # noqa: SLF001
        assert sorted(filtered.collect()) == [Row(n=45), Row(n=89955)]
    finally:
        spark.catalog.dropTempView("shared_parquet")


@pytest.mark.yamlsnapshot(group="plan")
def test_volatile_projection_stays_outside_shared_producer(spark, snapshot):
    # Fix the seed for stable snapshots; the function is still volatile.
    df = spark.sql("""
        WITH t AS (SELECT id, rand(42) AS r FROM range(0, 1000, 1, 4))
        SELECT a.id, a.r AS x, b.r AS y FROM t a JOIN t b ON a.id = b.id
    """)
    rows = df.collect()
    assert sorted(row.id for row in rows) == list(range(1000))
    plan = df._explain_string()  # noqa: SLF001
    assert normalize_plan_text(plan) == snapshot
