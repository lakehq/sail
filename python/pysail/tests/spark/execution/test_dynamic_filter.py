from __future__ import annotations

import pyspark.sql.functions as F  # noqa: N812
import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = [pytest.mark.skipif(is_jvm_spark(), reason="Sail distributed dynamic filters"), pytest.mark.timeout(60)]


@pytest.fixture(scope="module", params=["flight", "storage"])
def remote(request, tmp_path_factory):
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": "4",
        "SAIL_OPTIMIZER__ENABLE_JOIN_SWAP": "false",
        "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "2",
        "SAIL_CLUSTER__WORKER_MAX_COUNT": "2",
        "SAIL_CLUSTER__WORKER_TASK_SLOTS": "32",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE": request.param,
    }
    if request.param == "storage":
        envs["SAIL_CLUSTER__SHUFFLE_BACKEND__STORAGE__PATH"] = tmp_path_factory.mktemp("filter_shuffle").as_uri()
    with spark_connect_server(envs=envs) as server:
        yield server.remote


@pytest.fixture(scope="module")
def probe(spark, tmp_path_factory):
    path = str(tmp_path_factory.mktemp("filter_probe") / "data")
    spark.range(128, numPartitions=4).selectExpr("id AS k", "id % 3 AS g", "concat('row-', id) AS label").write.parquet(
        path
    )
    return spark.read.parquet(path)


@pytest.mark.parametrize("build_partitions", [1, 4])
@pytest.mark.yamlsnapshot(group="plan")
def test_join_filters_cross_exchanges(spark, probe, build_partitions, snapshot):
    build = (
        spark.range(4, numPartitions=build_partitions)
        .selectExpr("CASE id WHEN 0 THEN 1L WHEN 3 THEN 93L ELSE 17L END AS k")
        .repartition(4, "k")
    )
    # Project the probe key to a different column index before exchanging data.
    query = build.join(probe.select("label", "g", "k").repartition(4, "k"), "k").select("k", "label").orderBy("k")
    assert [(row.k, row.label) for row in query.collect()] == [
        (1, "row-1"),
        (17, "row-17"),
        (17, "row-17"),
        (93, "row-93"),
    ]
    plan = query._explain_string(mode="codegen").split("== Distributed Plan ==\n", 1)[1]  # noqa: SLF001
    assert normalize_plan_text(plan) == snapshot


@pytest.mark.parametrize("join_type", ["inner", "left", "full", "left_semi", "left_anti"])
def test_join_filters_preserve_nulls_and_unmatched_rows(spark, join_type):
    left = spark.createDataFrame([(None, "null"), (1, "a"), (2, "b"), (2, "c")], "k LONG, label STRING").repartition(
        4, "k"
    )
    right = spark.createDataFrame([(None,), (2,), (3,)], "r LONG").repartition(4, "r")
    query = left.join(right, left.k.eqNullSafe(right.r), join_type)
    actual = sorted((tuple(row) for row in query.collect()), key=repr)
    expected = {
        "inner": [(None, "null", None), (2, "b", 2), (2, "c", 2)],
        "left": [(None, "null", None), (1, "a", None), (2, "b", 2), (2, "c", 2)],
        "full": [(None, "null", None), (1, "a", None), (2, "b", 2), (2, "c", 2), (None, None, 3)],
        "left_semi": [(None, "null"), (2, "b"), (2, "c")],
        "left_anti": [(1, "a")],
    }
    assert actual == sorted(expected[join_type], key=repr)


def test_empty_and_composite_build_filters(spark, probe):
    empty = spark.createDataFrame([], "k LONG, g LONG").repartition(4, "k")
    assert probe.join(empty, ["k", "g"]).collect() == []
    build = spark.createDataFrame([(17, 2), (93, 0), (1, 2)], "k LONG, g LONG").repartition(4, "k", "g")
    actual = probe.repartition(4, "k", "g").join(build, ["k", "g"]).select("k").collect()
    assert sorted(row.k for row in actual) == [17, 93]


@pytest.mark.parametrize("descending", [False, True])
def test_topk_and_min_max_filters(probe, descending):
    order = F.col("k").desc() if descending else F.col("k").asc()
    query = probe.repartition(4).orderBy(order).limit(7)
    expected = list(range(127, 120, -1)) if descending else list(range(7))
    assert [row.k for row in query.collect()] == expected
    assert tuple(probe.repartition(4).agg(F.min("k"), F.max("k")).first()) == (0, 127)


def test_join_with_many_distinct_keys(spark, probe):
    build = spark.range(8192, numPartitions=1).selectExpr("id + 64 AS k")
    actual = probe.join(build, "k").select("k").collect()
    assert sorted(row.k for row in actual) == list(range(64, 128))


@pytest.mark.parametrize("null_safe", [False, True])
def test_floating_point_keys_preserve_nan_zero_and_null(spark, tmp_path, null_safe):
    probe = spark.createDataFrame(
        [
            (float("nan"), "nan"),
            (0.0, "zero"),
            (-0.0, "negative-zero"),
            (float("inf"), "infinity"),
            (float("-inf"), "negative-infinity"),
            (1.5, "other"),
            (None, "null"),
        ],
        "k DOUBLE, label STRING",
    ).repartition(4, "k")
    path = str(tmp_path / "floating_probe")
    probe.write.parquet(path)
    probe = spark.read.parquet(path).repartition(4, "k")
    build = spark.createDataFrame([(float("nan"),), (0.0,), (float("inf"),), (None,)], "b DOUBLE").repartition(4, "b")
    condition = probe.k.eqNullSafe(build.b) if null_safe else probe.k == build.b
    actual = sorted(row.label for row in build.join(probe, condition).select("label").collect())
    expected = ["nan", "zero", "negative-zero", "infinity"] + (["null"] if null_safe else [])
    assert actual == sorted(expected)


@pytest.mark.yamlsnapshot(group="plan")
def test_nested_filters(spark, probe, snapshot):
    first = spark.range(4, numPartitions=4).selectExpr("id * 17 AS k").repartition(4, "k")
    second = spark.range(2, numPartitions=2).selectExpr("id * 34 AS k").repartition(4, "k")
    query = second.join(first.join(probe.filter("k < 100"), "k"), "k").select("k")
    actual = query.collect()
    assert sorted(row.k for row in actual) == [0, 34]
    plan = query._explain_string(mode="codegen").split("== Distributed Plan ==\n", 1)[1]  # noqa: SLF001
    assert normalize_plan_text(plan) == snapshot


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "SELECT k FROM VALUES (1), (2), (NULL) AS probe(k) WHERE k NOT IN (SELECT k FROM VALUES (2), (3) AS build(k))",
            [1],
        ),
        (
            "SELECT k FROM VALUES (1), (2), (NULL) AS probe(k) WHERE k NOT IN (SELECT k FROM VALUES (2), (NULL) AS build(k))",
            [],
        ),
    ],
)
def test_null_aware_anti_join(spark, query, expected):
    assert [row.k for row in spark.sql(query).collect()] == expected
