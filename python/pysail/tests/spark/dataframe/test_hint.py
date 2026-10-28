import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.errors import AnalysisException, IllegalArgumentException
from pyspark.sql import Row

from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.fixture
def hint_config(spark):
    values = {
        "spark.sql.shuffle.partitions": "3",
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
    }
    previous = {key: spark.conf.get(key) for key in values}
    for key, value in values.items():
        spark.conf.set(key, value)
    try:
        yield
    finally:
        for key, value in previous.items():
            spark.conf.set(key, value)


@pytest.fixture
def hint_tables(spark, hint_config):  # noqa: ARG001
    left = spark.range(0, 12, 1, 3).selectExpr("id AS a", "id % 4 AS k")
    right = spark.range(0, 4, 1, 2).selectExpr("id AS b")
    left.createOrReplaceTempView("hint_left")
    right.createOrReplaceTempView("hint_right")
    try:
        yield left, right
    finally:
        spark.catalog.dropTempView("hint_left")
        spark.catalog.dropTempView("hint_right")


# Ported from Spark 3 (3.5.9): pyspark/sql/tests/connect/test_parity_dataframe.py,
# DataFrameParityTests.test_extended_hint_types; checks the API without Catalyst plan text.
@pytest.mark.parametrize(
    "parameters", [(1.2345,), ("what",), (["itworks1", "itworks2"],), (1.2345, "what", ["itworks"])]
)
def test_unknown_hint_accepts_parameters_and_preserves_rows(spark, parameters):
    df = spark.range(12)
    hinted = df.hint("my awesome hint", *parameters)
    assert isinstance(hinted, type(df))
    assert hinted.schema == df.schema
    assert hinted.orderBy("id").collect() == df.orderBy("id").collect()


# Ported from Spark 3 (3.5.9): pyspark/sql/tests/connect/test_parity_dataframe.py,
# DataFrameParityTests.test_extended_hint_types; checks the API without Catalyst plan text.
@pytest.mark.parametrize("parameters", [[], ["foo", "bar"]])
def test_hint_accepts_list_parameters(spark, parameters):
    df = spark.range(12)
    assert isinstance(df.hint("broadcast", parameters), type(df))


@pytest.mark.parametrize(
    "hint",
    [
        "BROADCAST",
        "broadcastjoin",
        "MapJoin",
        "MERGE",
        "mergejoin",
        "SHUFFLE_MERGE",
        "SHUFFLE_HASH",
        "SHUFFLE_REPLICATE_NL",
    ],
)
@pytest.mark.parametrize("sql", [False, True])
def test_join_hints_preserve_results_and_schema(spark, hint_tables, hint, sql):
    left, right = hint_tables
    expected = left.join(right, (left.k == right.b) & (left.a > right.b))
    if sql:
        result = spark.sql(f"SELECT /*+ {hint}(l) */ * FROM hint_left l JOIN hint_right r ON l.k = r.b AND l.a > r.b")  # noqa: S608
    else:
        result = left.hint(hint).join(right, (left.k == right.b) & (left.a > right.b))
    assert result.schema == expected.schema
    assert sorted(result.collect()) == sorted(expected.collect())
    aliases = {"BROADCASTJOIN": "BROADCAST", "MAPJOIN": "BROADCAST", "MERGEJOIN": "MERGE", "SHUFFLE_MERGE": "MERGE"}
    if not is_jvm_spark() and (canonical := aliases.get(hint.upper())):
        if sql:
            canonical_result = spark.sql(
                f"SELECT /*+ {canonical}(l) */ * FROM hint_left l JOIN hint_right r ON l.k = r.b AND l.a > r.b"  # noqa: S608
            )
        else:
            canonical_result = left.hint(canonical).join(right, (left.k == right.b) & (left.a > right.b))
        assert normalize_plan_text(result._explain_string()) == normalize_plan_text(canonical_result._explain_string())  # noqa: SLF001


@pytest.mark.parametrize("hint", ["BROADCAST", "MERGE", "SHUFFLE_HASH", "SHUFFLE_REPLICATE_NL"])
@pytest.mark.parametrize("how", ["left", "right", "full", "left_semi", "left_anti"])
def test_join_hints_with_outer_and_existence_joins(spark, hint_config, hint, how):  # noqa: ARG001
    left = spark.createDataFrame([(None,), (1,), (2,), (2,)], "k int")
    right = spark.createDataFrame([(None,), (2,), (3,)], "k int")
    expected = left.join(right, "k", how)
    result = left.join(right.hint(hint), "k", how)
    assert result.schema == expected.schema
    assert sorted(result.collect(), key=repr) == sorted(expected.collect(), key=repr)


@pytest.mark.parametrize("hint", ["BROADCAST", "MERGE", "SHUFFLE_HASH", "SHUFFLE_REPLICATE_NL"])
def test_join_hints_non_equi_and_null_safe(spark, hint_config, hint):  # noqa: ARG001
    left = spark.createDataFrame([(None,), (1,), (2,)], "a int")
    right = spark.createDataFrame([(None,), (2,), (3,)], "b int")
    for condition in [left.a < right.b, left.a.eqNullSafe(right.b)]:
        expected = left.join(right, condition)
        result = left.hint(hint).join(right, condition)
        assert result.schema == expected.schema
        assert sorted(result.collect(), key=repr) == sorted(expected.collect(), key=repr)


@pytest.mark.parametrize("parameter", [1, 1.5, True, [1, 2]])
def test_join_hint_rejects_non_relation_parameters(spark, parameter):
    with pytest.raises(AnalysisException):
        spark.range(4).hint("broadcast", parameter).collect()


@pytest.mark.skipif(is_jvm_spark(), reason="Sail physical plans")
@pytest.mark.yamlsnapshot(group="plan")
def test_join_hint_strategy_precedence_and_scope(spark, hint_tables, snapshot):  # noqa: ARG001
    hints = [
        "BROADCAST(l)",
        "BROADCAST(r)",
        "BROADCAST(l, r)",
        "MERGE(l)",
        "SHUFFLE_HASH(l)",
        "SHUFFLE_HASH(r)",
        "SHUFFLE_REPLICATE_NL(l)",
        "MERGE(l), BROADCAST(r)",
        "SHUFFLE_HASH(l), MERGE(r)",
        "SHUFFLE_REPLICATE_NL(l), SHUFFLE_HASH(r)",
        "MERGE(l), BROADCAST(l)",
        "BROADCAST(l), MERGE(l)",
        "BROADCAST(hint_left)",
        "BROADCAST(missing)",
    ]
    plans = {}
    for hint in hints:
        query = spark.sql(f"SELECT /*+ {hint} */ l.a, r.b FROM hint_left l JOIN hint_right r ON l.k = r.b")  # noqa: S608
        assert len(query.collect()) == 12  # noqa: PLR2004
        plans[hint] = normalize_plan_text(query._explain_string())  # noqa: SLF001
    for relation in ["c", "hint_left"]:
        query = spark.sql(
            f"WITH c AS (SELECT * FROM hint_left) SELECT /*+ BROADCAST({relation}) */ c.a, r.b FROM c JOIN hint_right r ON c.k = r.b"  # noqa: S608
        )
        plans[f"cte: {relation}"] = normalize_plan_text(query._explain_string())  # noqa: SLF001
    query = spark.sql("SELECT /*+ MERGE(hint_left) */ a, b FROM hint_left JOIN hint_right ON k = b")
    assert sorted(query.collect()) == [Row(a=i, b=i % 4) for i in range(12)]
    plans["unaliased temporary view"] = normalize_plan_text(query._explain_string())  # noqa: SLF001
    assert plans == snapshot


@pytest.mark.skipif(is_jvm_spark(), reason="Sail physical plans")
@pytest.mark.yamlsnapshot(group="plan")
def test_dataframe_join_hint_survives_unary_operators(spark, hint_tables, snapshot):  # noqa: ARG001
    left, right = hint_tables
    plans = {}
    for hint in ["BROADCAST", "MERGE", "SHUFFLE_HASH", "SHUFFLE_REPLICATE_NL"]:
        hinted = left.hint("BROADCAST").filter("a > 2").hint(hint).select("k", "a")
        result = hinted.join(right, hinted.k == right.b).select("a", "b")
        assert sorted(result.collect()) == [Row(a=i, b=i % 4) for i in range(3, 12)]
        plans[hint] = normalize_plan_text(result._explain_string())  # noqa: SLF001
    assert plans == snapshot


@pytest.mark.parametrize("sql", [False, True])
@pytest.mark.parametrize("with_count", [False, True])
def test_range_hint_orders_partition_boundaries(spark, hint_config, sql, with_count):  # noqa: ARG001
    source = spark.range(0, 48, 1, 4).selectExpr("id", "id % 12 AS k")
    if sql:
        source.createOrReplaceTempView("range_hint_source")
        try:
            count = "3, " if with_count else ""
            result = spark.sql(f"SELECT /*+ REPARTITION_BY_RANGE({count}k) */ * FROM range_hint_source")  # noqa: S608
            schema = result.schema
            rows = result.select("id", "k", F.spark_partition_id().alias("pid")).collect()
        finally:
            spark.catalog.dropTempView("range_hint_source")
    else:
        result = source.hint("REPARTITION_BY_RANGE", *([3, "k"] if with_count else ["k"]))
        schema = result.schema
        rows = result.select("id", "k", F.spark_partition_id().alias("pid")).collect()
    assert schema == source.schema
    assert sorted(row.id for row in rows) == list(range(48))
    groups = {pid: [row.k for row in rows if row.pid == pid] for pid in range(3)}
    assert all(groups.values())
    assert max(groups[0]) < min(groups[1]) <= max(groups[1]) < min(groups[2])


def test_range_hint_recovers_columns_and_keeps_null_peers_together(spark, hint_config):  # noqa: ARG001
    source = spark.createDataFrame(
        [(None, "a", 0), (None, "a", 1), (1, "b", 2), (1, "c", 3), (2, "a", 4)], "k int, s string, v int"
    )
    result = source.select("v").hint("REPARTITION_BY_RANGE", 3, "k", "s")
    rows = result.select("v", F.spark_partition_id().alias("pid")).collect()
    assert result.columns == ["v"]
    assert sorted(row.v for row in rows) == list(range(5))
    pids = {row.v: row.pid for row in rows}
    assert pids[0] == pids[1]
    assert [pids[i] for i in range(5)] == sorted(pids.values())


@pytest.mark.parametrize("count", [0, 1, 7])
def test_range_hint_empty_and_equal_keys(spark, hint_config, count):  # noqa: ARG001
    source = spark.range(count).selectExpr("id", "CAST(NULL AS STRING) AS k")
    result = source.hint("REPARTITION_BY_RANGE", 3, "k")
    assert result.schema == source.schema
    rows = result.select("id", F.spark_partition_id().alias("pid")).collect()
    assert sorted(row.id for row in rows) == list(range(count))
    assert len({row.pid for row in rows}) <= 1


@pytest.mark.parametrize("how", ["left", "right", "full", "left_semi", "left_anti"])
def test_merge_hint_with_residual_filter(spark, hint_config, how):  # noqa: ARG001
    left = spark.createDataFrame([(1, 4), (1, 8), (2, 0), (None, 3)], "a int, x int")
    right = spark.createDataFrame([(1, 6), (2, 1), (3, 0)], "b int, y int")
    condition = (left.a == right.b) & (left.x > right.y)
    result = left.hint("MERGE").join(right, condition, how)
    expected = left.join(right, condition, how)
    assert result.schema == expected.schema
    assert sorted(result.collect(), key=repr) == sorted(expected.collect(), key=repr)


@pytest.mark.parametrize("hint", ["REPARTITION_BY_RANGE", "REBALANCE"])
@pytest.mark.parametrize("parameters", [(0, "id"), (-1, "id"), (1.5,), (3, 5), (True,), ("missing",)])
def test_partition_hint_invalid_parameters(spark, hint_config, hint, parameters):  # noqa: ARG001
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    # Spark uses IllegalArgumentException for non-positive counts and AnalysisException for columns.
    with pytest.raises((AnalysisException, IllegalArgumentException)):
        spark.range(12).hint(hint, *parameters).collect()


@pytest.mark.parametrize("parameters", [(), (3,), ("id",), (3, "id")])
def test_rebalance_hint_preserves_rows(spark, hint_config, parameters):  # noqa: ARG001
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    source = spark.range(0, 24, 1, 4)
    result = source.hint("REBALANCE", *parameters)
    assert result.schema == source.schema
    assert sorted(result.collect()) == [Row(id=i) for i in range(24)]


def test_disabled_rebalance_ignores_even_invalid_parameters(spark, hint_config):  # noqa: ARG001
    source = spark.range(0, 12, 1, 3)
    result = source.hint("REBALANCE", "missing", -1)
    assert result.schema == source.schema
    assert sorted(result.collect()) == sorted(source.collect())


def test_disable_hints_skips_resolution(spark, hint_config):  # noqa: ARG001
    old = spark.conf.get("spark.sql.optimizer.disableHints")
    try:
        spark.conf.set("spark.sql.optimizer.disableHints", "true")
        for hint in ["BROADCAST", "REPARTITION", "COALESCE", "REPARTITION_BY_RANGE", "REBALANCE"]:
            assert sorted(spark.range(3).hint(hint, "missing", -1).collect()) == [Row(id=i) for i in range(3)]
    finally:
        spark.conf.set("spark.sql.optimizer.disableHints", old)
