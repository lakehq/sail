import pyspark.sql.functions as F  # noqa: N812
import pytest

from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark


def normalized_plan(df, mode="simple"):
    return normalize_plan_text(df._explain_string(mode=mode))  # noqa: SLF001


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_p1_eliminates_rr_repartition_above_rr_explicit_plan(spark, snapshot):
    df = spark.range(6).repartition(3).filter(F.col("id") % 2 == 0)
    plan = normalized_plan(df, mode="codegen")

    assert plan == snapshot


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_p1_eliminates_rr_repartition_above_hash_explicit_plan(spark, snapshot):
    df = spark.range(6).repartition(3, "id").filter(F.col("id") % 2 == 0)
    plan = normalized_plan(df, mode="codegen")

    assert plan == snapshot


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_p1_eliminates_rr_repartition_above_unkown_explicit_plan(spark, snapshot):
    df = spark.range(6).filter(F.col("id") % 2 == 0).coalesce(1).filter(F.col("id") == 4)  # noqa: PLR2004
    plan = normalized_plan(df, mode="codegen")

    assert plan == snapshot


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_p1_keeps_hash_repartition_plan(spark, snapshot):
    df1 = spark.sql("SELECT id AS id1 FROM range(6)")
    df2 = spark.sql("SELECT id AS id2 FROM range(6)")
    df = df1.repartition(3).join(df2, df1.id1 == df2.id2)
    plan = normalized_plan(df)

    assert plan == snapshot


def test_p1_keeps_hash_repartition_result(spark):
    df1 = spark.sql("SELECT id AS id1 FROM range(6)")
    df2 = spark.sql("SELECT id AS id2 FROM range(6)")
    df = df1.repartition(3).join(df2, df1.id1 == df2.id2).orderBy("id1", "id2")
    result = df.collect()

    assert [(row["id1"], row["id2"]) for row in result] == [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)]


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_p1_keeps_rr_repartition_when_child_is_not_explicit_plan(spark, snapshot):
    df = spark.range(6).filter(F.col("id") % 2 == 0)
    plan = normalized_plan(df)

    assert plan == snapshot


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_p2_collapses_rr_over_rr_plan(spark, snapshot):
    df = spark.range(6).repartition(3).repartition(5)
    plan = normalized_plan(df)

    assert plan == snapshot


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_p2_collapses_rr_over_hash_plan(spark, snapshot):
    df = spark.range(6).repartition(3, "id").repartition(5)
    plan = normalized_plan(df)

    assert plan == snapshot


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_p2_collapses_rr_over_unknown_plan(spark, snapshot):
    df = spark.range(6).filter(F.col("id") % 2 == 0).coalesce(1).repartition(5)
    plan = normalized_plan(df)

    assert plan == snapshot


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_p2_collapses_hash_over_rr_plan(spark, snapshot):
    df = spark.range(6).repartition(3).repartition(5, "id")
    plan = normalized_plan(df)

    assert plan == snapshot


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_p2_collapses_hash_over_hash_plan(spark, snapshot):
    df = spark.range(6).repartition(3, "id").repartition(5, "id")
    plan = normalized_plan(df)

    assert plan == snapshot


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_p2_collapses_hash_over_unkown_plan(spark, snapshot):
    df = spark.range(6).filter(F.col("id") % 2 == 0).coalesce(1).repartition(5, "id")
    plan = normalized_plan(df)

    assert plan == snapshot


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_p2_keeps_unknown_over_rr_plan(spark, snapshot):
    df = spark.range(6).repartition(5).coalesce(3)
    plan = normalized_plan(df)

    assert plan == snapshot


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_p2_keeps_unknown_over_hash_plan(spark, snapshot):
    df = spark.range(6).repartition(5, "id").coalesce(3)
    plan = normalized_plan(df)

    assert plan == snapshot


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_p2_collapses_unknown_over_unknown_plan(spark, snapshot):
    df = spark.range(6).repartition(5).filter(F.col("id") % 2 == 0).coalesce(3).coalesce(2)
    plan = normalized_plan(df)

    assert plan == snapshot


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_p2_collapses_chain_of_three_explicits_plan(spark, snapshot):
    df = spark.range(6).repartition(5).repartition(6).repartition(7)
    plan = normalized_plan(df)

    assert plan == snapshot


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_p2_noop_when_child_is_not_explicit_plan(spark, snapshot):
    df = spark.range(6).filter(F.col("id") % 2 == 0).repartition(3)
    plan = normalized_plan(df)

    assert plan == snapshot


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_p1_and_p2_combined_plan(spark, snapshot):
    df = spark.range(6).repartition(3).repartition(5).filter(F.col("id") % 2 == 0)
    plan = normalized_plan(df)

    assert plan == snapshot
