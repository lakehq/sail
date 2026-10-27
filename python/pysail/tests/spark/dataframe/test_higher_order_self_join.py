import pyspark.sql.functions as F  # noqa: N812
import pytest

from pysail.testing.spark.steps.plan import normalize_plan_text


@pytest.mark.parametrize(
    ("higher_order_expression", "expected"),
    [
        (
            "FILTER(array(coalesce(col_1, NULL)), x -> x IS NOT NULL)",
            [("h1", ["T_A"], "T_A"), ("h2", ["T_B"], "T_B")],
        ),
        (
            "TRANSFORM(array(coalesce(col_1, NULL)), x -> concat(x, '_t'))",
            [("h1", ["T_A_t"], "T_A_t"), ("h2", ["T_B_t"], "T_B_t")],
        ),
    ],
)
def test_higher_order_lambda_survives_self_join(spark, higher_order_expression, expected):
    base = spark.createDataFrame(
        [("h1", "T_A"), ("h2", "T_B")],
        ["entity_id", "col_1"],
    )
    auxiliary = spark.createDataFrame(
        [("h1", "x"), ("h2", "y")],
        ["entity_id", "seg"],
    )
    base = base.join(auxiliary, "entity_id", "left")

    for index in range(4):
        base = base.withColumn(
            f"d{index}",
            F.expr(f"concat(entity_id, '_{index}')"),
        )

    base = base.withColumn("arr", F.expr(higher_order_expression))
    child = base.withColumn("t", F.explode("arr")).select("entity_id", "t").withColumnRenamed("entity_id", "eid2")
    parent = base.alias("parent")
    child = child.alias("child")
    result = parent.join(
        child,
        F.col("parent.entity_id") == F.col("child.eid2"),
        "left",
    ).select("parent.*", F.col("child.t").alias("t"))

    actual = [(row["entity_id"], row["arr"], row["t"]) for row in result.orderBy("entity_id").collect()]
    assert actual == expected


@pytest.mark.yamlsnapshot(group="plan")
def test_higher_order_lambda_survives_nested_loop_join_filter(spark, snapshot):
    left = spark.createDataFrame(
        [
            (1, [1], 10, 20),
            (2, [None], 30, 40),
        ],
        ["id", "arr", "padding_1", "padding_2"],
    ).alias("l")
    right = spark.createDataFrame([(0,), (1,), (2,)], ["k"]).alias("r")

    result = left.join(
        right,
        F.expr("exists(l.arr, x -> x IS NOT NULL) = (l.id > r.k)"),
        "inner",
    ).select("l.id", "r.k")

    plan = normalize_plan_text(result._explain_string())  # noqa: SLF001
    assert plan == snapshot
    assert [tuple(row) for row in result.orderBy("id", "k").collect()] == [(1, 0), (2, 2)]
