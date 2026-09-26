import pytest


@pytest.mark.parametrize("ansi", ["true", "false"])
@pytest.mark.parametrize("allow_missing", [False, True])
def test_union_by_name_preserves_numeric_conditional_precision(spark, ansi, allow_missing):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    try:
        left = spark.sql("SELECT 0 AS id, CAST(1.25 AS FLOAT) AS v, 'left' AS label")
        right = spark.sql("SELECT 'right' AS label, CAST(16777217.25 AS DECIMAL(10,2)) AS v, 1 AS id")
        if allow_missing:
            right = right.selectExpr("*", "'extra' AS extra")
        union = left.unionByName(right, allowMissingColumns=allow_missing)
        result = union.selectExpr(
            "id",
            "NVL2(NULLIF(id, 1), CAST(2 AS FLOAT), v) AS value",
            "label",
        ).orderBy("id")
        assert result.dtypes == [("id", "int"), ("value", "double"), ("label", "string")]
        assert result.schema["value"].nullable is False
        assert result.collect() == [(0, 2.0, "left"), (1, 16777217.25, "right")]
        if allow_missing:
            assert union.orderBy("id").select("extra").collect() == [(None,), ("extra",)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)
