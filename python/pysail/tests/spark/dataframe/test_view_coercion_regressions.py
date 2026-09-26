import pytest


@pytest.mark.parametrize(("marker", "named"), [("?", False), (":value", True)], ids=["positional", "named"])
def test_parameterized_view_keeps_fractional_conditionals_after_ansi_change(spark, marker, named):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    try:
        spark.conf.set("spark.sql.ansi.enabled", "true")
        args = {"value": 0} if named else [0]
        spark.sql(
            f"SELECT id, IF(id = 0, {marker}, v) AS x "  # noqa: S608
            "FROM (SELECT 0 AS id, '2.5' AS v "
            "UNION ALL SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v) q",
            args=args,
        ).createOrReplaceTempView("parameterized_conditional_view")
        spark.conf.set("spark.sql.ansi.enabled", "false")
        result = spark.sql(
            "SELECT id, CAST(IF(id = 9, 0L, x) AS DOUBLE) AS value, "
            "typeof(IF(id = 0, 1, 2L)) AS result_type "
            "FROM parameterized_conditional_view ORDER BY id"
        )
        assert result.collect() == [(0, 0.0, "bigint"), (1, 2.5, "bigint")]
    finally:
        spark.sql("DROP VIEW IF EXISTS parameterized_conditional_view")
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)


@pytest.mark.parametrize(
    "expression",
    ["IF(id = 9, 0L, x)", "CASE WHEN id = 9 THEN 0L ELSE x END", "NVL2(NULLIF(id, 1), 0L, x)"],
)
def test_view_keeps_integral_conditional_values_after_ansi_change(spark, expression):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    try:
        spark.conf.set("spark.sql.ansi.enabled", "true")
        spark.sql(
            "CREATE OR REPLACE TEMP VIEW integral_conditional_view AS "
            "SELECT id, IF(id = 0, 0, v) AS x FROM VALUES (0, '2'), (1, '2') AS t(id, v)"
        )
        spark.conf.set("spark.sql.ansi.enabled", "false")
        result = spark.sql(
            f"SELECT id, {expression} AS value FROM integral_conditional_view ORDER BY id"  # noqa: S608
        )
        assert result.collect() == [(0, 0), (1, 2)]
    finally:
        spark.sql("DROP VIEW IF EXISTS integral_conditional_view")
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)
