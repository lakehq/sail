import pytest
from pyspark.sql import functions as F  # noqa: N812


@pytest.mark.parametrize(("operand", "resolved"), [("coalesce", True), ("md5", False), ("concat", True)])
def test_dataframe_string_minus_date_resolves_before_sql_string_promotion(spark, operand, resolved):
    # Spark Connect resolves these DataFrame expressions before datetime rewriting.
    # The equivalent SQL operands require coercion and are rejected instead.
    old_ansi = spark.conf.get("spark.sql.ansi.enabled")
    try:
        spark.conf.set("spark.sql.ansi.enabled", "false")
        expressions = {
            "coalesce": F.coalesce(F.lit(None), F.lit("2024-01-16")),
            "md5": F.md5(F.lit("x")),
            "concat": F.concat(F.lit("2024-01-"), F.lit(16)),
        }
        result = expressions[operand] - F.lit("2024-01-15").cast("date")
        actual = spark.range(1).select(result.isNotNull().alias("resolved")).collect()
        assert [row.resolved for row in actual] == [resolved]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", old_ansi)
