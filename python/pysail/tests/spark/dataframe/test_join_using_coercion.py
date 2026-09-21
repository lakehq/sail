from datetime import date

import pyspark.sql.functions as F  # noqa: N812
import pyspark.sql.types as T  # noqa: N812
import pytest
from pyspark.sql import Row


@pytest.mark.parametrize(
    ("join_type", "expected_data_type", "expected_period_end"),
    [
        ("inner", T.DateType(), date(2026, 1, 31)),
        ("left", T.DateType(), date(2026, 1, 31)),
        ("right", T.StringType(), "2026-01-31"),
        ("full", T.StringType(), "2026-01-31"),
    ],
)
def test_using_join_coerces_condition_and_selects_spark_output_key(
    spark, join_type, expected_data_type, expected_period_end
):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    try:
        left = spark.createDataFrame(
            [("p1", "2026-01-31")],
            ["pid", "period_end"],
        ).withColumn("period_end", F.last_day("period_end"))
        right = spark.createDataFrame(
            [("p1", "2026-01-31")],
            ["pid", "period_end"],
        )

        result = left.join(right, on=["pid", "period_end"], how=join_type)

        assert result.schema == T.StructType(
            [
                T.StructField("pid", T.StringType(), True),
                T.StructField("period_end", expected_data_type, True),
            ]
        )
        assert result.collect() == [Row(pid="p1", period_end=expected_period_end)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)
