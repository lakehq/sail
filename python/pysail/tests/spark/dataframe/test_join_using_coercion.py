from collections import Counter
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


def test_using_left_join_string_date_keys_with_ansi_enabled(spark):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        left = spark.createDataFrame([("2024-01-01",), ("2024-01-02",)], "event_day string")
        right = spark.createDataFrame([(date(2024, 1, 1), 1)], "event_day date, row_count long")

        result = left.join(right, on="event_day", how="left")

        assert result.schema == T.StructType(
            [
                T.StructField("event_day", T.StringType(), True),
                T.StructField("row_count", T.LongType(), True),
            ]
        )
        assert result.orderBy("event_day").collect() == [
            Row(event_day="2024-01-01", row_count=1),
            Row(event_day="2024-01-02", row_count=None),
        ]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)


@pytest.mark.parametrize("ansi_enabled", [False, True], ids=["non_ansi", "ansi"])
@pytest.mark.parametrize("date_on_left", [False, True], ids=["string_date", "date_string"])
@pytest.mark.parametrize(
    ("join_type", "expected_rows"),
    [
        ("inner", [(1, 1, 4)]),
        ("left", [(1, 1, 4), (2, 2, None), (None, 3, None)]),
        ("right", [(1, 1, 4), (3, None, 5), (None, None, 6)]),
        ("full", [(1, 1, 4), (2, 2, None), (3, None, 5), (None, 3, None), (None, None, 6)]),
        ("left_semi", [(1, 1)]),
        ("left_anti", [(2, 2), (None, 3)]),
    ],
)
def test_using_join_string_date_keys_preserve_spark_output(spark, ansi_enabled, date_on_left, join_type, expected_rows):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", str(ansi_enabled).lower())
    try:
        left_type, right_type = ("date", "string") if date_on_left else ("string", "date")
        left_keys = [date(2024, 1, 1), date(2024, 1, 2)] if date_on_left else ["2024-1-1", "2024-01-02"]
        right_keys = ["2024-1-1", "2024-01-03"] if date_on_left else [date(2024, 1, 1), date(2024, 1, 3)]
        left = spark.createDataFrame(
            [(left_keys[0], 1), (left_keys[1], 2), (None, 3)], f"event_day {left_type}, left_value int"
        )
        right = spark.createDataFrame(
            [(right_keys[0], 4), (right_keys[1], 5), (None, 6)], f"event_day {right_type}, right_value int"
        )

        result = left.join(right, on="event_day", how=join_type)

        # Spark preserves one input key except for FULL, which coalesces using ANSI-dependent coercion.
        if join_type == "full":
            output_is_date = ansi_enabled
        elif join_type == "right":
            output_is_date = not date_on_left
        else:
            output_is_date = date_on_left
        expected_keys = {day: date(2024, 1, day) if output_is_date else f"2024-01-0{day}" for day in (1, 2, 3)}
        expected_keys[None] = None
        if not output_is_date and not (join_type == "full" and date_on_left):
            # Comparison casts must not normalize the selected input string.
            expected_keys[1] = "2024-1-1"
        expected_fields = [
            T.StructField("event_day", T.DateType() if output_is_date else T.StringType(), True),
            T.StructField("left_value", T.IntegerType(), True),
        ]
        if join_type not in ("left_semi", "left_anti"):
            expected_fields.append(T.StructField("right_value", T.IntegerType(), True))

        assert result.schema == T.StructType(expected_fields)
        assert Counter(result.collect()) == Counter((expected_keys[day], *values) for day, *values in expected_rows)
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)
