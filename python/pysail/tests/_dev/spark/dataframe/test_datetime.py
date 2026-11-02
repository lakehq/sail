from datetime import date, datetime, timezone

import pyspark.sql.functions as F  # noqa: N812
import pyspark.sql.types as T  # noqa: N812
import pytest
from pyspark.sql.functions import lit
from pyspark.sql.types import Row

from pysail.testing.spark.utils.sql import strict


@pytest.mark.parametrize(
    ("session_timezone", "local_timezone", "data"),
    [
        (
            "America/Los_Angeles",
            "Asia/Shanghai",
            [
                (datetime(1970, 1, 1), "1969-12-31 08:00:00", datetime(1970, 1, 1)),  # noqa: DTZ001
                (datetime(1970, 1, 1, tzinfo=timezone.utc), "1969-12-31 16:00:00", datetime(1970, 1, 1, 8)),  # noqa: DTZ001
                (datetime(2024, 11, 3, 16, 30), "2024-11-03 01:30:00", datetime(2024, 11, 3, 16, 30)),  # noqa: DTZ001
                (datetime(2024, 11, 3, 17, 30), "2024-11-03 01:30:00", datetime(2024, 11, 3, 17, 30)),  # noqa: DTZ001
                (datetime(2025, 3, 9, 17, 30), "2025-03-09 01:30:00", datetime(2025, 3, 9, 17, 30)),  # noqa: DTZ001
                (datetime(2025, 3, 9, 18, 30), "2025-03-09 03:30:00", datetime(2025, 3, 9, 18, 30)),  # noqa: DTZ001
            ],
        ),
        (
            "America/Los_Angeles",
            "America/Los_Angeles",
            [
                (datetime(1970, 1, 1), "1970-01-01 00:00:00", datetime(1970, 1, 1)),  # noqa: DTZ001
                (datetime(1970, 1, 1, tzinfo=timezone.utc), "1969-12-31 16:00:00", datetime(1969, 12, 31, 16)),  # noqa: DTZ001
                # The PySpark client does not seem to respect the `fold` parameter for local `datetime` objects.
                # This is a bug on the client side, and no special handling is needed for the server.
                (datetime(2024, 11, 3, 1, 30), "2024-11-03 01:30:00", datetime(2024, 11, 3, 1, 30, fold=1)),  # noqa: DTZ001
                (datetime(2024, 11, 3, 1, 30, fold=0), "2024-11-03 01:30:00", datetime(2024, 11, 3, 1, 30, fold=1)),  # noqa: DTZ001
                (datetime(2024, 11, 3, 1, 30, fold=1), "2024-11-03 01:30:00", datetime(2024, 11, 3, 1, 30, fold=1)),  # noqa: DTZ001
                (datetime(2025, 3, 9, 2, 30), "2025-03-09 03:30:00", datetime(2025, 3, 9, 3, 30)),  # noqa: DTZ001
                (datetime(2025, 3, 9, 3, 30), "2025-03-09 03:30:00", datetime(2025, 3, 9, 3, 30)),  # noqa: DTZ001
            ],
        ),
    ],
    indirect=["session_timezone", "local_timezone"],
)
def test_datetime_literal(spark, session_timezone, local_timezone, data):  # noqa: ARG001
    for dt, k, v in data:
        assert spark.range(1).select(lit(dt)).collect() == [strict(Row(**{f"TIMESTAMP '{k}'": v}))]


def test_coalesce_mixed_string_temporal(spark):
    original = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    try:
        date_schema = T.StructType(
            [
                T.StructField("string_col", T.StringType(), True),
                T.StructField("date_col", T.DateType(), True),
            ]
        )
        date_df = spark.createDataFrame([(None, date(2024, 1, 15))], date_schema)
        date_result = date_df.select(F.coalesce(F.col("string_col"), F.col("date_col")).alias("c"))
        assert date_result.schema.simpleString() == "struct<c:string>"
        assert date_result.collect() == [Row(c="2024-01-15")]

        timestamp_schema = T.StructType(
            [
                T.StructField("string_col", T.StringType(), True),
                T.StructField("timestamp_col", T.TimestampType(), True),
            ]
        )
        timestamp_df = spark.createDataFrame(
            [(None, datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc))], timestamp_schema
        )
        timestamp_result = timestamp_df.select(F.coalesce(F.col("string_col"), F.col("timestamp_col")).alias("c"))
        assert timestamp_result.schema.simpleString() == "struct<c:string>"
        assert timestamp_result.collect() == [Row(c="2024-01-15 10:30:00")]

        literal_df = spark.createDataFrame(
            [(date(2024, 1, 15),)], T.StructType([T.StructField("date_col", T.DateType(), True)])
        )
        literal_result = literal_df.select(F.coalesce(F.lit("default"), F.col("date_col")).alias("c"))
        assert literal_result.schema.simpleString() == "struct<c:string>"
        assert literal_result.collect() == [Row(c="default")]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original)
