from datetime import date, datetime, timezone

import pyspark.sql.functions as F  # noqa: N812
import pyspark.sql.types as T  # noqa: N812


def test_coalesce_null_string_with_date(spark):
    schema = T.StructType(
        [
            T.StructField("string_col", T.StringType(), True),
            T.StructField("date_col", T.DateType(), True),
        ]
    )
    df = spark.createDataFrame([(None, date(2024, 1, 15))], schema)
    result = df.select(F.coalesce(F.col("string_col"), F.col("date_col"))).collect()
    assert len(result) == 1
    assert result[0][0] == date(2024, 1, 15)


def test_coalesce_null_string_with_timestamp(spark):
    schema = T.StructType(
        [
            T.StructField("string_col", T.StringType(), True),
            T.StructField("timestamp_col", T.TimestampType(), True),
        ]
    )
    df = spark.createDataFrame([(None, datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc))], schema)
    result = df.select(F.coalesce(F.col("string_col"), F.col("timestamp_col"))).collect()
    assert len(result) == 1
    assert result[0][0] == datetime(2024, 1, 15, 10, 30)  # noqa: DTZ001
