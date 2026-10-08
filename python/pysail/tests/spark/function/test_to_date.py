from datetime import date

import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as T  # noqa: N812


@pytest.mark.parametrize("input_type", ["bigint", "int"])
def test_to_date_numeric_column_schema_and_collect(spark, input_type):
    df = spark.createDataFrame([(20260220,), (20251201,)], ["date_int"])
    if input_type == "int":
        df = df.withColumn("date_int", F.col("date_int").cast("int"))
    assert df.dtypes == [("date_int", input_type)]

    result = df.withColumn("date_col", F.to_date(F.col("date_int"), "yyyyMMdd"))
    assert result.dtypes == [("date_int", input_type), ("date_col", "date")]
    assert result.schema["date_col"] == T.StructField("date_col", T.DateType(), nullable=True)
    assert sorted(result.collect()) == [(20251201, date(2025, 12, 1)), (20260220, date(2026, 2, 20))]
