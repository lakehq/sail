from datetime import datetime, timezone

import pytest
from pyspark.sql import functions as F  # noqa: N812


@pytest.mark.parametrize(
    "conversion",
    ["to_timestamp", "try_to_timestamp", "to_timestamp_ntz", "cast_timestamp", "cast_timestamp_ntz"],
)
def test_timestamp_conversion_in_dataframe_aggregate(spark, conversion):
    df = spark.createDataFrame([("2024-01-01",), ("2024-01-02",), (None,)], ["s"])
    timestamp = (
        F.col("s").cast(conversion.removeprefix("cast_"))
        if conversion.startswith("cast_")
        else getattr(F, conversion)("s")
    )

    result = df.agg(F.max(timestamp).alias("latest"), F.min(timestamp).alias("earliest"))
    timestamp_type = "timestamp_ntz" if conversion.endswith("_ntz") else "timestamp"
    assert result.dtypes == [("latest", timestamp_type), ("earliest", timestamp_type)]
    expected = [datetime(2024, 1, 2, tzinfo=timezone.utc), datetime(2024, 1, 1, tzinfo=timezone.utc)]
    if timestamp_type == "timestamp":
        expected = [value.astimezone() for value in expected]
    assert result.collect() == [tuple(value.replace(tzinfo=None) for value in expected)]
