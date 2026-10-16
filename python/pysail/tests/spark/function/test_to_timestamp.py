from datetime import datetime, timezone

import pyarrow as pa
import pytest
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.mark.skipif(is_jvm_spark(), reason="Arrow IPC data source is a Sail extension")
@pytest.mark.parametrize("seconds_type", [pa.int64(), pa.float64(), pa.decimal128(16, 7), pa.decimal256(50, 7)])
def test_make_timestamp_dictionary_seconds(spark, tmp_path, seconds_type):
    seconds = pa.array([1, None, 2, 1], type=seconds_type).dictionary_encode()
    table = pa.table({"id": range(4), "seconds": seconds})
    path = tmp_path / "seconds.arrow"
    with path.open("wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)

    rows = (
        spark.read.format("arrow")
        .load(str(path))
        .selectExpr("id", "CAST(make_timestamp_ntz(2024, 1, 1, 0, 0, seconds) AS STRING) AS result")
        .orderBy("id")
        .collect()
    )
    assert [row.result for row in rows] == [
        "2024-01-01 00:00:01",
        None,
        "2024-01-01 00:00:02",
        "2024-01-01 00:00:01",
    ]


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
