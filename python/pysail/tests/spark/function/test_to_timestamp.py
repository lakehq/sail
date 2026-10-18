from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.mark.skipif(is_jvm_spark(), reason="Arrow IPC data source is a Sail extension")
@pytest.mark.parametrize("encoding", ["dictionary", "run_end"])
@pytest.mark.parametrize(
    "seconds_type", [pa.int64(), pa.float64(), pa.decimal128(16, 7), pa.decimal256(50, 7), pa.string()]
)
def test_make_timestamp_encoded_seconds(spark, tmp_path, seconds_type, encoding):
    seconds = pa.array(["1", None, "2", "1"]).cast(seconds_type)
    seconds = seconds.dictionary_encode() if encoding == "dictionary" else pc.run_end_encode(seconds)
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


@pytest.mark.parametrize(
    "function", ["to_timestamp", "to_timestamp_ltz", "to_timestamp_ntz", "try_to_timestamp", "try_to_date"]
)
@pytest.mark.parametrize("ansi", ["true", "false"])
@pytest.mark.parametrize("timestamp_type", ["TIMESTAMP_LTZ", "TIMESTAMP_NTZ"])
@pytest.mark.parametrize("rows", [0, 3])
def test_temporal_format_aggregate_schema(spark, function, ansi, timestamp_type, rows):
    settings = {
        "spark.sql.ansi.enabled": ansi,
        "spark.sql.timestampType": timestamp_type,
        "spark.sql.session.timeZone": "America/Los_Angeles",
    }
    original = {key: spark.conf.get(key) for key in settings}
    try:
        for key, value in settings.items():
            spark.conf.set(key, value)
        value = F.lit(date(2024, 1, 1))
        format_expr = F.max(F.lit("yyyy"))
        result = spark.range(rows).select(
            F.call_function(function, value, format_expr).alias("nullable_format"),
            F.call_function(function, value, F.coalesce(format_expr, F.lit("yyyy"))).alias("nonnull_format"),
        )
        expected_type = {
            "try_to_date": "date",
            "to_timestamp_ntz": "timestamp_ntz",
            "to_timestamp_ltz": "timestamp",
        }.get(function, "timestamp_ntz" if timestamp_type == "TIMESTAMP_NTZ" else "timestamp")
        assert result.dtypes == [("nullable_format", expected_type), ("nonnull_format", expected_type)]
        assert [field.nullable for field in result.schema] == [True, ansi == "false" or function.startswith("try_")]
        expected_value = (
            "2024-01-01"
            if expected_type == "date"
            else "2024-01-01 08:00:00"
            if expected_type == "timestamp_ntz"
            else "2024-01-01 00:00:00"
        )
        assert result.selectExpr("CAST(nullable_format AS STRING)", "CAST(nonnull_format AS STRING)").collect() == [
            (expected_value, expected_value)
        ]
    finally:
        for key, value in original.items():
            spark.conf.set(key, value)
