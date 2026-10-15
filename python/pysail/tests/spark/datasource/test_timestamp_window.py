from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.errors import PySparkException
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import TimestampNTZType, TimestampType


@pytest.mark.usefixtures("session_timezone")
@pytest.mark.parametrize("unit", ["ms", "us"])
@pytest.mark.parametrize("producer_timezone", [None, "UTC", "America/New_York"])
@pytest.mark.parametrize(
    ("session_timezone", "first_instant"),
    [("UTC", 24), ("Asia/Shanghai", 8), ("America/Los_Angeles", 38), ("+05:30", 13)],
    indirect=["session_timezone"],
)
def test_parquet_timestamp_window(spark, tmp_path, first_instant, producer_timezone, unit):
    # External Parquet preserves the distinction between wall times and instants.
    values = [datetime(2020, 6, 13, 12) + timedelta(minutes=30 * i) for i in range(120)]  # noqa: DTZ001
    values.append(None)
    path = tmp_path / "timestamps.parquet"
    pq.write_table(
        pa.table({"id": range(len(values)), "ts": pa.array(values, type=pa.timestamp(unit, tz=producer_timezone))}),
        path,
    )
    df = spark.read.parquet(str(path))
    expected_type = TimestampNTZType if producer_timezone is None else TimestampType
    assert isinstance(df.schema["ts"].dataType, expected_type)
    assert df.count() == len(values)

    rows = (
        df.filter(
            (F.col("ts") >= F.expr("TIMESTAMP '2020-06-14 00:00:00'"))
            & (F.col("ts") < F.expr("TIMESTAMP '2020-06-15 00:00:00'"))
        )
        .select("id")
        .orderBy("id")
        .collect()
    )
    first = 24 if producer_timezone is None else first_instant
    assert [row.id for row in rows] == list(range(first, first + 48))


@pytest.mark.usefixtures("session_timezone")
@pytest.mark.parametrize(
    ("session_timezone", "offset_seconds"),
    [("+00:00", 0), ("+08", 8 * 3600), ("+05:30", 5 * 3600 + 30 * 60), ("-07:00", -7 * 3600)],
    indirect=["session_timezone"],
)
@pytest.mark.parametrize("cast", ["CAST", "TRY_CAST"])
def test_fixed_offset_timestamp_cast_range(spark, tmp_path, offset_seconds, cast):
    # Parquet can carry timestamp microseconds outside Python/Chrono's date range.
    day = 86_400_000_000
    values = [-(2**63) + day, -1, 0, 2**63 - 1 - day, None]
    path = tmp_path / "wide_timestamps.parquet"
    pq.write_table(pa.table({"id": range(len(values)), "ts": pa.array(values, type=pa.timestamp("us"))}), path)
    rows = (
        spark.read.parquet(str(path))
        .selectExpr("id", f"unix_micros({cast}(ts AS TIMESTAMP_LTZ)) AS micros")
        .orderBy("id")
        .collect()
    )
    assert [row.micros for row in rows] == [
        None if value is None else value - offset_seconds * 1_000_000 for value in values
    ]


@pytest.mark.usefixtures("session_timezone")
@pytest.mark.parametrize("session_timezone", ["+05:30"], indirect=True)
def test_fixed_offset_timestamp_cast_overflow(spark, tmp_path):
    path = tmp_path / "timestamp_overflow.parquet"
    pq.write_table(pa.table({"ts": pa.array([-(2**63)], type=pa.timestamp("us"))}), path)
    df = spark.read.parquet(str(path))
    assert df.selectExpr("unix_micros(TRY_CAST(ts AS TIMESTAMP_LTZ)) AS micros").collect()[0].micros is None
    with pytest.raises(PySparkException):
        df.selectExpr("unix_micros(CAST(ts AS TIMESTAMP_LTZ))").collect()


@pytest.mark.usefixtures("session_timezone")
@pytest.mark.parametrize("session_timezone", ["Asia/Shanghai", "America/Los_Angeles"], indirect=True)
@pytest.mark.parametrize("adjusted_to_utc", [False, True])
def test_external_parquet_timestamp_logical_type_roundtrip(spark, tmp_path, adjusted_to_utc):
    session_timezone = spark.conf.get("spark.sql.session.timeZone")
    values = [datetime(2020, 6, 14, 12) + timedelta(minutes=30 * i) for i in range(24)]  # noqa: DTZ001
    if adjusted_to_utc:
        values = [value.replace(tzinfo=ZoneInfo(session_timezone)) for value in values]
    data_type = pa.timestamp("us", tz=session_timezone if adjusted_to_utc else None)
    path = tmp_path / "external.parquet"
    # Exercise the Parquet logical type without an Arrow schema metadata override.
    pq.write_table(pa.table({"ts": pa.array(values, type=data_type)}), path, store_schema=False)
    logical_type = str(pq.ParquetFile(path).schema.column(0).logical_type)
    assert f"isAdjustedToUTC={str(adjusted_to_utc).lower()}" in logical_type

    source = spark.read.parquet(str(path))
    destination = tmp_path / "rewritten"
    source.write.parquet(str(destination))
    expected_type = TimestampType if adjusted_to_utc else TimestampNTZType
    for frame in (source, spark.read.parquet(str(destination))):
        assert isinstance(frame.schema["ts"].dataType, expected_type)
        row = (
            frame.filter("ts >= TIMESTAMP '2020-06-14 00:00:00' AND ts < TIMESTAMP '2020-06-15 00:00:00'")
            .selectExpr("count(*) AS n", "CAST(min(ts) AS STRING) AS lo", "CAST(max(ts) AS STRING) AS hi")
            .collect()[0]
        )
        assert (row.n, row.lo, row.hi) == (24, "2020-06-14 12:00:00", "2020-06-14 23:30:00")
