# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.mark.parametrize("session_timezone", ["Europe/Amsterdam", "Asia/Kathmandu"], indirect=True)
def test_parquet_timestamp_functions(spark, session_timezone, tmp_path):
    _ = session_timezone
    zone = ZoneInfo(spark.conf.get("spark.sql.session.timeZone"))
    instants = [
        datetime(2019, 12, 31, 23, 30, tzinfo=timezone.utc),
        datetime(2020, 6, 14, 22, 30, tzinfo=timezone.utc),
        datetime(2020, 10, 25, 0, 30, tzinfo=timezone.utc),
        datetime(2020, 10, 25, 1, 30, tzinfo=timezone.utc),
        None,
    ]
    pattern = "yyyy-MM-dd HH:mm XXX"
    path = tmp_path / "timestamps.parquet"
    pq.write_table(
        pa.table(
            {
                "id": range(len(instants)),
                "ts": pa.array(instants, type=pa.timestamp("us", tz="UTC")),
                "fmt": [pattern] * len(instants),
            }
        ),
        path,
    )
    timestamps = spark.read.parquet(str(path))
    rows = (
        timestamps.orderBy("id")
        .selectExpr(
            "year(ts)",
            "quarter(ts)",
            "month(ts)",
            "dayofmonth(ts)",
            "dayofyear(ts)",
            "dayofweek(ts)",
            "weekday(ts)",
            "weekofyear(ts)",
            "hour(ts)",
            "minute(ts)",
            "second(ts)",
            "extract(hour FROM ts)",
            "datepart('minute', ts)",
            "date_format(ts, fmt)",
            f"date_format(ts, '{pattern}')",
            "to_char(ts, fmt)",
            "to_varchar(ts, fmt)",
            "to_date(ts, 'yyyy')",
            "to_timestamp_ntz(ts)",
            "convert_timezone('UTC', 'UTC', ts)",
            "unix_micros(to_timestamp_ltz(ts))",
            "unix_micros(try_to_timestamp(ts))",
            "to_json(named_struct('ts', ts), map('timestampFormat', 'HH:mm'))",
            "to_json(named_struct('ts', ts), map('timestampFormat', 'HH:mm', 'timeZone', 'UTC'))",
        )
        .collect()
    )
    for instant, row in zip(instants, rows, strict=True):
        if instant is None:
            assert all(value is None for value in row[:-2])
            assert row[-2:] == ("{}", "{}")
            continue
        local = instant.astimezone(zone)
        wall = local.replace(tzinfo=None)
        formatted = local.isoformat(sep=" ", timespec="minutes")
        formatted = f"{formatted[:-6]} {formatted[-6:]}"
        micros = int(instant.timestamp()) * 1_000_000
        assert tuple(row) == (
            local.year,
            (local.month - 1) // 3 + 1,
            local.month,
            local.day,
            local.timetuple().tm_yday,
            local.isoweekday() % 7 + 1,
            local.weekday(),
            local.isocalendar().week,
            local.hour,
            local.minute,
            local.second,
            local.hour,
            local.minute,
            formatted,
            formatted,
            formatted,
            formatted,
            local.date(),
            wall,
            wall,
            micros,
            micros,
            f'{{"ts":"{local:%H:%M}"}}',
            f'{{"ts":"{instant:%H:%M}"}}',
        )

    matching_ids = timestamps.where("hour(ts) = 0 AND dayofmonth(ts) = 1").select("id").collect()
    assert [row.id for row in matching_ids] == [
        i
        for i, instant in enumerate(instants)
        if instant is not None and instant.astimezone(zone).hour == 0 and instant.astimezone(zone).day == 1
    ]


@pytest.mark.parametrize("session_timezone", ["+05:30:45"], indirect=True)
def test_fixed_offset_date_cast_parquet_nullability(spark, session_timezone, tmp_path):
    _ = session_timezone
    dates = spark.range(2).selectExpr(
        "cast(timestamp_micros(id) as date) as required_date",
        "cast(timestamp_micros(nullif(id, 0L)) as date) as optional_date",
        "try_cast(timestamp_micros(id) as date) as try_date",
    )
    path = tmp_path / "dates"
    dates.write.parquet(str(path))
    expected = {"required_date": False, "optional_date": True, "try_date": True}
    assert {field.name: field.nullable for field in dates.schema} == expected
    assert {field.name: field.nullable for field in pq.read_schema(next(path.glob("*.parquet")))} == expected
