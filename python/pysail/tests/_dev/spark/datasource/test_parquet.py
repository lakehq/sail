# SPDX-License-Identifier: Apache-2.0

import base64
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import TimestampNTZType, TimestampType

from pysail.testing._dev.spark.utils.files import get_data_directory_size
from pysail.testing.spark.utils.sql import escape_sql_identifier, escape_sql_string_literal


def safe_sort_key(row):
    if isinstance(row, Mapping):
        return tuple((v is not None, v) for _, v in sorted(row.items()))
    return tuple((v is not None, v) for v in row)


@pytest.mark.parametrize("session_timezone", ["America/Los_Angeles"], indirect=True)
def test_parquet_merges_mixed_timestamp_units_with_different_arrow_timezones(spark, tmp_path, session_timezone):
    _ = session_timezone
    path = tmp_path / "mixed_timestamp_units_and_timezones"
    path.mkdir()
    files = [
        ("a_us_new_york.parquet", "us", "America/New_York", datetime(2024, 1, 2, 3, 4, 6, 123456, tzinfo=UTC)),
        ("b_ms_utc.parquet", "ms", "UTC", datetime(2024, 1, 2, 3, 4, 5, 123000, tzinfo=UTC)),
    ]
    for name, unit, timezone, value in files:
        pq.write_table(
            pa.table({"ts": pa.array([value], type=pa.timestamp(unit, tz=timezone))}),
            path / name,
        )

    df = spark.read.parquet(str(path))
    rows = df.selectExpr("CAST(ts AS STRING) AS value").orderBy("value").collect()

    assert df.schema.simpleString() == "struct<ts:timestamp>"
    assert rows == [
        Row(value="2024-01-01 19:04:05.123"),
        Row(value="2024-01-01 19:04:06.123456"),
    ]


def test_parquet_nanosecond_timestamp_is_reported_not_truncated(spark, tmp_path):
    # The counterpart of `test_csv_infer_schema_timestamp_fractional_digits`: a nanosecond
    # timestamp is widened for CSV, where Spark's reader truncates, but not for Parquet, where
    # Spark rejects `NANOS` rather than losing precision silently (SPARK-40819). Pinned here so
    # the two formats stay deliberately different rather than accidentally so.
    path = str(tmp_path / "parquet_nanosecond_timestamp.parquet")
    value = datetime(2018, 5, 1, 0, 0, 5, 123456, tzinfo=UTC)
    pq.write_table(
        pa.table({"t": pa.array([value], type=pa.timestamp("ns", tz="UTC"))}),
        path,
        version="2.6",
    )

    with pytest.raises(Exception, match="Nanosecond"):
        _ = spark.read.parquet(path).schema


@pytest.mark.parametrize("explicit", [False, True])
def test_parquet_rejects_nanos_for_microsecond_timestamp(spark, tmp_path, explicit):
    for i, unit in enumerate(["us", "ns"]):
        value = pa.array([123456789], pa.timestamp(unit))
        nested = pa.StructArray.from_arrays([value, pa.array([i])], names=["ts", "id"])
        pq.write_table(pa.table({"ts": value, "nested": nested}), tmp_path / f"{i}.parquet", version="2.6")

    reader = spark.read.option("mergeSchema", False)
    if explicit:
        reader = reader.schema("ts TIMESTAMP, nested STRUCT<ts:TIMESTAMP_NTZ,id:BIGINT>")
    df = reader.parquet(str(tmp_path))
    assert df.select("nested.id").orderBy("id").collect() == [(0,), (1,)]
    for column in ["ts", "nested.ts"]:
        with pytest.raises(Exception, match="PARQUET_COLUMN_DATA_TYPE_MISMATCH"):
            df.select(column).collect()


def test_parquet_binary_column_collects_as_binary(spark, tmp_path):
    # collect() is the honest probe (unlike toArrow(), which casts view types away): a binary
    # column read as BinaryView must still reach the client as Spark binary.
    path = str(tmp_path / "parquet_binary_collect.parquet")
    pq.write_table(pa.table({"b": pa.array([b"\x00\x01", None], type=pa.binary())}), path)
    df = spark.read.parquet(path)
    assert df.schema.simpleString() == "struct<b:binary>"
    assert {bytes(r.b) if r.b is not None else None for r in df.collect()} == {b"\x00\x01", None}


@pytest.mark.parametrize("force_view_types", ["true", "false"])
def test_parquet_merges_view_and_plain_columns(spark, tmp_path, force_view_types):
    # A directory can hold the same column as a view type in one file and a plain type in
    # another (e.g. a raw write followed by an INSERT). Inference must reconcile the two
    # whichever way `schemaForceViewTypes` is set.
    path = tmp_path / "mixed_view_and_plain"
    path.mkdir()
    view_string = "a string longer than twelve bytes"
    view_bytes = b"binary data longer than twelve bytes"
    pq.write_table(
        pa.table(
            {
                "s": pa.array([view_string], type=pa.string_view()),
                "b": pa.array([view_bytes], type=pa.binary_view()),
            }
        ),
        path / "a_view.parquet",
    )
    pq.write_table(
        pa.table({"s": pa.array(["plain"], type=pa.string()), "b": pa.array([b"plain"], type=pa.binary())}),
        path / "b_plain.parquet",
    )

    df = spark.read.option("schemaForceViewTypes", force_view_types).parquet(str(path))

    assert df.schema.simpleString() == "struct<s:string,b:binary>"
    assert sorted((r.s, bytes(r.b)) for r in df.collect()) == [
        (view_string, view_bytes),
        ("plain", b"plain"),
    ]


@pytest.mark.parametrize("file_uri", [False, True], ids=["native-path", "file-uri"])
def test_parquet_read_write_basic(spark, sample_df, tmp_path, file_uri):
    path = tmp_path / "parquet % # space"
    sample_df.write.parquet(str(path), mode="overwrite")
    # Path.as_uri() has no trailing slash even though the path is a directory.
    read_df = spark.read.parquet(path.as_uri() if file_uri else str(path))
    assert sample_df.count() == read_df.count()
    assert sample_df.schema == read_df.schema
    assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)


def test_parquet_path_glob_filter(spark, tmp_path):
    keep_source = tmp_path / "parquet_keep_source"
    drop_source = tmp_path / "parquet_drop_source"
    spark.createDataFrame([(1,)], "id INT").coalesce(1).write.parquet(str(keep_source))
    spark.createDataFrame([(2,)], "id INT").coalesce(1).write.parquet(str(drop_source))

    path = tmp_path / "parquet_path_glob_filter"
    path.mkdir()
    next(keep_source.glob("*.parquet")).rename(path / "keep.parquet")
    next(drop_source.glob("*.parquet")).rename(path / "drop.parquet")

    df = spark.read.option("pathGlobFilter", "keep.*").parquet(str(path))

    assert df.collect() == [Row(id=1)]


def test_parquet_write_modes(spark, tmp_path):
    path = str(tmp_path / "parquet_write_modes")

    spark.createDataFrame([(1, "old")], schema="id INT, value STRING").write.parquet(path)

    with pytest.raises(Exception, match="already exists"):
        spark.createDataFrame([(2, "error")], schema="id INT, value STRING").write.parquet(path)

    spark.createDataFrame([(3, "ignored")], schema="id INT, value STRING").write.mode("ignore").parquet(path)
    assert spark.read.parquet(path).orderBy("id").collect() == [Row(id=1, value="old")]

    spark.createDataFrame([(4, "appended")], schema="id INT, value STRING").write.mode("append").parquet(path)
    assert spark.read.parquet(path).orderBy("id").collect() == [
        Row(id=1, value="old"),
        Row(id=4, value="appended"),
    ]

    spark.createDataFrame([(5, "new")], schema="id INT, value STRING").write.mode("overwrite").parquet(path)
    assert spark.read.parquet(path).orderBy("id").collect() == [Row(id=5, value="new")]


def test_parquet_write_modes_with_empty_existing_path(spark, tmp_path):
    path = tmp_path / "parquet_write_modes_empty_existing_path"
    path.mkdir()

    with pytest.raises(Exception, match="already exists"):
        spark.createDataFrame([(1, "error")], schema="id INT, value STRING").write.parquet(str(path))
    assert list(path.iterdir()) == []

    spark.createDataFrame([(2, "ignored")], schema="id INT, value STRING").write.mode("ignore").parquet(str(path))
    assert list(path.iterdir()) == []


def test_parquet_read_write_compressed(spark, sample_df, sample_pandas_df, tmp_path):
    # Test reading a compressed Parquet file written by Sail
    path = str(tmp_path / "parquet_compressed_zstd")
    sample_df.write.option("compression", "zstd(4)").parquet(path, mode="overwrite")
    read_df = spark.read.parquet(path)
    assert sample_df.count() == read_df.count()
    assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)
    assert len(list((tmp_path / "parquet_compressed_zstd").glob("*.zst.parquet"))) > 0

    # Test reading a compressed Parquet file written by Pandas.
    path = tmp_path / "parquet_compressed_gzip_pandas_1"
    path.mkdir()
    path = str(path)
    sample_pandas_df.to_parquet(f"{path}/sample_pandas_df.parquet", compression="gzip")
    read_df = spark.read.parquet(path)
    assert len(sample_pandas_df) == read_df.count()
    assert sorted(sample_pandas_df.to_dict(orient="records"), key=safe_sort_key) == sorted(
        read_df.toPandas().to_dict(orient="records"), key=safe_sort_key
    )
    assert len(list((tmp_path / "parquet_compressed_gzip_pandas_1").glob("*.parquet"))) > 0

    # Test reading a compressed Parquet file written by Pandas with `.gz` in the filename.
    path = tmp_path / "parquet_compressed_gzip_pandas_2"
    path.mkdir()
    path = str(path)
    sample_pandas_df.to_parquet(f"{path}/sample_pandas_df.gz.parquet", compression="gzip")
    read_df = spark.read.parquet(path)
    assert len(sample_pandas_df) == read_df.count()
    assert sorted(sample_pandas_df.to_dict(orient="records"), key=safe_sort_key) == sorted(
        read_df.toPandas().to_dict(orient="records"), key=safe_sort_key
    )
    assert len(list((tmp_path / "parquet_compressed_gzip_pandas_2").glob("*.gz.parquet"))) > 0


def test_parquet_write_options(spark, sample_df, tmp_path):
    path = str(tmp_path / "parquet_write_options")
    sample_df.write.option("writerVersion", "1.0").parquet(path, mode="overwrite", compression="gzip(4)")
    read_df = spark.read.parquet(path)
    assert sample_df.count() == read_df.count()
    assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)
    assert len(list((tmp_path / "parquet_write_options").glob("*.gz.parquet"))) > 0

    path = str(tmp_path / "parquet_write_options_1")
    sample_df.write.option("writerVersion", "1.0").parquet(path, mode="overwrite", compression="snappy")
    read_df = spark.read.parquet(path)
    assert sample_df.count() == read_df.count()
    assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)
    assert len(list((tmp_path / "parquet_write_options_1").glob("*.snappy.parquet"))) > 0


def test_parquet_read_options(spark, sample_df, tmp_path):
    path = str(tmp_path / "parquet_read_options")
    sample_df.write.parquet(path, mode="overwrite")
    read_df = spark.read.option("binaryAsString", "false").option("pruning", "true").parquet(path)
    assert sample_df.count() == read_df.count()
    assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)


def write_unannotated_binary(path):
    """Writes a BYTE_ARRAY column without the UTF-8 annotation and without the Arrow schema,
    as produced by writers that do not distinguish binary data from strings."""
    path.mkdir()
    pq.write_table(
        pa.table({"value": pa.array([b"abc"], type=pa.binary())}),
        path / "data.parquet",
        store_schema=False,
    )
    return str(path)


def test_parquet_session_binary_as_string(spark, tmp_path):
    key = "spark.sql.parquet.binaryAsString"
    path = write_unannotated_binary(tmp_path / "session_binary_as_string")
    assert spark.read.parquet(path).dtypes == [("value", "binary")]
    try:
        spark.conf.set(key, "true")
        assert spark.read.parquet(path).dtypes == [("value", "string")]
        assert spark.read.parquet(path).first()[0] == "abc"

        spark.conf.set(key, "false")
        assert spark.read.parquet(path).dtypes == [("value", "binary")]
    finally:
        spark.conf.unset(key)
    assert spark.read.parquet(path).dtypes == [("value", "binary")]


def test_parquet_invalid_session_binary_as_string(spark, tmp_path):
    key = "spark.sql.parquet.binaryAsString"
    path = write_unannotated_binary(tmp_path / "invalid_session_binary_as_string")
    try:
        spark.conf.set(key, "true")
        with pytest.raises(Exception, match=r"INVALID_CONF_VALUE\.TYPE_MISMATCH"):
            spark.conf.set(key, "yes")
        assert spark.conf.get(key) == "true"
        assert spark.read.parquet(path).dtypes == [("value", "string")]
    finally:
        spark.conf.unset(key)


@pytest.mark.spark(skip_if_jvm=True, reason="`binaryAsString` is a Sail-only read option")
def test_parquet_read_option_overrides_session_binary_as_string(spark, tmp_path):
    key = "spark.sql.parquet.binaryAsString"
    path = write_unannotated_binary(tmp_path / "read_option_over_session_binary_as_string")
    try:
        spark.conf.set(key, "true")
        assert spark.read.option("binaryAsString", "false").parquet(path).dtypes == [("value", "binary")]

        spark.conf.set(key, "false")
        assert spark.read.option("binaryAsString", "true").parquet(path).dtypes == [("value", "string")]
    finally:
        spark.conf.unset(key)


@pytest.mark.parametrize(
    ("physical_schema", "requested_schema", "value"),
    [
        ("value DOUBLE", "value INT", 1.5),
        ("value INT", "value BOOLEAN", 1),
        ("value STRUCT<nested: DOUBLE>", "value STRUCT<nested: INT>", Row(nested=1.5)),
        ("value ARRAY<DOUBLE>", "value ARRAY<INT>", [1.5]),
        ("value MAP<STRING, DOUBLE>", "value MAP<STRING, INT>", {"key": 1.5}),
    ],
)
def test_parquet_explicit_schema_rejects_incompatible_types(spark, tmp_path, physical_schema, requested_schema, value):
    path = str(tmp_path / "incompatible_explicit_schema")
    spark.createDataFrame([(value,)], physical_schema).write.parquet(path)

    with pytest.raises(Exception, match="PARQUET_COLUMN_DATA_TYPE_MISMATCH"):
        spark.read.schema(requested_schema).parquet(path).collect()


def test_parquet_explicit_schema_allows_supported_widening(spark, tmp_path):
    path = str(tmp_path / "supported_explicit_schema_widening")
    spark.createDataFrame([(1, 1.5), (None, None)], "id INT, value FLOAT").write.parquet(path)

    rows = spark.read.schema("id BIGINT, value DOUBLE").parquet(path).orderBy("id").collect()

    assert rows == [Row(id=None, value=None), Row(id=1, value=1.5)]


def test_parquet_explicit_schema_allows_missing_fields(spark, tmp_path):
    path = str(tmp_path / "missing_explicit_schema_field")
    spark.createDataFrame([(1,)], "id INT").write.parquet(path)

    rows = spark.read.schema("id INT, missing STRING").parquet(path).collect()

    assert rows == [Row(id=1, missing=None)]


def test_parquet_single_footer_widens_small_integers(spark, tmp_path):
    for name, ty in [("a", pa.int64()), ("b", pa.int8()), ("c", pa.int16())]:
        pq.write_table(pa.table({"value": pa.array([42], type=ty)}), tmp_path / f"{name}.parquet")
    df = spark.read.option("mergeSchema", False).parquet(str(tmp_path))
    assert df.schema.simpleString() == "struct<value:bigint>"
    assert df.collect() == [Row(value=42)] * 3


def test_parquet_single_footer_reads_raw_temporal_values(spark, tmp_path):
    for file, types, value in [
        ("a", [pa.date32(), pa.int32(), pa.int64(), pa.int64(), pa.int64()], 1),
        ("b", [pa.int32(), pa.date32(), pa.timestamp("ms", tz="UTC"), pa.timestamp("us"), pa.timestamp("ns")], -42),
    ]:
        columns = {
            name: pa.array([value, None], ty)
            for name, ty in zip(["day", "days", "millis", "micros", "nanos"], types, strict=True)
        }
        if file == "b":
            columns["days"] = columns["days"].cast(pa.date64())
            columns["micros"] = columns["micros"].dictionary_encode()
        columns["nested"] = pa.StructArray.from_arrays([columns["nanos"]], names=["ticks"])
        columns["unchanged"] = pa.array([1_234, None], pa.timestamp("ms", tz="UTC"))
        pq.write_table(pa.table(columns), tmp_path / f"{file}.parquet", use_dictionary=False)

    df = spark.read.option("mergeSchema", False).parquet(str(tmp_path))
    assert df.selectExpr(
        "unix_date(day)", "days", "millis", "micros", "nanos", "nested.ticks", "unix_millis(unchanged)"
    ).orderBy("days").collect() == [
        (None,) * 7,
        (None,) * 7,
        (-42, -42, -42, -42, -42, -42, 1_234),
        (1, 1, 1, 1, 1, 1, 1_234),
    ]
    assert df.selectExpr("unix_date(min(day))", "min(days)", "min(millis)", "min(micros)", "min(nanos)").collect() == [
        (-42, -42, -42, -42, -42)
    ]
    assert df.where("days < 0 AND millis < 0 AND micros < 0").selectExpr("unix_date(day)").collect() == [(-42,)]


def test_parquet_single_footer_reads_date_and_time_storage(spark, tmp_path):
    names = ["tiny", "small", "whole", "wide", "day", "clock"]
    types = [pa.int8(), pa.int16(), pa.int64(), pa.float64(), pa.date32(), pa.int64()]
    first = {name: pa.array([1], ty) for name, ty in zip(names, types, strict=True)}
    first["nested"] = pa.StructArray.from_arrays([first["tiny"], first["clock"]], names=["tiny", "clock"])
    pq.write_table(pa.table(first), tmp_path / "a.parquet", use_dictionary=False)

    days = pa.array([32768, -42, None], pa.date32())
    later = dict.fromkeys(names[:4], days)
    later["day"] = pa.array([123, -42, None], pa.int16())
    later["clock"] = pa.array([1234, 5678, None], pa.time64("us"))
    later["nested"] = pa.StructArray.from_arrays([days, later["clock"]], names=["tiny", "clock"])
    pq.write_table(pa.table(later), tmp_path / "b.parquet", use_dictionary=False)

    df = spark.read.option("mergeSchema", False).parquet(str(tmp_path))
    assert df.orderBy("clock").selectExpr(
        "tiny", "small", "whole", "wide", "unix_date(day)", "clock", "nested.tiny", "nested.clock"
    ).collect() == [
        (None,) * 8,
        (1, 1, 1, 1.0, 1, 1, 1, 1),
        (0, -32768, 32768, 32768.0, 123, 1234, 0, 1234),
        (-42, -42, -42, -42.0, -42, 5678, -42, 5678),
    ]
    assert df.selectExpr("min(small)", "max(whole)", "max(wide)", "max(clock)").collect() == [
        (-32768, 32768, 32768.0, 5678)
    ]
    assert df.where("small = -32768 AND nested.clock > 1000").selectExpr("unix_date(day)").collect() == [(123,)]


def test_parquet_single_footer_rejects_int96_as_bigint(spark, tmp_path):
    for file, ty in [("a", pa.int64()), ("b", pa.timestamp("us"))]:
        value = pa.array([42], ty)
        nested = pa.StructArray.from_arrays([value, pa.array([1])], names=["value", "id"])
        pq.write_table(
            pa.table({"value": value, "nested": nested}),
            tmp_path / f"{file}.parquet",
            use_deprecated_int96_timestamps=True,
        )
    df = spark.read.option("mergeSchema", False).parquet(str(tmp_path))
    assert df.select("nested.id").collect() == [(1,), (1,)]
    for column in ["value", "nested.value"]:
        with pytest.raises(Exception, match="PARQUET_COLUMN_DATA_TYPE_MISMATCH"):
            df.select(column).collect()


def test_parquet_integer_backed_decimals_read_as_unscaled_integers(spark, tmp_path):
    first = pa.table({"i": pa.array([1], pa.int32()), "l": pa.array([1], pa.int64())})
    pq.write_table(first, tmp_path / "a.parquet")
    values = [Decimal("-1234.56"), Decimal("78.90"), None]
    decimals = pa.table({"i": pa.array(values, pa.decimal128(7, 2)), "l": pa.array(values, pa.decimal128(15, 2))})
    pq.write_table(decimals, tmp_path / "b.parquet", store_decimal_as_integer=True, use_dictionary=False)
    df = spark.read.option("mergeSchema", False).parquet(str(tmp_path))
    assert df.orderBy("i").collect() == [(None, None), (-123456, -123456), (1, 1), (7890, 7890)]
    assert df.selectExpr("min(i)", "max(l)").collect() == [(-123456, 7890)]

    pq.write_table(decimals, tmp_path / "c.parquet", store_decimal_as_integer=False, use_dictionary=False)
    with pytest.raises(Exception, match="PARQUET_COLUMN_DATA_TYPE_MISMATCH"):
        spark.read.option("mergeSchema", False).parquet(str(tmp_path)).collect()


def test_parquet_requested_integer_from_null_column(spark, tmp_path):
    pq.write_table(pa.table({"v": pa.array([7], pa.int32())}), tmp_path / "a.parquet")
    path = tmp_path / "b.parquet"
    pq.write_table(pa.table({"v": pa.nulls(2)}), path)
    df = spark.read.option("mergeSchema", False).parquet(str(tmp_path))
    assert df.orderBy("v").collect() == [(None,), (None,), (7,)]
    assert df.where("v IS NULL").count() == 2
    assert df.selectExpr("count(v)", "min(v)").collect() == [(1, 7)]
    with pytest.raises(Exception, match="PARQUET_COLUMN_DATA_TYPE_MISMATCH"):
        spark.read.schema("v STRING").parquet(str(path)).collect()


@pytest.mark.parametrize("use_dictionary", [True, False])
def test_parquet_requested_binary_from_decimal(spark, tmp_path, use_dictionary):
    path = tmp_path / "decimal.parquet"
    values = pa.array([Decimal("-1.00"), Decimal("1.00"), None], pa.decimal128(7, 2))
    items = pa.ListArray.from_arrays([0, 1, 2, 3], values)
    pq.write_table(pa.table({"v": values, "items": items}), path, use_dictionary=use_dictionary)
    df = spark.read.schema("v BINARY, items ARRAY<BINARY>").parquet(str(path))
    assert df.selectExpr("hex(v)").collect() == [("FFFFFF9C",), ("00000064",), (None,)]
    assert df.selectExpr("hex(items[0])").collect() == [("FFFFFF9C",), ("00000064",), (None,)]
    assert df.selectExpr("hex(min(v))", "hex(max(v))", "count(v)").collect() == [("00000064", "FFFFFF9C", 2)]
    assert df.where("v > unhex('7f')").selectExpr("hex(v)").collect() == [("FFFFFF9C",)]


def test_parquet_binary_from_mixed_width_decimals(spark, tmp_path):
    narrow, wide = tmp_path / "narrow.parquet", tmp_path / "wide.parquet"
    values = pa.array([Decimal("-2.00"), Decimal("2.00"), None], pa.decimal128(7, 2))
    pq.write_table(pa.table({"v": values}), narrow, store_schema=False)
    # PyArrow uses the minimum width for each precision. This fixture was written
    # as DECIMAL(18,2) and both precision fields in the footer were set to 7.
    wide.write_bytes(
        base64.b64decode(
            "UEFSMRUAFSwVLCwVBhUAFQYVBhwAAAACAAAAAwP/////////nAAAAAAAAABkFQQZLDUAGAZzY2hlbWEVAgAVDhUQFQIYAXYlChUE"
            "FQ4sXBUEFQ4AAAAWBhkcGRwmABwVDhklBgAZGAF2FQAWBhZSFlImCEkcFQAVABUCADwpBhkmAgQAAAAWUhYGJggWUgAoIHBhcnF1"
            "ZXQtY3BwLWFycm93IHZlcnNpb24gMjQuMC4wGRwcAAAAigAAAFBBUjE="
        )
    )
    assert pq.read_schema(narrow) == pq.read_schema(wide)
    assert [pq.ParquetFile(path).schema.column(0).length for path in (narrow, wide)] == [4, 8]

    df = spark.read.schema("v BINARY").parquet(str(tmp_path))
    assert df.selectExpr("hex(v) AS h").orderBy("h").collect() == [
        (None,),
        (None,),
        ("0000000000000064",),
        ("000000C8",),
        ("FFFFFF38",),
        ("FFFFFFFFFFFFFF9C",),
    ]


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
def test_parquet_arrow_timestamp_hint_on_unannotated_int64_is_bigint(spark, tmp_path, unit):
    # The footer declares a plain INT64. Spark reads its stored value for every Arrow timestamp hint.
    path = tmp_path / "arrow_timestamp_hint.parquet"
    encoded_arrow_schema = base64.b64encode(
        pa.schema([pa.field("ts", pa.timestamp(unit))]).serialize().to_pybytes()
    ).decode()
    table = pa.table({"ts": pa.array([1704164645, -1], type=pa.int64())})
    with pq.ParquetWriter(path, table.schema, store_schema=False) as writer:
        writer.add_key_value_metadata({"ARROW:schema": encoded_arrow_schema})
        writer.write_table(table)

    df = spark.read.parquet(str(path))

    assert df.schema.simpleString() == "struct<ts:bigint>"
    assert df.orderBy("ts").collect() == [
        Row(ts=-1),
        Row(ts=1704164645),
    ]


def test_parquet_explicit_year_month_interval_reads_int32_months(spark, tmp_path):
    pq.write_table(pa.table({"v": pa.array([-13, None, 14], pa.int32())}), tmp_path / "months.parquet")
    df = spark.read.schema("v INTERVAL YEAR TO MONTH").parquet(str(tmp_path))
    assert df.selectExpr("CAST(v AS STRING) AS v").orderBy("v").collect() == [
        (None,),
        ("INTERVAL '-1-1' YEAR TO MONTH",),
        ("INTERVAL '1-2' YEAR TO MONTH",),
    ]


@pytest.mark.spark(skip_if_jvm=True, reason="Checks interval qualifiers in Sail-written Arrow schema metadata")
@pytest.mark.parametrize("skip_metadata", [False, True])
def test_parquet_sail_interval_round_trip(spark, tmp_path, skip_metadata):
    path = str(tmp_path / "intervals")
    spark.sql("SELECT INTERVAL '2' HOUR AS v, named_struct('x', INTERVAL '-3' MINUTE) AS s").write.parquet(path)
    reader = spark.read.option("skipMetadata", skip_metadata)

    inferred = reader.parquet(path)
    assert inferred.schema.simpleString() == "struct<v:interval hour,s:struct<x:interval day to second>>"
    assert inferred.collect() == [Row(v=timedelta(hours=2), s=Row(x=timedelta(minutes=-3)))]

    for option, value in [("basePath", path), ("mergeSchema", True)]:
        inferred = spark.read.option("skipMetadata", skip_metadata).option(option, value).parquet(path)
        assert inferred.schema.simpleString() == "struct<v:interval hour,s:struct<x:bigint>>"
        assert inferred.collect() == [Row(v=timedelta(hours=2), s=Row(x=-180_000_000))]

    raw = reader.schema("v BIGINT, s STRUCT<x: BIGINT>").parquet(path)
    assert raw.collect() == [Row(v=7_200_000_000, s=Row(x=-180_000_000))]


def test_parquet_explicit_interval_reads_int64_ticks(spark, tmp_path):
    for i, ty in enumerate([pa.duration("us"), pa.int64()]):
        ticks = pa.array([-1, None, 42], ty)
        nested = pa.StructArray.from_arrays([ticks], names=["x"])
        pq.write_table(pa.table({"v": ticks, "s": nested}), tmp_path / f"{i}.parquet")

    df = spark.read.schema("v INTERVAL DAY TO SECOND, s STRUCT<x:INTERVAL DAY TO SECOND>").parquet(str(tmp_path))
    rows = df.select("v", "s.x").orderBy("v").collect()
    assert rows == [
        (value, value)
        for value in [
            None,
            None,
            timedelta(microseconds=-1),
            timedelta(microseconds=-1),
            timedelta(microseconds=42),
            timedelta(microseconds=42),
        ]
    ]


def test_parquet_intervals_read_annotated_integer_storage(spark, tmp_path):
    month_count = 14
    micros = 43
    columns = {
        "d": pa.array([month_count], pa.date32()),
        "t": pa.array([micros], pa.timestamp("ms")),
        "small": pa.array([month_count], pa.int8()),
        "u32": pa.array([2**32 - 1], pa.uint32()),
        "u64": pa.array([2**64 - 1], pa.uint64()),
        "d32": pa.array([Decimal("0.14")], pa.decimal128(8, 2)),
        "d64": pa.array([Decimal("0.43")], pa.decimal128(16, 2)),
    }
    pq.write_table(
        pa.table(columns), tmp_path / "annotated.parquet", store_decimal_as_integer=True, use_dictionary=False
    )
    year_month = ["d", "small", "u32", "d32"]
    day_time = ["t", "u64", "d64"]
    schema = [f"{col} INTERVAL YEAR TO MONTH" for col in year_month] + [
        f"{col} INTERVAL DAY TO SECOND" for col in day_time
    ]
    df = spark.read.schema(", ".join(schema)).parquet(str(tmp_path))
    expressions = [f"CAST({col} AS STRING)" for col in year_month] + day_time
    positive = "INTERVAL '1-2' YEAR TO MONTH"
    negative = "INTERVAL '-0-1' YEAR TO MONTH"
    assert df.selectExpr(*expressions).collect() == [
        (
            positive,
            positive,
            negative,
            positive,
            timedelta(microseconds=micros),
            timedelta(microseconds=-1),
            timedelta(microseconds=micros),
        )
    ]
    assert df.selectExpr("CAST(min(u32) AS STRING)", "max(u64)", "min(d64)").collect() == [
        (negative, timedelta(microseconds=-1), timedelta(microseconds=micros))
    ]


def test_parquet_single_footer_interval_reads_annotated_ticks(spark, tmp_path):
    written = tmp_path / "written"
    spark.sql("SELECT INTERVAL '1' SECOND AS v").coalesce(1).write.parquet(str(written))
    next(written.glob("*.parquet")).rename(tmp_path / "a.parquet")
    pq.write_table(pa.table({"v": pa.array([42], pa.timestamp("ms"))}), tmp_path / "b.parquet")
    pq.write_table(pa.table({"v": pa.array([3], pa.uint64())}), tmp_path / "c.parquet", use_dictionary=False)
    df = spark.read.option("mergeSchema", False).parquet(str(tmp_path / "*.parquet"))
    assert df.orderBy("v").collect() == [(timedelta(microseconds=n),) for n in [3, 42, 1_000_000]]


@pytest.mark.parametrize(
    ("physical", "requested", "int96"),
    [
        (pa.date32(), "DAY TO SECOND", False),
        (pa.timestamp("us"), "YEAR TO MONTH", False),
        (pa.timestamp("us"), "DAY TO SECOND", True),
    ],
)
def test_parquet_interval_rejects_other_storage_widths(spark, tmp_path, physical, requested, int96):
    path = tmp_path / "wrong.parquet"
    pq.write_table(pa.table({"v": pa.array([1], physical)}), path, use_deprecated_int96_timestamps=int96)
    df = spark.read.schema(f"v INTERVAL {requested}").parquet(str(path))
    with pytest.raises(Exception, match="PARQUET_COLUMN_DATA_TYPE_MISMATCH"):
        df.selectExpr("CAST(v AS STRING)").collect()


def test_parquet_explicit_schema_uses_unannotated_integer_storage(spark, tmp_path):
    path = tmp_path / "data.parquet"
    values = pa.array([-1, 42], pa.int64())
    table = pa.table({"v": values, "nested": pa.StructArray.from_arrays([values], names=["x"])})
    hint = pa.schema([("v", pa.timestamp("s")), ("nested", pa.struct([("x", pa.timestamp("us"))]))])
    with pq.ParquetWriter(path, table.schema, store_schema=False) as writer:
        writer.add_key_value_metadata({"ARROW:schema": base64.b64encode(hint.serialize()).decode()})
        writer.write_table(table)

    for field, column in [("v TIMESTAMP_NTZ", "v"), ("nested STRUCT<x:TIMESTAMP_NTZ>", "nested.x")]:
        with pytest.raises(Exception, match="PARQUET_COLUMN_DATA_TYPE_MISMATCH"):
            spark.read.schema(field).parquet(str(path)).select(column).collect()

    df = spark.read.schema("v DECIMAL(20,0), nested STRUCT<x:DECIMAL(20,0)>").parquet(str(path))
    assert df.select("v", "nested.x").orderBy("v").collect() == [(Decimal(-1), Decimal(-1)), (Decimal(42), Decimal(42))]
    assert df.selectExpr("min(v)", "max(v)").collect() == [(Decimal(-1), Decimal(42))]


@pytest.mark.parametrize("requested_type", ["TIMESTAMP", "TIMESTAMP_NTZ"])
def test_parquet_explicit_schema_determines_int96_timestamp_family(spark, tmp_path, requested_type):
    path = tmp_path / "int96_explicit_timestamp_schema.parquet"
    table = pa.table(
        {
            "value": pa.array(
                [datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)],
                type=pa.timestamp("us"),
            )
        }
    )
    pq.write_table(table, path, use_deprecated_int96_timestamps=True)

    rows = (
        spark.read.schema(f"value {requested_type}")
        .parquet(str(path))
        .selectExpr("CAST(value AS STRING) AS value")
        .collect()
    )

    assert rows == [Row(value="2024-01-02 03:04:05")]


@pytest.mark.usefixtures("session_timezone")
@pytest.mark.parametrize("session_timezone", ["Asia/Shanghai"], indirect=True)
def test_parquet_int96_with_arrow_schema_hint_reads_as_timestamp(spark, tmp_path):
    # pyarrow stores an `ARROW:schema` hint with the unit of the values, which Spark ignores.
    path = tmp_path / "int96_with_arrow_schema_hint.parquet"
    values = [datetime(2020, 6, 14), datetime(2020, 6, 14, 8), datetime(2020, 6, 13, 16)]  # noqa: DTZ001
    table = pa.table({"id": [1, 3, 4], "value": pa.array(values, type=pa.timestamp("us"))})
    pq.write_table(table, path, use_deprecated_int96_timestamps=True)

    spark.read.parquet(str(path)).createOrReplaceTempView("int96_with_arrow_schema_hint")
    rows = spark.sql(
        """
        SELECT id, typeof(value) AS type, CAST(value AS STRING) AS value,
          value >= TIMESTAMP '2020-06-14 05:30:00' AS ge
        FROM int96_with_arrow_schema_hint
        ORDER BY id
        """
    ).collect()

    assert rows == [
        Row(id=1, type="timestamp", value="2020-06-14 08:00:00", ge=True),
        Row(id=3, type="timestamp", value="2020-06-14 16:00:00", ge=True),
        Row(id=4, type="timestamp", value="2020-06-14 00:00:00", ge=False),
    ]


@pytest.mark.usefixtures("session_timezone")
@pytest.mark.parametrize("session_timezone", ["UTC"], indirect=True)
def test_parquet_int96_with_arrow_schema_hint_before_1677(spark, tmp_path):
    path = tmp_path / "int96_before_1677.parquet"
    values = [datetime(1582, 10, 15), datetime(1, 1, 1), datetime(2020, 6, 14)]  # noqa: DTZ001
    table = pa.table({"id": [0, 1, 2], "value": pa.array(values, type=pa.timestamp("us"))})
    pq.write_table(table, path, use_deprecated_int96_timestamps=True)

    spark.read.parquet(str(path)).createOrReplaceTempView("int96_before_1677")
    rows = spark.sql(
        """
        SELECT id, weekofyear(value) AS week, dayname(value) AS day, monthname(value) AS month,
          CAST(date_trunc('MONTH', value) AS STRING) AS month_start
        FROM int96_before_1677
        ORDER BY id
        """
    ).collect()

    assert rows == [
        Row(id=0, week=41, day="Fri", month="Oct", month_start="1582-10-01 00:00:00"),
        Row(id=1, week=1, day="Mon", month="Jan", month_start="0001-01-01 00:00:00"),
        Row(id=2, week=24, day="Sun", month="Jun", month_start="2020-06-01 00:00:00"),
    ]


def test_parquet_int96_in_legacy_list(spark, tmp_path):
    path = tmp_path / "legacy_list.parquet"
    values = [datetime(year, 6, 15, tzinfo=UTC) for year in [1600, 2500]]
    array = pa.array([values], type=pa.list_(pa.timestamp("us", tz="UTC")))
    pq.write_table(pa.table({"ts": array}), path, use_deprecated_int96_timestamps=True, store_schema=False)

    # Rename the repeated LIST wrapper in the footer, preserving its byte length and all page offsets.
    data = path.read_bytes()
    footer_start = len(data) - 8 - int.from_bytes(data[-8:-4], "little")
    path.write_bytes(data[:footer_start] + data[footer_start:].replace(b"\x04list", b"\x04bags"))
    assert pq.ParquetFile(path).schema.column(0).path == "ts.bags.element"

    rows = spark.read.parquet(str(path)).selectExpr("year(ts[0])", "year(ts[1])").collect()
    assert rows == [(1600, 2500)]


def test_parquet_write_with_bloom_filter(spark, tmpdir):
    def size(p):
        return get_data_directory_size(p, extension=".parquet")

    path = str(tmpdir / "default")
    spark.sql("SELECT 1").write.parquet(path)
    # The Parquet file without bloom filter is small (less than 1 kB).
    base_size = size(path)
    assert base_size < 1024

    path = str(tmpdir / "bloom_filter_off_explicit")
    (
        spark.sql("SELECT 1")
        .write.option("bloom_filter_on_write", "false")
        .option("bloom_filter_fpp", "0.05")
        .option("bloom_filter_ndv", "10000")
        .parquet(path)
    )
    assert size(path) < 1024

    path = str(tmpdir / "bloom_filter_off_implicit")
    (
        spark.sql("SELECT 1")
        # The default configuration does not enable bloom filters on write.
        .write.option("bloom_filter_fpp", "0.05")
        .option("bloom_filter_ndv", "10000")
        .parquet(path)
    )
    assert size(path) < 1024

    path = str(tmpdir / "bloom_filter_on")
    (
        spark.sql("SELECT 1")
        .write.option("bloom_filter_on_write", "true")
        .option("bloom_filter_fpp", "0.05")
        .option("bloom_filter_ndv", "10000")
        .parquet(path)
    )
    # Bloom filter adds overhead; file must be larger than the base (no bloom filter) size.
    assert size(path) > base_size

    path = str(tmpdir / "bloom_filter_on_with_multiple_columns")
    (
        spark.sql("SELECT 1, 2")
        .write.option("bloom_filter_on_write", "true")
        .option("bloom_filter_fpp", "0.05")
        .option("bloom_filter_ndv", "10000")
        .parquet(path)
    )
    # Two columns produce a larger bloom filter than one column.
    assert size(path) > size(str(tmpdir / "bloom_filter_on"))


def test_parquet_write_with_path_option(spark, tmpdir):
    """Test that df.write.format("parquet").option("path", path).save() works (issue #811)."""
    data = [(1, "Alice"), (2, "Bob")]
    df = spark.createDataFrame(data, schema="id INT, name STRING")

    path = str(tmpdir / "output")
    df.write.format("parquet").option("path", path).save()

    actual = spark.read.parquet(path).orderBy("id").toPandas()
    expected = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]}).astype({"id": "int32"})
    assert_frame_equal(actual, expected)


def test_parquet_format_path(spark, sample_df, tmp_path):
    path = str(tmp_path / "data.parquet")
    sample_df.write.parquet(path, mode="overwrite")
    df = spark.sql(f"SELECT * FROM parquet.`{escape_sql_identifier(path)}`")  # noqa: S608
    assert df.count() == sample_df.count()
    assert sorted(df.collect(), key=safe_sort_key) == sorted(sample_df.collect(), key=safe_sort_key)


def test_parquet_read_with_custom_extension(spark, sample_pandas_df, tmp_path):
    """`pathGlobFilter` selects Parquet files with a custom suffix."""
    directory = tmp_path / "parquet_custom_extension"
    directory.mkdir()
    file_path = directory / "data.hive"
    sample_pandas_df.to_parquet(str(file_path))
    sample_pandas_df.to_parquet(str(directory / "ignored.parquet"))

    expected_count = len(sample_pandas_df)
    expected_rows = sorted(sample_pandas_df.to_dict(orient="records"), key=safe_sort_key)

    def actual_rows(df):
        return sorted(df.toPandas().to_dict(orient="records"), key=safe_sort_key)

    read_df = spark.read.option("pathGlobFilter", "*.hive").parquet(str(directory))
    assert read_df.count() == expected_count
    assert actual_rows(read_df) == expected_rows

    # SQL CREATE TABLE with OPTIONS (pathGlobFilter '*.hive').
    table_name = "parquet_custom_extension_table"
    try:
        spark.sql(
            f"CREATE TABLE {table_name} USING parquet "
            f"OPTIONS (pathGlobFilter '*.hive') "
            f"LOCATION '{escape_sql_string_literal(str(directory))}'"
        )
        read_df = spark.sql(f"SELECT * FROM {table_name}")  # noqa: S608
        assert read_df.count() == expected_count
        assert actual_rows(read_df) == expected_rows
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_parquet_read_uppercase_extension(spark, sample_df, tmp_path):
    # Extensions are matched case-insensitively, so renaming the parquet
    # file's extension to `.PARQUET` must still allow it to be read.
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    dst = tmp_path / "dst"
    dst.mkdir()
    for i, f in enumerate(src.glob("*.parquet")):
        f.rename(dst / f"part-{i}.PARQUET")
    df = spark.read.parquet(str(dst))
    assert df.count() == sample_df.count()
    assert sorted(df.collect(), key=safe_sort_key) == sorted(sample_df.collect(), key=safe_sort_key)


# -----------------------------------------------------------------------------
# Case-insensitive extension matching for Parquet, plus partition-aware reads.
# Sail reads every non-hidden file in a directory regardless of extension case
# (matching Spark). The tests below cover that across single files,
# directories, partitioned trees, mixed case, schema-shape variations, and
# user-supplied globs.
# -----------------------------------------------------------------------------


def _rename_part_files(src, dst, ext):
    """Move every `*.parquet` file from `src` into `dst` with the given extension."""
    dst.mkdir()
    for i, f in enumerate(src.glob("*.parquet")):
        f.rename(dst / f"part-{i}.{ext}")


@pytest.mark.parametrize("ext", ["PARQUET", "Parquet", "ParQuet", "parqueT"])
def test_parquet_read_uppercase_extension_file(spark, sample_df, tmp_path, ext):
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    dst = tmp_path / "dst"
    _rename_part_files(src, dst, ext)
    files = list(dst.glob(f"*.{ext}"))
    assert files, "expected renamed parquet files"
    df = spark.read.parquet(str(files[0]))
    assert df.count() > 0


@pytest.mark.parametrize("ext", ["PARQUET", "Parquet"])
def test_parquet_read_uppercase_extension_with_schema_struct_file(spark, sample_df, tmp_path, ext):
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    dst = tmp_path / "dst"
    _rename_part_files(src, dst, ext)
    files = list(dst.glob(f"*.{ext}"))
    # `sample_df` is tiny (4 rows) so a single output file is expected.
    assert len(files) == 1
    df = spark.read.schema(sample_df.schema).parquet(str(files[0]))
    assert df.count() == sample_df.count()
    assert sorted(df.collect(), key=safe_sort_key) == sorted(sample_df.collect(), key=safe_sort_key)


@pytest.mark.parametrize("ext", ["PARQUET", "Parquet"])
def test_parquet_read_uppercase_extension_with_schema_directory(spark, sample_df, tmp_path, ext):
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    dst = tmp_path / "dst"
    _rename_part_files(src, dst, ext)
    df = spark.read.schema(sample_df.schema).parquet(str(dst))
    assert df.count() == sample_df.count()
    assert sorted(df.collect(), key=safe_sort_key) == sorted(sample_df.collect(), key=safe_sort_key)


def test_parquet_read_lowercase_extension_with_schema_regression(spark, sample_df, tmp_path):
    # Regression: lowercase `.parquet` with schema must keep working.
    path = str(tmp_path / "lower")
    sample_df.write.parquet(path, mode="overwrite")
    df = spark.read.schema(sample_df.schema).parquet(path)
    assert df.count() == sample_df.count()


def test_parquet_read_uppercase_extension_with_schema_subset_columns(spark, sample_df, tmp_path):
    # Parquet column projection is by name: schema with one field should
    # only return that column.
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    dst = tmp_path / "dst"
    _rename_part_files(src, dst, "PARQUET")
    df = spark.read.schema("col1 STRING").parquet(str(dst))
    assert df.columns == ["col1"]
    assert df.count() == sample_df.count()


def test_parquet_read_mixed_case_directory_with_schema(spark, sample_df, tmp_path):
    # Spark parity: directory with both `.parquet` and `.PARQUET` reads
    # every non-hidden file regardless of extension case.
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    mixed = tmp_path / "mixed"
    mixed.mkdir()
    for i, f in enumerate(src.glob("*.parquet")):
        if i % 2 == 0:
            f.rename(mixed / f"part-{i}.parquet")
        else:
            f.rename(mixed / f"part-{i}.PARQUET")
    df = spark.read.schema(sample_df.schema).parquet(str(mixed))
    assert df.count() == sample_df.count()
    assert sorted(df.collect(), key=safe_sort_key) == sorted(sample_df.collect(), key=safe_sort_key)


def test_parquet_read_uppercase_extension_partitioned_directory(spark, tmp_path):
    # Partitioned write produces a partitioned tree under a directory.
    # Renaming every leaf `.parquet` to `.PARQUET` must still let the table
    # be read. Relies on partition discovery (no `.schema()`) since
    # `part` lives only in the directory name, not in the file.
    df_in = spark.createDataFrame(
        [(1, "a", "x"), (2, "b", "x"), (3, "c", "y")],
        "id INT, val STRING, part STRING",
    )
    src = tmp_path / "src"
    df_in.write.partitionBy("part").parquet(str(src), mode="overwrite")
    for f in src.rglob("*.parquet"):
        f.rename(f.with_suffix(".PARQUET"))
    df = spark.read.parquet(str(src))
    rows = sorted(df.collect(), key=lambda r: r.id)
    assert rows == [
        Row(id=1, val="a", part="x"),
        Row(id=2, val="b", part="x"),
        Row(id=3, val="c", part="y"),
    ]


def test_parquet_read_uppercase_extension_partitioned_directory_with_schema(spark, tmp_path):
    # Same as the previous test but with an explicit schema that includes
    # the partition column. Spark recognizes `part` as a partition column
    # from the directory structure even when a schema is supplied; Sail
    # should match that so users migrating from Spark don't see a regression.
    df_in = spark.createDataFrame(
        [(1, "a", "x"), (2, "b", "x"), (3, "c", "y")],
        "id INT, val STRING, part STRING",
    )
    src = tmp_path / "src"
    df_in.write.partitionBy("part").parquet(str(src), mode="overwrite")
    for f in src.rglob("*.parquet"):
        f.rename(f.with_suffix(".PARQUET"))
    df = spark.read.schema("id INT, val STRING, part STRING").parquet(str(src))
    rows = sorted(df.collect(), key=lambda r: r.id)
    assert rows == [
        Row(id=1, val="a", part="x"),
        Row(id=2, val="b", part="x"),
        Row(id=3, val="c", part="y"),
    ]


def test_parquet_hidden_files_are_excluded(spark, sample_df, tmp_path):
    # `_SUCCESS`, `_committed_*`, and `.crc` files commonly land alongside
    # parquet output. The default URL glob (`[!._]*`) must skip them so the
    # parquet reader doesn't try to parse them as parquet.
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    # Drop in a few hidden marker files of the kinds Spark / Hadoop tools
    # write next to data files.
    (src / "_SUCCESS").write_text("")
    (src / "_committed_xyz").write_text("garbage")
    (src / ".crc").write_text("garbage")
    df = spark.read.parquet(str(src))
    assert df.count() == sample_df.count()
    assert sorted(df.collect(), key=safe_sort_key) == sorted(sample_df.collect(), key=safe_sort_key)


@pytest.mark.skip(
    reason=(
        "FIXME: Sail's partition-column type inference returns every "
        "discovered column as STRING; Spark infers Int/Double/String from "
        "the observed `key=value` strings (see FIXME above "
        "`rewrite_listing_partitions` in `crates/sail-data-source/src/listing.rs`)."
    )
)
@pytest.mark.parametrize("provide_schema", [True, False])
def test_parquet_read_partitioned_directory_type_inference(spark, tmp_path, provide_schema):
    # Verify partition-column type inference against Spark behavior across
    # int / double / string. The original DataFrame columns are
    # `int_part INT, float_part DOUBLE, string_part STRING`.
    #
    # No-schema (Spark `partitionColumnTypeInference.enabled=true`, default):
    #   - int_part      → integer (all values parse as int)
    #   - float_part    → double  (all values parse as float)
    #   - string_part   → string  (alpha/beta don't parse numerically)
    #
    # With-schema: declared types are honored.
    df_in = spark.createDataFrame(
        [
            (1, "a", 2024, 1.5, "alpha"),
            (2, "b", 2024, 2.5, "beta"),
            (3, "c", 2025, 3.0, "alpha"),
        ],
        "id INT, val STRING, int_part INT, float_part DOUBLE, string_part STRING",
    )
    src = tmp_path / "src"
    df_in.write.partitionBy("int_part", "float_part", "string_part").parquet(str(src), mode="overwrite")

    if provide_schema:
        df = (
            spark.read.schema("id INT, val STRING, int_part INT, float_part DOUBLE, string_part STRING")
            .parquet(str(src))
            .orderBy("id")
        )
        type_by_name = {f.name: f.dataType.simpleString() for f in df.schema.fields}
        assert type_by_name["int_part"] == "int"
        assert type_by_name["float_part"] == "double"
        assert type_by_name["string_part"] == "string"
        rows = df.collect()
        assert rows == [
            Row(
                id=1,
                val="a",
                int_part=2024,
                float_part=1.5,
                string_part="alpha",
            ),
            Row(
                id=2,
                val="b",
                int_part=2024,
                float_part=2.5,
                string_part="beta",
            ),
            Row(
                id=3,
                val="c",
                int_part=2025,
                float_part=3.0,
                string_part="alpha",
            ),
        ]
    else:
        df = spark.read.parquet(str(src)).orderBy("id")
        type_by_name = {f.name: f.dataType.simpleString() for f in df.schema.fields}
        assert type_by_name["int_part"] == "int"
        assert type_by_name["float_part"] == "double"
        assert type_by_name["string_part"] == "string"
        rows = df.collect()
        assert rows == [
            Row(id=1, val="a", int_part=2024, float_part=1.5, string_part="alpha"),
            Row(id=2, val="b", int_part=2024, float_part=2.5, string_part="beta"),
            Row(id=3, val="c", int_part=2025, float_part=3.0, string_part="alpha"),
        ]


# TODO: remove this test once the FIXME above `rewrite_listing_partitions`
# in `crates/sail-data-source/src/listing.rs` is addressed and the
# Spark-parity test above starts passing.
@pytest.mark.parametrize("provide_schema", [True, False])
def test_parquet_read_partitioned_directory_type_inference_string_only(spark, tmp_path, provide_schema):
    # Pins Sail's current behavior: every partition column comes back as
    # STRING regardless of the underlying values. Once partition type
    # inference matches Spark, this test should be deleted in favor of the
    # `test_parquet_read_partitioned_directory_type_inference` above.
    df_in = spark.createDataFrame(
        [
            (1, "a", 2024, 1.5, "alpha"),
            (2, "b", 2024, 2.5, "beta"),
            (3, "c", 2025, 3.0, "alpha"),
        ],
        "id INT, val STRING, int_part INT, float_part DOUBLE, string_part STRING",
    )
    src = tmp_path / "src"
    df_in.write.partitionBy("int_part", "float_part", "string_part").parquet(str(src), mode="overwrite")

    if provide_schema:
        df = (
            spark.read.schema("id INT, val STRING, int_part STRING, float_part STRING, string_part STRING")
            .parquet(str(src))
            .orderBy("id")
        )
    else:
        df = spark.read.parquet(str(src)).orderBy("id")

    type_by_name = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert type_by_name["int_part"] == "string"
    assert type_by_name["float_part"] == "string"
    assert type_by_name["string_part"] == "string"
    rows = df.collect()
    # Note: Sail's writer drops trailing zeros when stringifying a DOUBLE
    # partition value (`3.0` → `"3"`). Spark would write `"3.0"`. Pinned
    # against Sail's current behavior.
    assert rows == [
        Row(id=1, val="a", int_part="2024", float_part="1.5", string_part="alpha"),
        Row(id=2, val="b", int_part="2024", float_part="2.5", string_part="beta"),
        Row(id=3, val="c", int_part="2025", float_part="3", string_part="alpha"),
    ]


@pytest.mark.skip(
    reason=(
        "FIXME: Sail's partition-column type inference returns every "
        "discovered column as STRING; Spark infers Int/Double/String from "
        "the observed `key=value` strings (see FIXME above "
        "`rewrite_listing_partitions` in `crates/sail-data-source/src/listing.rs`)."
    )
)
@pytest.mark.parametrize("provide_schema", [True, False])
def test_parquet_read_multi_level_partitioned_directory(spark, tmp_path, provide_schema):
    # Two-level partition tree: `year=2024/month=11/...`. Partition discovery
    # must walk both segments and surface both columns. Tested both with
    # auto-discovery (no schema) and with an explicit schema that includes
    # the partition columns.
    df_in = spark.createDataFrame(
        [
            (1, "a", 2024, 10),
            (2, "b", 2024, 11),
            (3, "c", 2025, 1),
        ],
        "id INT, val STRING, year INT, month INT",
    )
    src = tmp_path / "src"
    df_in.write.partitionBy("year", "month").parquet(str(src), mode="overwrite")
    for f in src.rglob("*.parquet"):
        f.rename(f.with_suffix(".PARQUET"))
    if provide_schema:
        df = spark.read.schema("id INT, val STRING, year INT, month INT").parquet(str(src))
    else:
        df = spark.read.parquet(str(src))
    rows = sorted(df.collect(), key=lambda r: r.id)
    assert rows == [
        Row(id=1, val="a", year=2024, month=10),
        Row(id=2, val="b", year=2024, month=11),
        Row(id=3, val="c", year=2025, month=1),
    ]


# TODO: remove this test once the FIXME above `rewrite_listing_partitions`
# in `crates/sail-data-source/src/listing.rs` is addressed and the
# Spark-parity test above starts passing.
@pytest.mark.parametrize("provide_schema", [True, False])
def test_parquet_read_multi_level_partitioned_directory_string_only(spark, tmp_path, provide_schema):
    # Pins Sail's current behavior: every partition column comes back as
    # STRING regardless of the underlying values. Once partition type
    # inference matches Spark, this test should be deleted in favor of the
    # `test_parquet_read_multi_level_partitioned_directory` above.
    df_in = spark.createDataFrame(
        [
            (1, "a", 2024, 10),
            (2, "b", 2024, 11),
            (3, "c", 2025, 1),
        ],
        "id INT, val STRING, year INT, month INT",
    )
    src = tmp_path / "src"
    df_in.write.partitionBy("year", "month").parquet(str(src), mode="overwrite")
    for f in src.rglob("*.parquet"):
        f.rename(f.with_suffix(".PARQUET"))
    if provide_schema:
        df = spark.read.schema("id INT, val STRING, year STRING, month STRING").parquet(str(src))
    else:
        df = spark.read.parquet(str(src))
    rows = sorted(df.collect(), key=lambda r: r.id)
    assert rows == [
        Row(id=1, val="a", year="2024", month="10"),
        Row(id=2, val="b", year="2024", month="11"),
        Row(id=3, val="c", year="2025", month="1"),
    ]


@pytest.mark.parametrize("ext", ["parquet", "PARQUET"])
def test_parquet_read_with_schema_column_projection(spark, sample_df, tmp_path, ext):
    # Parquet natively supports column projection. Supplying a schema with a
    # subset of the file's columns must return only those columns (without
    # erroring like CSV does). Verified for both the lowercase and uppercase
    # extension paths.
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    if ext != "parquet":
        for f in src.glob("*.parquet"):
            f.rename(f.with_suffix(f".{ext}"))
    df = spark.read.schema("col1 STRING").parquet(str(src))
    assert df.columns == ["col1"]
    assert df.count() == sample_df.count()
    actual = sorted([r.col1 for r in df.collect()], key=lambda v: (v is not None, v))
    expected = sorted([r.col1 for r in sample_df.collect()], key=lambda v: (v is not None, v))
    assert actual == expected


def test_parquet_read_with_user_supplied_glob(spark, sample_df, tmp_path):
    # When the caller passes their own glob in the URL (e.g. `*.PARQUET`),
    # our default hidden-file glob must not interfere. The user's pattern
    # takes precedence, and the read should match exactly the files the
    # user asked for — including dropping a co-located `.parquet` file.
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    # Half the files become uppercase, half stay lowercase.
    files = sorted(src.glob("*.parquet"))
    for i, f in enumerate(files):
        if i % 2 == 0:
            f.rename(f.with_suffix(".PARQUET"))
    upper_glob = str(src / "*.PARQUET")
    df = spark.read.parquet(upper_glob)
    # Expected = rows from only the renamed (uppercase) files.
    expected_count = sum(spark.read.parquet(str(f)).count() for f in src.glob("*.PARQUET"))
    assert df.count() == expected_count


def test_parquet_read_uppercase_single_file_with_schema(spark, sample_df, tmp_path):
    # A single uppercase-extension parquet file plus an explicit schema.
    # Pinned in addition to the directory variant to make sure single-file
    # URL handling (which goes through `head` instead of `list`) behaves
    # the same way.
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    files = list(src.glob("*.parquet"))
    assert len(files) == 1
    upper = files[0].with_suffix(".PARQUET")
    files[0].rename(upper)
    df = spark.read.schema(sample_df.schema).parquet(str(upper))
    assert df.count() == sample_df.count()
    assert sorted(df.collect(), key=safe_sort_key) == sorted(sample_df.collect(), key=safe_sort_key)


@pytest.mark.parametrize("session_timezone", ["America/Los_Angeles"], indirect=True)
def test_parquet_int96_timestamp_is_instant(spark, tmp_path, session_timezone):
    # Spark reads INT96 as TIMESTAMP, which compares with other TIMESTAMP values as instants.
    _ = session_timezone
    path = tmp_path / "int96.parquet"
    value = datetime(2020, 6, 14, 4, tzinfo=UTC)
    pq.write_table(
        pa.table({"ts": pa.array([value], type=pa.timestamp("us", tz="UTC"))}),
        path,
        use_deprecated_int96_timestamps=True,
        store_schema=False,
    )
    df = spark.read.parquet(str(path))
    assert isinstance(df.schema["ts"].dataType, TimestampType)
    assert df.selectExpr("unix_micros(ts)").collect() == [(1592107200000000,)]
    assert df.filter("ts = TIMESTAMP '2020-06-13 21:00:00'").count() == 1


@pytest.mark.parametrize(
    ("session_timezone", "expected"),
    [("Asia/Shanghai", "2020-06-14 12:00:00"), ("America/Los_Angeles", "2020-06-13 21:00:00")],
    indirect=["session_timezone"],
)
def test_parquet_timestamp_show_uses_session_timezone(spark, tmp_path, session_timezone, expected):
    _ = session_timezone
    path = tmp_path / "instants.parquet"
    pq.write_table(
        pa.table({"ts": pa.array([datetime(2020, 6, 14, 4, tzinfo=UTC)], type=pa.timestamp("us", tz="UTC"))}),
        path,
        store_schema=False,
    )
    shown = spark.read.parquet(str(path))._show_string()  # noqa: SLF001
    assert expected in shown


_INT96_DISPLAY_INSTANTS = [
    datetime(2020, 6, 14, 6, 59, 59, tzinfo=UTC),
    datetime(2020, 6, 14, 7, 0, tzinfo=UTC),
    datetime(2020, 6, 13, 16, 0, tzinfo=UTC),
    datetime(2020, 3, 8, 10, 0, tzinfo=UTC),
    datetime(2020, 11, 1, 8, 30, tzinfo=UTC),
    datetime(2020, 11, 1, 9, 30, tzinfo=UTC),
    None,
]


@pytest.mark.parametrize(
    ("session_timezone", "expected"),
    [
        (
            "America/Los_Angeles",
            [
                (0, "2020-06-13 23:59:59", "2020-06-13", "2020-06-13"),
                (1, "2020-06-14 00:00:00", "2020-06-14", "2020-06-14"),
                (2, "2020-06-13 09:00:00", "2020-06-13", "2020-06-13"),
                (3, "2020-03-08 03:00:00", "2020-03-08", "2020-03-08"),
                (4, "2020-11-01 01:30:00", "2020-11-01", "2020-11-01"),
                (5, "2020-11-01 01:30:00", "2020-11-01", "2020-11-01"),
                (6, None, None, None),
            ],
        ),
        (
            "Asia/Shanghai",
            [
                (0, "2020-06-14 14:59:59", "2020-06-14", "2020-06-14"),
                (1, "2020-06-14 15:00:00", "2020-06-14", "2020-06-14"),
                (2, "2020-06-14 00:00:00", "2020-06-14", "2020-06-14"),
                (3, "2020-03-08 18:00:00", "2020-03-08", "2020-03-08"),
                (4, "2020-11-01 16:30:00", "2020-11-01", "2020-11-01"),
                (5, "2020-11-01 17:30:00", "2020-11-01", "2020-11-01"),
                (6, None, None, None),
            ],
        ),
        (
            "+05:30",
            [
                (0, "2020-06-14 12:29:59", "2020-06-14", "2020-06-14"),
                (1, "2020-06-14 12:30:00", "2020-06-14", "2020-06-14"),
                (2, "2020-06-13 21:30:00", "2020-06-13", "2020-06-13"),
                (3, "2020-03-08 15:30:00", "2020-03-08", "2020-03-08"),
                (4, "2020-11-01 14:00:00", "2020-11-01", "2020-11-01"),
                (5, "2020-11-01 15:00:00", "2020-11-01", "2020-11-01"),
                (6, None, None, None),
            ],
        ),
    ],
    indirect=["session_timezone"],
)
def test_parquet_int96_timestamp_display_uses_session_timezone(spark, tmp_path, session_timezone, expected):
    _ = session_timezone
    path = tmp_path / "int96_display.parquet"
    pq.write_table(
        pa.table(
            {
                "id": list(range(len(_INT96_DISPLAY_INSTANTS))),
                "ts": pa.array(_INT96_DISPLAY_INSTANTS, type=pa.timestamp("us", tz="UTC")),
            }
        ),
        path,
        use_deprecated_int96_timestamps=True,
        store_schema=False,
    )
    df = spark.read.parquet(str(path))
    actual = (
        df.selectExpr(
            "id",
            "CAST(ts AS STRING)",
            "CAST(CAST(ts AS DATE) AS STRING)",
            "CAST(to_date(ts) AS STRING)",
        )
        .orderBy("id")
        .collect()
    )
    assert [tuple(r) for r in actual] == expected
    shown = df.orderBy("id")._show_string(n=100, truncate=False)  # noqa: SLF001
    assert expected[0][1] in shown


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


@pytest.mark.usefixtures("session_timezone")
@pytest.mark.parametrize("session_timezone", ["Asia/Shanghai", "America/Los_Angeles"], indirect=True)
def test_external_parquet_timestamp_instant_roundtrip(spark, tmp_path):
    # Check stored instants separately from the logical-type test's string rendering.
    session_timezone = spark.conf.get("spark.sql.session.timeZone")
    first = datetime(2020, 6, 14, 12, tzinfo=ZoneInfo(session_timezone))
    values = [first + timedelta(minutes=30 * i) for i in range(24)]
    path = tmp_path / "external_instants.parquet"
    pq.write_table(
        pa.table({"ts": pa.array(values, type=pa.timestamp("us", tz=session_timezone))}),
        path,
        store_schema=False,
    )

    source = spark.read.parquet(str(path))
    destination = tmp_path / "rewritten_instants"
    source.write.parquet(str(destination))
    for frame in (source, spark.read.parquet(str(destination))):
        assert isinstance(frame.schema["ts"].dataType, TimestampType)
        row = (
            frame.filter("ts >= TIMESTAMP '2020-06-14 00:00:00' AND ts < TIMESTAMP '2020-06-15 00:00:00'")
            .selectExpr("count(*) AS n", "unix_micros(min(ts)) AS lo", "unix_micros(max(ts)) AS hi")
            .collect()[0]
        )
        assert (row.n, row.lo, row.hi) == (
            24,
            int(values[0].timestamp() * 1_000_000),
            int(values[-1].timestamp() * 1_000_000),
        )


@pytest.mark.usefixtures("session_timezone")
@pytest.mark.parametrize("session_timezone", ["Asia/Shanghai", "America/Los_Angeles"], indirect=True)
@pytest.mark.parametrize("adjusted_to_utc", [False, True])
@pytest.mark.parametrize(
    ("lower", "upper"), [("2020-06-14", "2020-06-15"), ("2020-06-14 00:00:00", "2020-06-15 00:00:00")]
)
def test_external_parquet_timestamp_string_window(spark, tmp_path, adjusted_to_utc, lower, upper):
    # String bounds cast to the column type, so the window is the day in the session time zone.
    session_timezone = spark.conf.get("spark.sql.session.timeZone")
    values = [datetime(2020, 6, 14, 12) + timedelta(minutes=30 * i) for i in range(24)]  # noqa: DTZ001
    if adjusted_to_utc:
        values = [value.replace(tzinfo=ZoneInfo(session_timezone)) for value in values]
    data_type = pa.timestamp("us", tz=session_timezone if adjusted_to_utc else None)
    path = tmp_path / "external.parquet"
    pq.write_table(pa.table({"ts": pa.array(values, type=data_type)}), path, store_schema=False)

    row = (
        spark.read.parquet(str(path))
        .filter(f"ts >= '{lower}' AND ts < '{upper}'")
        .selectExpr("count(*) AS n", "CAST(min(ts) AS STRING) AS lo", "CAST(max(ts) AS STRING) AS hi")
        .collect()[0]
    )
    assert (row.n, row.lo, row.hi) == (24, "2020-06-14 12:00:00", "2020-06-14 23:30:00")


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
@pytest.mark.parametrize("session_timezone", ["UTC"], indirect=True)
def test_parquet_int96_timestamp_window_precision(spark, tmp_path):
    path = tmp_path / "int96.parquet"
    pq.write_table(
        pa.table({"id": range(3), "ts": pa.array([-1000, -1, 0], type=pa.timestamp("ns"))}),
        path,
        use_deprecated_int96_timestamps=True,
    )
    # Spark reads the pre-epoch nanosecond as -1 microsecond. Sail can retain
    # nanoseconds; neither representation may compare equal to the epoch.
    frame = spark.read.option("coerce_int96", "ns").parquet(str(path))
    assert frame.filter("id = 1 AND ts = TIMESTAMP '1970-01-01 00:00:00'").select("id").collect() == []
    assert frame.filter("ts = TIMESTAMP '1970-01-01 00:00:00'").select("id").collect() == [(2,)]


@pytest.mark.parametrize("case_sensitive", [False, True])
def test_parquet_merge_schema_field_case(spark, tmp_path, case_sensitive):
    for filename, name, value in [("a", "id", 1), ("b", "ID", 2)]:
        nested = pa.array([{name: value}], type=pa.struct([(name, pa.int64())]))
        pq.write_table(pa.table({name: [value], "nested": nested}), tmp_path / f"{filename}.parquet")

    key = "spark.sql.caseSensitive"
    previous = spark.conf.get(key)
    try:
        spark.conf.set(key, str(case_sensitive).lower())
        df = spark.read.option("mergeSchema", True).parquet(str(tmp_path))
        if case_sensitive:
            assert df.columns == ["id", "nested", "ID"]
            assert df.orderBy(df["ID"].asc_nulls_first()).selectExpr(
                "id", "ID", "nested.id AS first_id", "nested.ID AS second_id"
            ).collect() == [
                (1, None, 1, None),
                (None, 2, None, 2),
            ]
        else:
            assert df.columns == ["id", "nested"]
            assert df.orderBy(df.id).selectExpr("id", "nested.id AS nested_id").collect() == [(1, 1), (2, 2)]
            assert df.where("id = 2").select("nested.id").collect() == [(2,)]
            assert df.selectExpr("min(id)", "max(id)", "count(id)").collect() == [(1, 2, 2)]
    finally:
        spark.conf.set(key, previous)


def test_parquet_merge_schema_decimal_precision(spark, tmp_path):
    for filename, precision, value in [("a", 5, "11.11"), ("b", 12, "12345678.90")]:
        price = pa.array([Decimal(value)], type=pa.decimal128(precision, 2))
        pq.write_table(pa.table({"price": price}), tmp_path / f"{filename}.parquet")
    df = spark.read.option("mergeSchema", True).parquet(str(tmp_path))
    assert df.schema.simpleString() == "struct<price:decimal(12,2)>"
    assert df.orderBy("price").collect() == [(Decimal("11.11"),), (Decimal("12345678.90"),)]


def test_parquet_merge_schema_interval_qualifiers(spark, tmp_path):
    paths = []
    for i, (day_time, year_month) in enumerate([("HOUR", "MONTH"), ("MINUTE", "YEAR")]):
        path = str(tmp_path / str(i))
        spark.sql(f"SELECT INTERVAL '2' {day_time} AS dt, INTERVAL '1' {year_month} AS ym").write.parquet(path)
        paths.append(path)

    df = spark.read.option("mergeSchema", True).parquet(*paths)
    assert df.schema.simpleString() == "struct<dt:interval hour to minute,ym:interval year to month>"
    assert df.selectExpr("dt", "EXTRACT(YEAR FROM ym) AS yr", "EXTRACT(MONTH FROM ym) AS mon").orderBy(
        "yr"
    ).collect() == [
        (timedelta(hours=2), 0, 1),
        (timedelta(minutes=2), 1, 0),
    ]


@pytest.mark.spark(skip_if_jvm=True, reason="Reads Spark interval qualifiers embedded in Arrow field metadata")
def test_parquet_merge_schema_nested_interval_qualifiers(spark, tmp_path):
    for i, qualifier in enumerate(["hour", "minute"]):
        metadata = {
            "SAIL::spark::interval": f'{{"unit":"dayTime","startField":"{qualifier}","endField":"{qualifier}"}}'
        }
        leaf = pa.field("value", pa.duration("us"), metadata=metadata)
        schema = pa.schema([("s", pa.struct([leaf])), ("a", pa.list_(leaf)), ("m", pa.map_(pa.string(), leaf))])
        value = timedelta(hours=1) if i == 0 else timedelta(minutes=2)
        table = pa.Table.from_pylist([{"s": {"value": value}, "a": [value], "m": {"k": value}}], schema=schema)
        pq.write_table(table, tmp_path / f"{i}.parquet")

    df = spark.read.option("mergeSchema", True).parquet(str(tmp_path))
    assert df.schema.simpleString() == (
        "struct<s:struct<value:interval hour to minute>,a:array<interval hour to minute>,"
        "m:map<string,interval hour to minute>>"
    )
    assert df.selectExpr("s.value", "a[0]", "m['k']").orderBy("value").collect() == [
        (value, value, value) for value in [timedelta(minutes=2), timedelta(hours=1)]
    ]


def test_parquet_merge_schema_statistics_cache_field_case(spark, tmp_path):
    pq.write_table(pa.table({"id": [1]}), tmp_path / "a.parquet")
    pq.write_table(pa.table({"ID": [2]}), tmp_path / "b.parquet")
    key = "spark.sql.caseSensitive"
    previous = spark.conf.get(key)
    try:
        for case_sensitive, expected in [(True, (1, 1)), (False, (2, 2)), (True, (1, 1))]:
            spark.conf.set(key, str(case_sensitive).lower())
            df = spark.read.schema("id BIGINT").option("mergeSchema", True).parquet(str(tmp_path))
            assert df.selectExpr("count(id)", "max(id)").collect() == [expected]
    finally:
        spark.conf.set(key, previous)


@pytest.mark.parametrize("merge_schema", [True, False])
def test_parquet_mixed_arrow_offset_widths(spark, tmp_path, merge_schema):
    for i, (text, binary, list_type) in enumerate(
        [
            (pa.large_string(), pa.large_binary(), lambda item: pa.list_(item, 2)),
            (pa.string(), pa.binary(), pa.list_),
            (pa.large_string(), pa.binary(1), pa.large_list),
        ]
    ):
        label = str(i)
        nested_type = pa.struct([("labels", list_type(text)), ("payload", binary)])
        nested = pa.array([{"labels": [label, None], "payload": label.encode()}], type=nested_type)
        pq.write_table(pa.table({"label": pa.array([label], type=text), "nested": nested}), tmp_path / f"{i}.parquet")

    df = spark.read.option("mergeSchema", merge_schema).parquet(str(tmp_path))
    assert df.schema.simpleString() == "struct<label:string,nested:struct<labels:array<string>,payload:binary>>"
    assert df.orderBy("label").select("label", "nested.labels", "nested.payload").collect() == [
        (str(i), [str(i), None], str(i).encode()) for i in range(3)
    ]


@pytest.mark.usefixtures("session_timezone")
@pytest.mark.parametrize("session_timezone", ["America/Los_Angeles"], indirect=True)
def test_parquet_merge_schema_nested_timestamp_timezones(spark, tmp_path):
    for i, timezone in enumerate(["UTC", "America/New_York"]):
        ty = pa.struct([("ts", pa.timestamp("us", tz=timezone))])
        instant = datetime(2024, 1, 2, 3, 4, i, tzinfo=UTC)
        nested = pa.array([{"ts": instant}], type=ty)
        pq.write_table(pa.table({"nested": nested}), tmp_path / f"{i}.parquet")

    df = spark.read.option("mergeSchema", True).parquet(str(tmp_path))
    assert df.schema.simpleString() == "struct<nested:struct<ts:timestamp>>"
    assert df.selectExpr("cast(nested.ts AS STRING) AS value").orderBy("value").collect() == [
        Row(value="2024-01-01 19:04:00"),
        Row(value="2024-01-01 19:04:01"),
    ]


@pytest.mark.parametrize("case_sensitive", [False, True])
def test_parquet_single_footer_ambiguous_field_projection(spark, tmp_path, case_sensitive):
    pq.write_table(pa.table({"id": [1], "q": [10]}), tmp_path / "a.parquet")
    pq.write_table(pa.table({"id": [2], "q": [20], "Q": [30]}), tmp_path / "b.parquet")
    key = "spark.sql.caseSensitive"
    previous = spark.conf.get(key)
    try:
        spark.conf.set(key, str(case_sensitive).lower())
        df = spark.read.option("mergeSchema", False).parquet(str(tmp_path))
        assert df.columns == ["id", "q"]
        assert df.select("id").orderBy("id").collect() == [(1,), (2,)]
        assert df.count() == 2
        if case_sensitive:
            assert df.select("q").orderBy("q").collect() == [(10,), (20,)]
        else:
            for projection in ["q", "count(q)"]:
                with pytest.raises(
                    Exception,
                    match=r"Multiple physical fields match 'q'|FAILED_READ_FILE\.NO_HINT|Found duplicate field",
                ):
                    df.selectExpr(projection).collect()
    finally:
        spark.conf.set(key, previous)


def test_parquet_single_footer_missing_required_fields(spark, tmp_path):
    required = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("q", pa.int64(), nullable=False)])
    pq.write_table(pa.table({"id": [1], "q": [42]}, schema=required), tmp_path / "a.parquet")
    pq.write_table(pa.table({"id": [2]}), tmp_path / "b.parquet")

    df = spark.read.option("basePath", str(tmp_path)).parquet(str(tmp_path))
    assert df.schema["q"].nullable
    assert df.orderBy("id").collect() == [Row(id=1, q=42), Row(id=2, q=None)]


@pytest.mark.parametrize("use_dictionary", [False, True])
def test_parquet_int32_reads_as_byte_and_short(spark, tmp_path, use_dictionary):
    def write(name, tiny, small, nested, ids):
        # The JVM vectorized reader calls decodeToLong on a UINT32 dictionary
        # when the requested type is SMALLINT. Use plain pages for that column.
        dictionary = ["id", "tiny", "items"] if use_dictionary and small.type == pa.uint32() else use_dictionary
        pq.write_table(
            pa.table({"id": ids, "tiny": tiny, "small": small, "items": nested}),
            tmp_path / name,
            use_dictionary=dictionary,
        )

    write(
        "a.parquet",
        pa.array([1], pa.int8()),
        pa.array([2], pa.int16()),
        pa.array([[3]], pa.list_(pa.int8())),
        [0],
    )
    write(
        "b.parquet",
        pa.array([-129, -128, 128, None], pa.int32()),
        pa.array([-32769, -32768, 32768, None], pa.int32()),
        pa.array([[255, None], [-257], [], None], pa.list_(pa.int16())),
        [1, 2, 3, 4],
    )
    write(
        "c.parquet",
        pa.array([255], pa.uint8()),
        pa.array([2**32 - 1], pa.uint32()),
        pa.array([[65535]], pa.list_(pa.uint16())),
        [5],
    )

    frame = spark.read.option("mergeSchema", False).parquet(str(tmp_path))
    assert frame.schema.simpleString() == "struct<id:bigint,tiny:tinyint,small:smallint,items:array<tinyint>>"
    assert frame.orderBy("id").collect() == [
        (0, 1, 2, [3]),
        (1, 127, 32767, [-1, None]),
        (2, -128, -32768, [-1]),
        (3, -128, -32768, []),
        (4, None, None, None),
        (5, -1, -1, [-1]),
    ]
    assert frame.where("tiny = -128").select("id").orderBy("id").collect() == [(2,), (3,)]
    assert frame.selectExpr("min(tiny)", "max(tiny)", "min(small)", "max(small)", "count(tiny)").collect() == [
        (-128, 127, -32768, 32767, 5)
    ]


def test_parquet_scan_validates_projected_fields_per_file(spark, tmp_path):
    pq.write_table(pa.table({"id": [1], "q": [42]}), tmp_path / "a.parquet")
    pq.write_table(pa.table({"id": [2], "q": ["7"]}), tmp_path / "b.parquet")

    df = spark.read.option("basePath", str(tmp_path)).parquet(str(tmp_path))
    assert df.select("id").orderBy("id").collect() == [Row(id=1), Row(id=2)]
    assert df.count() == 2
    with pytest.raises(Exception, match="PARQUET_COLUMN_DATA_TYPE_MISMATCH"):
        df.select("q").collect()


def test_parquet_explicit_schema_skips_unrequested_merge_conflicts(spark, tmp_path):
    for i, value in enumerate([2, "two"]):
        pq.write_table(pa.table({"id": [i], "other": [value]}), tmp_path / f"{i}.parquet")

    df = spark.read.option("mergeSchema", True).schema("id BIGINT").parquet(str(tmp_path))
    assert df.orderBy("id").collect() == [Row(id=0), Row(id=1)]


def test_parquet_validates_case_insensitive_nested_field(spark, tmp_path):
    pq.write_table(pa.table({"item": [{"x": 42, "value": 7}]}), tmp_path / "data.parquet")

    df = (
        spark.read.option("basePath", str(tmp_path)).schema("item STRUCT<x:BIGINT,VALUE:STRING>").parquet(str(tmp_path))
    )
    assert df.select("item.x").collect() == [Row(x=42)]
    with pytest.raises(Exception, match="PARQUET_COLUMN_DATA_TYPE_MISMATCH"):
        df.select("item.VALUE").collect()
    with pytest.raises(Exception, match="PARQUET_COLUMN_DATA_TYPE_MISMATCH"):
        df.collect()


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("is_map", [False, True])
def test_parquet_projected_collection_struct_field(spark, tmp_path, is_map, nested):
    column = "s.items" if nested else "items"
    element = f"{column}['k']" if is_map else f"{column}[0]"
    for i, bad_type in enumerate([pa.int64(), pa.string()]):
        item_type = pa.struct([("good", pa.int64()), ("bad", bad_type)])
        item = {"good": i, "bad": "two" if i else 1}
        if is_map:
            values = pa.array([[("k", item)]], pa.map_(pa.string(), item_type))
        else:
            values = pa.array([[item]], pa.list_(item_type))
        table = (
            pa.table({"s": pa.StructArray.from_arrays([values], names=["items"])})
            if nested
            else pa.table({"items": values})
        )
        pq.write_table(table, tmp_path / f"{i}.parquet")

    df = spark.read.option("mergeSchema", False).parquet(str(tmp_path))
    assert df.selectExpr(f"{element}.good AS v").orderBy("v").collect() == [(0,), (1,)]
    invalid = [f"{element}.bad", column]
    if not is_map:
        for array in [column, f"reverse({column})", f"shuffle({column})", f"slice({column}, 1, 1)"]:
            assert sorted(df.selectExpr(f"{array}.good").collect()) == [([0],), ([1],)]
        assert sorted(df.selectExpr(f"get({column}, 0).good").collect()) == [(0,), (1,)]
        assert df.selectExpr(f"try_element_at({column}, 2).good").collect() == [(None,), (None,)]
        invalid.append(f"{column}.bad")
    key = "'k'" if is_map else "1"
    assert sorted(df.selectExpr(f"try_element_at({column}, {key}).good").collect()) == [(0,), (1,)]
    for expression in invalid:
        with pytest.raises(Exception, match="PARQUET_COLUMN_DATA_TYPE_MISMATCH"):
            df.selectExpr(expression).collect()


@pytest.mark.parametrize("accessor", ["map_keys", "map_values"])
def test_parquet_projected_map_struct_field(spark, tmp_path, accessor):
    for i, bad_type in enumerate([pa.int64(), pa.string()]):
        struct = pa.struct([("good", pa.int64()), ("bad", bad_type)])
        item = {"good": i, "bad": "two" if i else 1}
        if accessor == "map_keys":
            entries = pa.array([[(item, "v")]], pa.map_(struct, pa.string()))
        else:
            entries = pa.array([[("k", item)]], pa.map_(pa.string(), struct))
        pq.write_table(pa.table({"items": entries}), tmp_path / f"{i}.parquet")

    df = spark.read.option("mergeSchema", False).parquet(str(tmp_path))
    assert sorted(df.selectExpr(f"{accessor}(items).good").collect()) == [([0],), ([1],)]
    with pytest.raises(Exception, match="PARQUET_COLUMN_DATA_TYPE_MISMATCH"):
        df.selectExpr(f"{accessor}(items)").collect()


def test_parquet_projected_chained_collection_access(spark, tmp_path):
    for i, bad_type in enumerate([pa.int64(), pa.string()]):
        leaf = pa.struct([("good", pa.int64()), ("bad", bad_type)])
        group = pa.struct([("members", pa.map_(pa.string(), leaf))])
        value = {"good": i, "bad": "two" if i else 1}
        groups = pa.array([[{"members": [("k", value)]}]], pa.list_(group))
        pq.write_table(pa.table({"groups": groups}), tmp_path / f"{i}.parquet")

    df = spark.read.option("mergeSchema", False).parquet(str(tmp_path))
    assert df.selectExpr("groups[0].members['k'].good AS v").orderBy("v").collect() == [(0,), (1,)]
    assert df.selectExpr("groups[0].members['absent'].good").collect() == [(None,), (None,)]
    with pytest.raises(Exception, match="INVALID_ARRAY_INDEX"):
        df.selectExpr("groups[1].members['k'].good").collect()


def test_parquet_user_case_reads_condition_and_result_fields(spark, tmp_path):
    ty = pa.list_(pa.struct([("v", pa.int64()), ("flag", pa.bool_())]))
    items = pa.array([[{"v": 1, "flag": True}], [{"v": 2, "flag": False}]], ty)
    pq.write_table(pa.table({"items": items}), tmp_path / "items.parquet")
    df = spark.read.parquet(str(tmp_path))
    key = "spark.sql.ansi.enabled"
    previous = spark.conf.get(key)
    try:
        spark.conf.set(key, "false")
        selected = df.selectExpr(
            "(CASE WHEN items[0].flag THEN named_struct('v', -1L, 'flag', true) ELSE items[0] END).v AS v"
        )
        assert selected.orderBy("v").collect() == [(-1,), (2,)]
    finally:
        spark.conf.set(key, previous)


def test_parquet_merge_dictionary_with_plain_string(spark, tmp_path):
    pq.write_table(pa.table({"color": pa.array(["red"]).dictionary_encode()}), tmp_path / "a.parquet")
    pq.write_table(pa.table({"color": ["blue"]}), tmp_path / "b.parquet")

    df = spark.read.option("mergeSchema", True).parquet(str(tmp_path))
    assert df.schema.simpleString() == "struct<color:string>"
    assert df.orderBy("color").collect() == [Row(color="blue"), Row(color="red")]


def test_parquet_disjoint_struct_children(spark, tmp_path):
    pq.write_table(pa.table({"id": [1], "item": [{"x": 4}]}), tmp_path / "a.parquet")
    pq.write_table(pa.table({"id": [2, 3], "item": [{"y": 5}, None]}), tmp_path / "b.parquet")
    key = "spark.sql.caseSensitive"
    previous = spark.conf.get(key)
    try:
        spark.conf.set(key, "true")
        df = spark.read.option("mergeSchema", False).parquet(str(tmp_path))
        assert [r.item for r in df.orderBy("id").collect()] == [Row(x=4), Row(x=None), None]
    finally:
        spark.conf.set(key, previous)


def test_parquet_reads_nested_unannotated_int64_with_seconds_hint(spark, tmp_path):
    path = tmp_path / "data.parquet"

    def schema(leaf):
        return pa.schema(
            [("item", pa.struct([("x", leaf), ("arr", pa.list_(leaf)), ("m", pa.map_(pa.string(), leaf))]))]
        )

    table = pa.Table.from_pylist([{"item": {"x": 7, "arr": [-1], "m": {"k": 42}}}], schema=schema(pa.int64()))
    with pq.ParquetWriter(path, table.schema, store_schema=False) as writer:
        writer.add_key_value_metadata(
            {"ARROW:schema": base64.b64encode(schema(pa.timestamp("s")).serialize()).decode()}
        )
        writer.write_table(table)

    df = spark.read.parquet(str(path))
    assert df.schema.simpleString() == "struct<item:struct<x:bigint,arr:array<bigint>,m:map<string,bigint>>>"
    assert df.collect() == [Row(item=Row(x=7, arr=[-1], m={"k": 42}))]


def test_parquet_merge_schema_decimal_widths(spark, tmp_path):
    for index, make_type in enumerate([pa.decimal32, pa.decimal64, pa.decimal128, pa.decimal256]):
        amount = pa.array([Decimal(f"{index}.12")], type=make_type(8, 2))
        pq.write_table(pa.table({"amount": amount}), tmp_path / f"{index}.parquet")

    df = spark.read.option("mergeSchema", True).parquet(str(tmp_path))
    assert df.schema.simpleString() == "struct<amount:decimal(8,2)>"
    assert [row.amount for row in df.orderBy("amount").collect()] == [Decimal(f"{i}.12") for i in range(4)]


def test_parquet_merge_schema_unsigned_integers(spark, tmp_path):
    maxima = [2**width - 1 for width in (8, 16, 32, 64)]
    unsigned = [pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64()]
    widened = [pa.int16(), pa.int32(), pa.int64(), pa.decimal128(20, 0)]
    names = ["u8", "u16", "u32", "u64"]

    def write(name, types, values):
        columns = {key: pa.array([value], type=ty) for key, value, ty in zip(names, values, types, strict=True)}
        columns["nested"] = pa.StructArray.from_arrays([columns["u32"]], names=["value"])
        pq.write_table(pa.table(columns), tmp_path / name)

    write("a.parquet", unsigned, maxima)
    write("b.parquet", widened, [-1, -1, -1, Decimal(-1)])
    df = spark.read.option("mergeSchema", True).parquet(str(tmp_path))
    assert (
        df.schema.simpleString()
        == "struct<u8:smallint,u16:int,u32:bigint,u64:decimal(20,0),nested:struct<value:bigint>>"
    )
    assert df.orderBy("u32").select(*names, "nested.value").collect() == [
        (-1, -1, -1, Decimal(-1), -1),
        (*maxima[:3], Decimal(maxima[3]), maxima[2]),
    ]
    assert df.selectExpr("max(u32)", "max(u64)").collect() == [(maxima[2], Decimal(maxima[3]))]


def test_parquet_merge_schema_binary_as_string(spark, tmp_path):
    for index, value in enumerate([b"first", "second"]):
        pq.write_table(
            pa.table({"value": [value], "nested": [{"value": value}]}),
            tmp_path / f"{index}.parquet",
            store_schema=False,
        )

    key = "spark.sql.parquet.binaryAsString"
    previous = spark.conf.get(key)
    try:
        spark.conf.set(key, "true")
        df = spark.read.option("mergeSchema", True).parquet(str(tmp_path))
        assert df.schema.simpleString() == "struct<value:string,nested:struct<value:string>>"
        assert df.selectExpr("value", "nested.value AS nested_value").orderBy("value").collect() == [
            ("first", "first"),
            ("second", "second"),
        ]
    finally:
        spark.conf.set(key, previous)


@pytest.mark.parametrize(("ty", "precision"), [(pa.uint32(), 20), (pa.uint64(), 21)])
def test_parquet_unsigned_explicit_decimal(spark, tmp_path, ty, precision):
    pq.write_table(pa.table({"value": pa.array([1], type=ty)}), tmp_path / "data.parquet")
    df = spark.read.schema(f"value DECIMAL({precision}, 0)").parquet(str(tmp_path))
    with pytest.raises(Exception, match="PARQUET_COLUMN_DATA_TYPE_MISMATCH"):
        df.collect()


def test_parquet_single_footer_reads_unsigned_storage(spark, tmp_path):
    pq.write_table(
        pa.table({"i": pa.array([1], pa.int32()), "d": [1.0], "l": pa.array([1], pa.int64())}), tmp_path / "a.parquet"
    )
    u32 = pa.array([2**32 - 1, 42], pa.uint32())
    u64 = pa.array([2**64 - 1, 42], pa.uint64())
    pq.write_table(pa.table({"i": u32, "d": u32, "l": u64}), tmp_path / "b.parquet", use_dictionary=False)
    df = spark.read.option("mergeSchema", False).parquet(str(tmp_path))
    assert df.orderBy("i").collect() == [(-1, -1.0, -1), (1, 1.0, 1), (42, 42.0, 42)]
    assert df.selectExpr("min(i)", "min(d)", "min(l)").collect() == [(-1, -1.0, -1)]


@pytest.mark.parametrize("binary_as_string", [False, True])
def test_parquet_requested_byte_array_type(spark, tmp_path, binary_as_string):
    pq.write_table(pa.table({"value": ["café"]}), tmp_path / "a.parquet", store_schema=False)
    pq.write_table(pa.table({"value": [b"row"]}), tmp_path / "b.parquet", store_schema=False)
    key = "spark.sql.parquet.binaryAsString"
    previous = spark.conf.get(key)
    try:
        spark.conf.set(key, str(binary_as_string).lower())
        reader = spark.read.option("mergeSchema", False)
        assert sorted(row.value for row in reader.parquet(str(tmp_path)).collect()) == ["café", "row"]
        binary_rows = reader.schema("value BINARY").parquet(str(tmp_path)).collect()
        assert sorted(bytes(row.value) for row in binary_rows) == ["café".encode(), b"row"]
    finally:
        spark.conf.set(key, previous)


def test_parquet_fixed_binary_rejects_string_schema(spark, tmp_path):
    path = tmp_path / "fixed.parquet"
    pq.write_table(pa.table({"value": pa.array([b"abc"], type=pa.binary(3))}), path)
    with pytest.raises(Exception, match="PARQUET_COLUMN_DATA_TYPE_MISMATCH"):
        spark.read.schema("value STRING").parquet(str(path)).collect()


@pytest.mark.spark(skip_if_jvm=True, reason="Checks Sail preservation of Arrow UUID metadata")
@pytest.mark.parametrize("skip_metadata", [False, True])
@pytest.mark.parametrize("store_schema", [False, True])
def test_parquet_uuid_storage_round_trip(spark, tmp_path, skip_metadata, store_schema):
    value = pa.array([bytes(range(16))], type=pa.uuid())
    nested = pa.StructArray.from_arrays([value], names=["u"])
    source = tmp_path / "in.parquet"
    output = tmp_path / "out"
    pq.write_table(pa.table({"v": value, "s": nested}), source, store_schema=store_schema)

    spark.read.option("skipMetadata", skip_metadata).parquet(str(source)).write.parquet(str(output))
    parquet = pq.ParquetFile(next(output.glob("*.parquet")))
    assert parquet.schema.column(1).logical_type.type == "UUID"
    restored = parquet.read()
    assert restored["s"].combine_chunks().field("u").equals(value)
    if not skip_metadata:
        assert parquet.schema.column(0).logical_type.type == "UUID"
        assert restored["v"].combine_chunks().equals(value)


def test_parquet_single_footer_preserves_epoch_across_timestamp_families(spark, tmp_path):
    for i, zone in enumerate(["Asia/Shanghai", None]):
        values = pa.array([i], type=pa.timestamp("us", tz=zone))
        pq.write_table(pa.table({"id": [i], "v": values}), tmp_path / f"{i}.parquet")

    df = spark.read.option("mergeSchema", False).parquet(str(tmp_path))
    assert df.selectExpr("id", "unix_micros(v)").orderBy("id").collect() == [(0, 0), (1, 1)]
    assert df.where("unix_micros(v) = 1").select("id").collect() == [(1,)]
    assert df.selectExpr("unix_micros(min(v))", "unix_micros(max(v))").collect() == [(0, 1)]


@pytest.mark.parametrize("merge_schema", [False, True])
def test_parquet_reads_legacy_map_child_names(spark, tmp_path, merge_schema):
    for i in range(2):
        values = pa.array([[("x", i)]], type=pa.map_(pa.string(), pa.int64()))
        path = tmp_path / f"{i}.parquet"
        pq.write_table(pa.table({"id": [i], "m": values}), path, store_schema=False)
        if i == 1:
            # PyArrow emits standard map names. Rename the length-prefixed footer
            # strings to same-length legacy names, keeping the footer length and offsets.
            data = path.read_bytes()
            path.write_bytes(data.replace(b"\x03key", b"\x03abc").replace(b"\x05value", b"\x05entry"))

    df = spark.read.option("mergeSchema", merge_schema).parquet(str(tmp_path))
    assert df.schema.simpleString() == "struct<id:bigint,m:map<string,bigint>>"
    assert df.orderBy("id").collect() == [(0, {"x": 0}), (1, {"x": 1})]
