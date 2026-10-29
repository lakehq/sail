from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

pytestmark = pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.scalar requires PySpark 4+")


@pytest.fixture
def cast_key_scan(spark, tmp_path):
    previous = {key: spark.conf.get(key) for key in ("spark.sql.ansi.enabled", "spark.sql.session.timeZone")}
    spark.conf.set("spark.sql.ansi.enabled", "true")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    def scan(name, table):
        path = tmp_path / f"{name}.parquet"
        pq.write_table(table, path)
        return spark.read.parquet(str(path))

    try:
        yield scan
    finally:
        for key, value in previous.items():
            spark.conf.set(key, value)


def _cast_key_counts(inner, outer, target):
    inner, outer = inner.alias("i"), outer.alias("o")
    key = F.col("i.k") if target is None else F.col("i.k").cast(target)
    matching = inner.where(key == F.col("o.key").outer())
    result = outer.select("oid", matching.select(F.count("*")).scalar().alias("n")).orderBy("oid")
    assert result.schema.simpleString() == "struct<oid:bigint,n:bigint>"
    return result


def _outer_table(keys):
    return pa.table({"oid": pa.array(range(len(keys)), type=pa.int64()), "key": keys})


@pytest.mark.parametrize("source", ["date", "ntz"])
def test_utc_correlated_cast_keeps_nulls_and_subsecond_values(cast_key_scan, source):
    day = 86_400_000_000
    keys = pa.array([-day, 0, day, None], type=pa.timestamp("us", tz="UTC"))
    values = (
        pa.array([-1, 0, 0, 1, None], type=pa.date32())
        if source == "date"
        else pa.array([-day, -1, 0, 0, 1, day, None], type=pa.timestamp("us"))
    )
    inner = cast_key_scan("inner", pa.table({"k": values}))
    outer = cast_key_scan("outer", _outer_table(keys))
    target = "timestamp" if source == "date" else None
    assert [tuple(row) for row in _cast_key_counts(inner, outer, target).collect()] == [(0, 1), (1, 2), (2, 1), (3, 0)]


@pytest.mark.parametrize("scale", [0, 2])
def test_correlated_string_decimal_cast_rounds_and_rejects_invalid_rows(cast_key_scan, scale):
    values = ["0", "+0", "-0", "00", "1.4", "1.5", "-1.4", "-1.5", None]
    target = f"decimal(19,{scale})"
    keys = [-2, -1, 0, 1, 2, 3, None] if scale == 0 else [-1.5, -1.4, 0, 1.4, 1.5, 3, None]
    keys = pa.array([None if key is None else Decimal(str(key)) for key in keys], type=pa.decimal128(19, scale))
    inner = cast_key_scan("inner", pa.table({"k": pa.array(values, type=pa.string())}))
    outer = cast_key_scan("outer", _outer_table(keys))
    counts = [1, 1, 4, 1, 1, 0, 0]
    assert [tuple(row) for row in _cast_key_counts(inner, outer, target).collect()] == list(enumerate(counts))
    invalid = cast_key_scan("invalid", pa.table({"k": ["0", "invalid"]}))
    with pytest.raises(Exception, match=r"(?i)(cast|decimal|invalid)"):
        _cast_key_counts(invalid, outer, target).collect()


def test_correlated_decimal_bigint_cast_preserves_boundary_values(cast_key_scan):
    low, high = -(1 << 63), (1 << 63) - 1
    values = pa.array(
        [Decimal(x) if x is not None else None for x in [low, -1, 0, 0, 1, high, None]],
        type=pa.decimal128(19, 0),
    )
    inner = cast_key_scan("inner", pa.table({"k": values}))
    outer = cast_key_scan("outer", _outer_table(pa.array([low, -1, 0, 1, high, 2, None], type=pa.int64())))
    expected = list(enumerate([1, 1, 2, 1, 1, 0, 0]))
    assert [tuple(row) for row in _cast_key_counts(inner, outer, "bigint").collect()] == expected
    invalid = cast_key_scan("invalid", pa.table({"k": pa.array([Decimal(high + 1)], type=pa.decimal128(19, 0))}))
    with pytest.raises(Exception, match=r"(?i)(cast|range|overflow)"):
        _cast_key_counts(invalid, outer, "bigint").collect()


@pytest.mark.parametrize("unit", ["us", "ns"])
@pytest.mark.parametrize("zone", [None, "UTC"])
def test_correlated_timestamp_date_cast_floors_the_full_input_range(cast_key_scan, unit, zone):
    if unit == "ns" and is_jvm_spark():
        pytest.skip("Spark Parquet does not support nanosecond timestamp logical types")
    day = 86_400_000_000 if unit == "us" else 86_400_000_000_000
    values = [-(1 << 63), -day - 1, -day, -1, 0, day - 1, day, (1 << 63) - 1, None]
    days = sorted({value // day for value in values if value is not None}) + [None]  # noqa: RUF005
    inner = cast_key_scan("inner", pa.table({"k": pa.array(values, type=pa.timestamp(unit, tz=zone))}))
    outer = cast_key_scan("outer", _outer_table(pa.array(days, type=pa.date32())))
    expected = [
        sum(value is not None and value // day == key for value in values) if key is not None else 0 for key in days
    ]
    assert [tuple(row) for row in _cast_key_counts(inner, outer, "date").collect()] == list(enumerate(expected))


def test_correlated_string_double_cast_preserves_collisions_and_rejects_invalid_rows(cast_key_scan):
    values = ["0", "-0", "+0.0", "-1e-999", "1.25", " 1.25\t", "1.25e0", "9007199254740992", "9007199254740993", None]
    inner = cast_key_scan("inner", pa.table({"k": pa.array(values, type=pa.string())}))
    keys = pa.array([-0.0, 0.0, 1.25, 9007199254740992.0, 2.0, None], type=pa.float64())
    outer = cast_key_scan("outer", _outer_table(keys))
    expected = list(enumerate([4, 4, 3, 2, 0, 0]))
    assert [tuple(row) for row in _cast_key_counts(inner, outer, "double").collect()] == expected
    invalid = cast_key_scan("invalid", pa.table({"k": ["0", "not-double"]}))
    with pytest.raises(Exception, match=r"(?i)(cast|double|invalid)"):
        _cast_key_counts(invalid, outer, "double").collect()
