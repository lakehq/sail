# SPDX-License-Identifier: Apache-2.0

import base64
from datetime import timedelta
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import types


def test_parquet_spark_footer_intervals(spark, tmp_path):
    # JVM Spark writes intervals as ordinary INT64 microseconds and INT32 months.
    hour = types.DayTimeIntervalType.HOUR
    logical = (
        types.StructType()
        .add("elapsed", types.DayTimeIntervalType(hour, hour))
        .add("period", types.YearMonthIntervalType())
    )
    physical = pa.table({"elapsed": pa.array([7_200_000_000], pa.int64()), "period": pa.array([14], pa.int32())})
    metadata = {b"org.apache.spark.sql.parquet.row.metadata": logical.json().encode()}
    with pq.ParquetWriter(tmp_path / "a.parquet", physical.schema, store_schema=False) as writer:
        writer.add_key_value_metadata(metadata)
        writer.write_table(physical)

    reader = spark.read.option("mergeSchema", True)
    df = reader.parquet(str(tmp_path))
    assert df.schema == logical
    assert df.selectExpr("elapsed", "EXTRACT(YEAR FROM period)", "EXTRACT(MONTH FROM period)").collect() == [
        (timedelta(hours=2), 1, 2)
    ]

    pq.write_table(physical, tmp_path / "b.parquet")
    with pytest.raises(Exception, match=r"CANNOT_MERGE|Cannot merge"):
        _ = reader.parquet(str(tmp_path)).schema


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("other", ["same", "storage", "mllib"])
def test_parquet_merge_vector_udt(spark, tmp_path, nested, other):
    from pyspark.ml.linalg import VectorUDT
    from pyspark.mllib.linalg import VectorUDT as MllibVectorUDT
    from pyspark.sql.pandas.types import to_arrow_schema

    def schema(data_type, marker):
        if nested:
            data_type = types.StructType().add("vector", data_type)
        return types.StructType().add("v", data_type).add(marker, types.IntegerType())

    vector = VectorUDT()
    storage = vector.sqlType()
    right = {"same": vector, "storage": storage, "mllib": MllibVectorUDT()}[other]
    for name, value_type in [("a", vector), ("b", right)]:
        physical = to_arrow_schema(schema(storage, name))
        with pq.ParquetWriter(tmp_path / f"{name}.parquet", physical, store_schema=False) as writer:
            writer.add_key_value_metadata(
                {"org.apache.spark.sql.parquet.row.metadata": schema(value_type, name).json()}
            )
            writer.write_table(pa.Table.from_batches([], schema=physical))

    reader = spark.read.option("mergeSchema", True)
    if other == "same":
        assert reader.parquet(str(tmp_path)).schema == schema(vector, "a").add("b", types.IntegerType())
    else:
        with pytest.raises(Exception, match=r"CANNOT_MERGE|Cannot merge"):
            _ = reader.parquet(str(tmp_path)).schema


@pytest.mark.parametrize("binary_as_string", [False, True])
def test_parquet_annotated_byte_arrays(spark, tmp_path, binary_as_string):
    # parquet-mr ExampleParquetWriter. ENUM and BSON have distinct logical types
    # even though both use BYTE_ARRAY storage, like the unannotated raw column.
    path = tmp_path / "annotations.parquet"
    path.write_bytes(
        base64.b64decode(
            "UEFSMRUAFRoVGhXbscWTChwVAhUAFQYVCAAAAgAAAAMBAwAAAHJlZBUAFSAVIBXS+rKPDRwVAhUAFQYVCAAAAwAAAAMCAAUAAAAFAAAAABUA"
            "FRwVHBXXyM3GDRwVAhUAFQYVCAAAAwAAAAMCAAMAAABhYmMZHBYIFUgWAAAZFgYAGRwWUBVOFgAAGRYKABkcFp4BFUoWAAAZFgYAFQIZXEgB"
            "bRUEABUMJQIYCGNhdGVnb3J5JQhMTAAAADUCGAFzFQQAFQwlAhgDZG9jJShM3AAAABUMJQIYA3JhdwAWAhkcGTwmABwVDBk1AAYIGRgIY2F0"
            "ZWdvcnkVABYCFkgWSCYISRwVABUAFQIAPBYGAAAW6AEVGgAmABwVDBk1AAYIGSgBcwNkb2MVABYCFk4WTiZQSRwVABUAFQIAPBYKKTYAAAIA"
            "ABaCAhUaACYAHBUMGTUABggZKAFzA3JhdxUAFgIWShZKJp4BSRwVABUAFQIAPBYGKTYAAAIAABacAhUcABbgARYCJggW4AEUAAAZHBgRd3Jp"
            "dGVyLm1vZGVsLm5hbWUYB2V4YW1wbGUAGEpwYXJxdWV0LW1yIHZlcnNpb24gMS4xNy4wIChidWlsZCBmYWMwYzc0NjUzMmUxMzNiZWI5Mjhh"
            "N2Y2YTdlNTdiNTEwYjQ3N2ExKRk8HAAAHAAAHAAAAGkBAABQQVIx"
        )
    )
    key = "spark.sql.parquet.binaryAsString"
    previous = spark.conf.get(key)
    try:
        spark.conf.set(key, str(binary_as_string).lower())
        df = spark.read.parquet(str(path))
        raw_type = "string" if binary_as_string else "binary"
        assert df.schema.simpleString() == f"struct<category:string,s:struct<doc:binary,raw:{raw_type}>>"
        assert df.select("category", "s.doc", "s.raw").collect() == [
            ("red", b"\x05\x00\x00\x00\x00", "abc" if binary_as_string else b"abc")
        ]
    finally:
        spark.conf.set(key, previous)


@pytest.mark.parametrize("large", [False, True])
def test_parquet_list_view_with_following_decimal(spark, tmp_path, large):
    path = tmp_path / "values.parquet"
    element = pa.struct([("a", pa.int32()), ("b", pa.int32())])
    decimal = pa.decimal128(6, 2)
    table = pa.table(
        {"items": pa.array([[{"a": 1, "b": 2}]], pa.list_(element)), "amount": pa.array([Decimal("12.34")], decimal)}
    )
    # The file stores a Parquet LIST. Arrow metadata chooses its in-memory list layout.
    view = pa.large_list_view(element) if large else pa.list_view(element)
    hint = pa.schema([("items", view), ("amount", decimal)])
    with pq.ParquetWriter(path, table.schema, store_schema=False, store_decimal_as_integer=True) as writer:
        writer.add_key_value_metadata({"ARROW:schema": base64.b64encode(hint.serialize().to_pybytes())})
        writer.write_table(table)

    inferred = spark.read.parquet(str(path))
    assert inferred.schema.simpleString() == "struct<items:array<struct<a:int,b:int>>,amount:decimal(6,2)>"
    assert inferred.selectExpr("items[0].b", "amount").collect() == [(2, Decimal("12.34"))]
    raw = spark.read.schema("amount INT").parquet(str(path))
    assert raw.collect() == [(1234,)]
