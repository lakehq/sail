# SPDX-License-Identifier: Apache-2.0

import base64
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


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
