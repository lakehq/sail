"""UUID is a Sail SQL extension; Spark rejects UUID DDL.
"""

import json
from pathlib import Path
from uuid import UUID

import pyarrow.parquet as pq
import pytest
from pyiceberg.schema import Schema
from pyiceberg.table import StaticTable
from pyiceberg.transforms import BucketTransform
from pyiceberg.types import FixedType, LongType, NestedField, UUIDType
from pyspark.sql.types import ArrayType, BinaryType, MapType, StringType, StructField, StructType

from pysail.testing._dev.spark.steps.iceberg import _find_latest_metadata
from pysail.testing.spark.utils.sql import escape_sql_string_literal

VALUE = "6f1c2d3e-4b5a-4c6d-8e7f-0123456789ab"
VALUE_BYTES = UUID(VALUE).bytes


def _current_schema(metadata):
    return next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])


def _data_files(location):
    paths = list(Path(location).rglob("*.parquet"))
    assert paths
    return [pq.ParquetFile(path) for path in paths]


@pytest.mark.parametrize("partitioned", [False, True])
def test_uuid_sql_insert_cast_and_artifacts(spark, tmp_path, partitioned):
    table = "uuid_sql_types"
    location = escape_sql_string_literal(str(tmp_path))
    partition = " PARTITIONED BY (bucket(16, x))" if partitioned else ""
    try:
        spark.sql(f"CREATE TABLE {table} (id BIGINT, x UUID) USING iceberg{partition} LOCATION '{location}'")
        # The benchmark's INSERT SELECT must convert string values, not their UTF-8 bytes.
        spark.sql(f"INSERT INTO {table} SELECT CAST(1 AS BIGINT), '{VALUE}'")
        spark.sql(f"INSERT INTO {table} SELECT CAST(2 AS BIGINT), CAST('{VALUE.upper()}' AS UUID)")
        spark.sql(f"INSERT INTO {table} SELECT CAST(3 AS BIGINT), CAST(NULL AS UUID)")
        spark.createDataFrame([(4, VALUE.upper())], "id LONG, x STRING").repartition(2).write.format("iceberg").mode(
            "append"
        ).save(str(tmp_path))
        frame = spark.table(table).orderBy("id")
        assert isinstance(frame.schema["x"].dataType, BinaryType)
        assert [(r.id, bytes(r.x) if r.x is not None else None) for r in frame.collect()] == [
            (1, VALUE_BYTES),
            (2, VALUE_BYTES),
            (3, None),
            (4, VALUE_BYTES),
        ]
        metadata = _find_latest_metadata(tmp_path)
        fields = _current_schema(metadata)["fields"]
        assert [(f["name"], f["type"]) for f in fields] == [("id", "long"), ("x", "uuid")]
        assert len({f["id"] for f in fields}) == len(fields)
        for parquet in _data_files(tmp_path):
            column = parquet.schema.column(1)
            assert column.physical_type == "FIXED_LEN_BYTE_ARRAY"
            assert column.length == 16
            assert column.logical_type.type == "UUID"
            assert parquet.schema_arrow.field("x").metadata[b"PARQUET:field_id"] == str(fields[1]["id"]).encode()
        if partitioned:
            metadata_file = next(
                p for p in (tmp_path / "metadata").glob("*.metadata.json") if json.loads(p.read_text()) == metadata
            )
            reference = StaticTable.from_metadata(str(metadata_file))
            bucket = BucketTransform(16).transform(UUIDType())(UUID(VALUE))
            for task in reference.scan().plan_files():
                rows = pq.ParquetFile(task.file.file_path).read().to_pylist()
                expected = {bucket if row["x"] is not None else None for row in rows}
                assert expected == {task.file.partition[0]}
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("entry", ["sql", "dataframe"])
@pytest.mark.parametrize("invalid", ["bad", "1234567890123456", "6f1c2d3e-4b5a-4c6d-8e7f-0123456789ag"])
def test_uuid_invalid_write_keeps_snapshot(spark, tmp_path, entry, invalid):
    table = "uuid_invalid_values"
    location = escape_sql_string_literal(str(tmp_path))
    try:
        spark.sql(f"CREATE TABLE {table} (id BIGINT, x UUID) USING iceberg LOCATION '{location}'")
        spark.sql(f"INSERT INTO {table} VALUES (1, '{VALUE}')")
        before = _find_latest_metadata(tmp_path)
        with pytest.raises(Exception, match="(?i)invalid uuid"):
            if entry == "sql":
                spark.sql(f"INSERT INTO {table} VALUES (2, '{invalid}')").collect()
            else:
                spark.createDataFrame([(2, invalid)], "id LONG, x STRING").write.format("iceberg").mode("append").save(
                    str(tmp_path)
                )
        assert _find_latest_metadata(tmp_path) == before
        assert [(r.id, bytes(r.x)) for r in spark.table(table).collect()] == [(1, VALUE_BYTES)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_uuid_cast_ctas_preserves_type_identity(spark, tmp_path):
    table = "uuid_cast_identity"
    location = escape_sql_string_literal(str(tmp_path))
    try:
        spark.sql(f"CREATE TABLE {table} USING iceberg LOCATION '{location}' AS SELECT CAST('{VALUE}' AS UUID) AS x")
        fields = _current_schema(_find_latest_metadata(tmp_path))["fields"]
        assert fields[0]["type"] == "uuid"
        assert bytes(spark.table(table).first().x) == VALUE_BYTES
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_uuid_cast_nonconstant_and_try_cast(spark):
    frame = spark.createDataFrame([(VALUE.upper(),), (None,), ("bad",)], "x STRING").repartition(2)
    rows = frame.selectExpr("x", "TRY_CAST(x AS UUID) AS u").collect()
    assert {r.x: bytes(r.u) if r.u is not None else None for r in rows} == {
        VALUE.upper(): VALUE_BYTES,
        None: None,
        "bad": None,
    }
    with pytest.raises(Exception, match="(?i)invalid uuid"):
        frame.selectExpr("CAST(x AS UUID)").collect()


def test_uuid_nested_dataframe_write(spark, tmp_path):
    table = "uuid_nested_values"
    location = escape_sql_string_literal(str(tmp_path))
    try:
        spark.sql(
            f"CREATE TABLE {table} (id BIGINT, s STRUCT<u: UUID>, a ARRAY<UUID>, m MAP<STRING, UUID>) "
            f"USING iceberg LOCATION '{location}'"
        )
        frame = spark.createDataFrame(
            [(1, (VALUE.upper(),), [VALUE, None], {"a": VALUE, "b": None}), (2, None, None, None)],
            "id LONG, s STRUCT<u: STRING>, a ARRAY<STRING>, m MAP<STRING, STRING>",
        )
        frame.repartition(2).write.format("iceberg").mode("append").save(str(tmp_path))
        rows = spark.read.format("iceberg").load(str(tmp_path)).orderBy("id").collect()
        assert bytes(rows[0].s.u) == VALUE_BYTES
        assert [bytes(v) if v is not None else None for v in rows[0].a] == [VALUE_BYTES, None]
        assert {k: bytes(v) if v is not None else None for k, v in rows[0].m.items()} == {"a": VALUE_BYTES, "b": None}
        assert tuple(rows[1]) == (2, None, None, None)
        fields = _current_schema(_find_latest_metadata(tmp_path))["fields"]
        assert fields[1]["type"]["fields"][0]["type"] == "uuid"
        assert fields[2]["type"]["element"] == "uuid"
        assert fields[3]["type"]["value"] == "uuid"
        for parquet in _data_files(tmp_path):
            assert [parquet.schema.column(i).logical_type.type for i in (1, 2, 4)] == ["UUID"] * 3
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("cast", ["CAST", "TRY_CAST"])
@pytest.mark.parametrize("container", ["struct", "array", "map"])
def test_uuid_struct_cast_uses_positions(spark, cast, container):
    # Spark Cast.castStruct zips source/target fields; UUID is a Sail-only DDL type.
    second = "00000000-0000-0000-0000-000000000001"
    frame = spark.createDataFrame([(VALUE, second)], "a STRING, b STRING").repartition(2)
    expression = "named_struct('a', a, 'b', b)"
    target = "STRUCT<b: UUID, a: UUID>"
    if container == "array":
        expression, target = f"array({expression})", f"ARRAY<{target}>"
    elif container == "map":
        expression, target = f"map('k', {expression})", f"MAP<STRING, {target}>"
    result = frame.selectExpr(f"{cast}({expression} AS {target}) AS value").first().value
    if container == "array":
        result = result[0]
    elif container == "map":
        result = result["k"]
    assert result.__fields__ == ["b", "a"]
    assert (bytes(result.b), bytes(result.a)) == (VALUE_BYTES, UUID(second).bytes)


def test_uuid_try_cast_keeps_ordinary_leaf_conversion_safe(spark):
    frame = spark.createDataFrame([(VALUE, "bad")], "a STRING, b STRING").repartition(2)
    value = frame.selectExpr("TRY_CAST(named_struct('a', a, 'b', b) AS STRUCT<b: UUID, a: INT>) AS value").first().value
    assert bytes(value.b) == VALUE_BYTES
    assert value.a is None


def test_uuid_try_cast_nonnullable_nested_values(spark):
    leaf = StructType([StructField("u", StringType(), nullable=False)])
    schema = StructType(
        [
            StructField("s", leaf),
            StructField("a", ArrayType(StringType(), containsNull=False)),
            StructField("m", MapType(StringType(), StringType(), valueContainsNull=False)),
            StructField("nested", ArrayType(MapType(StringType(), leaf, valueContainsNull=False), containsNull=False)),
        ]
    )
    frame = spark.createDataFrame(
        [
            (("bad",), ["bad"], {"k": "bad"}, [{"k": ("bad",)}]),
            ((VALUE,), [VALUE], {"k": VALUE}, [{"k": (VALUE,)}]),
            (None, None, None, None),
        ],
        schema,
    ).repartition(2)
    converted = frame.selectExpr(
        "TRY_CAST(s AS STRUCT<u: UUID>) AS s",
        "TRY_CAST(a AS ARRAY<UUID>) AS a",
        "TRY_CAST(m AS MAP<STRING, UUID>) AS m",
        "TRY_CAST(nested AS ARRAY<MAP<STRING, STRUCT<u: UUID>>>) AS nested",
    )
    assert converted.schema["s"].dataType["u"].nullable
    assert converted.schema["a"].dataType.containsNull
    assert converted.schema["m"].dataType.valueContainsNull
    assert converted.schema["nested"].dataType.elementType.valueType["u"].nullable
    rows = converted.collect()
    assert len(rows) == 3
    assert sum(tuple(row) == (None, None, None, None) for row in rows) == 1
    values = [row for row in rows if row.s is not None]
    assert len(values) == 2
    for row in values:
        expected = bytes(row.s.u) if row.s.u is not None else None
        assert expected in (None, VALUE_BYTES)
        assert [bytes(v) if v is not None else None for v in row.a] == [expected]
        assert {k: bytes(v) if v is not None else None for k, v in row.m.items()} == {"k": expected}
        nested = row.nested[0]["k"].u
        assert (bytes(nested) if nested is not None else None) == expected
    assert sum(row.s.u is None for row in values) == 1


@pytest.mark.parametrize("name", ["X", "a.b"])
@pytest.mark.parametrize("binary", [False, True])
def test_uuid_dataframe_write_literal_column_names(spark, tmp_path, name, binary):
    table = "uuid_literal_names"
    location = escape_sql_string_literal(str(tmp_path))
    try:
        spark.sql(f"CREATE TABLE {table} (`{name}` UUID) USING iceberg LOCATION '{location}'")
        schema = StructType([StructField(name, BinaryType() if binary else StringType())])
        spark.createDataFrame([(VALUE_BYTES if binary else VALUE,), (None,)], schema).repartition(2).write.format(
            "iceberg"
        ).mode("append").save(str(tmp_path))
        frame = spark.read.format("iceberg").load(str(tmp_path))
        assert frame.columns == [name]
        assert {bytes(row[0]) if row[0] is not None else None for row in frame.collect()} == {VALUE_BYTES, None}
        fields = _current_schema(_find_latest_metadata(tmp_path))["fields"]
        assert [(field["name"], field["type"]) for field in fields] == [(name, "uuid")]
        for parquet in _data_files(tmp_path):
            assert parquet.schema_arrow.names == [name]
            assert parquet.schema.column(0).logical_type.type == "UUID"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_uuid_and_fixed16_keep_distinct_identity(spark, sql_catalog):
    identifier = "default.uuid_and_fixed16"
    table = sql_catalog.create_table(
        identifier,
        Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "u", UUIDType(), required=False),
            NestedField(3, "f", FixedType(16), required=False),
        ),
    )
    try:
        frame = spark.createDataFrame([(1, VALUE.upper(), VALUE_BYTES)], "id LONG, u STRING, f BINARY")
        frame.write.format("iceberg").mode("append").save(table.location())
        row = spark.read.format("iceberg").load(table.location()).first()
        assert bytes(row.u) == bytes(row.f) == VALUE_BYTES
        location = Path(table.location().removeprefix("file://"))
        fields = _current_schema(_find_latest_metadata(location))["fields"]
        assert [(f["id"], f["type"]) for f in fields] == [(1, "long"), (2, "uuid"), (3, "fixed[16]")]
        for parquet in _data_files(location):
            assert parquet.schema.column(1).logical_type.type == "UUID"
            assert parquet.schema.column(2).logical_type.type == "NONE"
        with pytest.raises(Exception, match="(?i)Column 'f' has type"):
            spark.createDataFrame([(2, VALUE, VALUE)], "id LONG, u STRING, f STRING").write.format("iceberg").mode(
                "append"
            ).save(table.location())
    finally:
        sql_catalog.drop_table(identifier)
