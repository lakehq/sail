"""UUID is a Sail SQL extension; Spark rejects UUID DDL."""

# ruff: noqa: S608

import json
from pathlib import Path
from uuid import UUID

import pyarrow.parquet as pq
import pytest
from pyiceberg import types as iceberg_types
from pyiceberg.schema import Schema
from pyiceberg.table import StaticTable
from pyiceberg.transforms import BucketTransform
from pyiceberg.types import FixedType, LongType, NestedField, UUIDType
from pyspark.sql.types import ArrayType, BinaryType, MapType, StringType, StructField, StructType

from pysail.testing._dev.spark.steps.iceberg import _find_latest_metadata
from pysail.testing.spark.utils.sql import escape_sql_string_literal

VALUE = "6f1c2d3e-4b5a-4c6d-8e7f-0123456789ab"
VALUE_BYTES = UUID(VALUE).bytes
UUID_BYTE_LENGTH = 16


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
            assert column.length == UUID_BYTE_LENGTH
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
        if entry == "sql":
            with pytest.raises(Exception, match=r"(?i)invalid uuid"):
                spark.sql(f"INSERT INTO {table} VALUES (2, '{invalid}')").collect()
        else:
            with pytest.raises(Exception, match=r"(?i)invalid uuid"):
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
    with pytest.raises(Exception, match=r"(?i)invalid uuid"):
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
    expected_row_count = 3
    assert len(rows) == expected_row_count
    assert sum(tuple(row) == (None, None, None, None) for row in rows) == 1
    values = [row for row in rows if row.s is not None]
    expected_nonnull_count = 2
    assert len(values) == expected_nonnull_count
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


@pytest.mark.parametrize(
    "operation", ["ADD COLUMN u UUID COMMENT 'added'", "REPLACE COLUMNS (id BIGINT, u UUID COMMENT 'added')"]
)
def test_uuid_column_evolution_preserves_identity(spark, tmp_path, operation):
    table = "uuid_column_evolution"
    location = escape_sql_string_literal(str(tmp_path))
    try:
        spark.sql(f"CREATE TABLE {table} (id BIGINT) USING iceberg LOCATION '{location}'")
        before = _find_latest_metadata(tmp_path)
        spark.sql(f"ALTER TABLE {table} {operation}")
        evolved = _current_schema(_find_latest_metadata(tmp_path))
        fields = evolved["fields"]
        assert evolved["schema-id"] > before["current-schema-id"]
        assert [(f["name"], f["type"]) for f in fields] == [("id", "long"), ("u", "uuid")]
        assert fields[1]["id"] > before["last-column-id"]
        assert fields[1]["doc"] == "added"
        if operation.startswith("ADD"):
            assert fields[0]["id"] == _current_schema(before)["fields"][0]["id"]
        else:
            assert fields[0]["id"] > before["last-column-id"]
        assert len({f["id"] for f in fields}) == len(fields)
        spark.sql(f"INSERT INTO {table} VALUES (1, '{VALUE}')")
        spark.sql(f"ALTER TABLE {table} RENAME COLUMN u TO renamed")
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN renamed COMMENT 'updated'")
        after = _current_schema(_find_latest_metadata(tmp_path))
        assert after["schema-id"] > evolved["schema-id"]
        assert after["fields"] == [fields[0], {**fields[1], "name": "renamed", "doc": "updated"}]
        spark.sql(f"INSERT INTO {table} VALUES (2, '{VALUE.upper()}')")
        rows = spark.read.format("iceberg").load(str(tmp_path)).orderBy("id").collect()
        assert [(row.id, bytes(row.renamed)) for row in rows] == [(1, VALUE_BYTES), (2, VALUE_BYTES)]
        for parquet in _data_files(tmp_path):
            column = parquet.schema.column(1)
            assert column.physical_type == "FIXED_LEN_BYTE_ARRAY"
            assert column.length == UUID_BYTE_LENGTH
            assert column.logical_type.type == "UUID"
            assert parquet.schema_arrow.field(1).metadata[b"PARQUET:field_id"] == str(fields[1]["id"]).encode()
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("container", ["struct", "array", "map"])
def test_uuid_add_nested_leaf_preserves_identity(spark, tmp_path, container):
    table = "uuid_nested_evolution"
    location = escape_sql_string_literal(str(tmp_path))
    data_type, path = "STRUCT<n: INT>", "s.u"
    expression = f"named_struct('n', 10, 'u', '{VALUE}')"
    if container == "array":
        data_type, path = f"ARRAY<{data_type}>", "s.element.u"
        expression = f"array({expression})"
    elif container == "map":
        data_type, path = f"MAP<STRING, {data_type}>", "s.value.u"
        expression = f"map('k', {expression})"

    def struct_type(schema):
        ty = schema["fields"][1]["type"]
        return ty["element"] if container == "array" else ty["value"] if container == "map" else ty

    try:
        spark.sql(f"CREATE TABLE {table} (id BIGINT, s {data_type}) USING iceberg LOCATION '{location}'")
        before = _find_latest_metadata(tmp_path)
        spark.sql(f"ALTER TABLE {table} ADD COLUMN {path} UUID COMMENT 'added'")
        evolved = _current_schema(_find_latest_metadata(tmp_path))
        assert evolved["schema-id"] > before["current-schema-id"]
        assert [f["id"] for f in evolved["fields"]] == [f["id"] for f in _current_schema(before)["fields"]]
        children = struct_type(evolved)["fields"]
        assert children[0] == struct_type(_current_schema(before))["fields"][0]
        leaf = children[1]
        assert leaf["type"] == "uuid"
        assert leaf["doc"] == "added"
        assert leaf["id"] > before["last-column-id"]
        spark.sql(f"INSERT INTO {table} SELECT 1, {expression}")
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {path} COMMENT 'updated'")
        after = _current_schema(_find_latest_metadata(tmp_path))
        assert after["schema-id"] > evolved["schema-id"]
        assert struct_type(after)["fields"] == [children[0], {**leaf, "doc": "updated"}]
        spark.sql(f"INSERT INTO {table} SELECT 2, {expression}")
        rows = spark.read.format("iceberg").load(str(tmp_path)).orderBy("id").collect()
        assert [row.id for row in rows] == [1, 2]
        for row in rows:
            value = row.s[0] if container == "array" else row.s["k"] if container == "map" else row.s
            assert bytes(value.u) == VALUE_BYTES
        for parquet in _data_files(tmp_path):
            column = parquet.schema.column(len(parquet.schema) - 1)
            assert column.physical_type == "FIXED_LEN_BYTE_ARRAY"
            assert column.length == UUID_BYTE_LENGTH
            assert column.logical_type.type == "UUID"
            field = parquet.schema_arrow.field("s")
            if container == "array":
                field = field.type.value_field
            elif container == "map":
                field = field.type.item_field
            assert field.type.field("u").metadata[b"PARQUET:field_id"] == str(leaf["id"]).encode()
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("target", ["STRING", "INT", "UUID"])
@pytest.mark.parametrize("nested", [False, True])
def test_uuid_try_cast_infallible_map_keys(spark, target, nested):
    frame = spark.createDataFrame([(VALUE, "bad")], "k STRING, v STRING").repartition(2)
    typed = frame.selectExpr("CAST(map(k, v) AS MAP<UUID, STRING>) AS m")
    expression, data_type, projection = "m", f"MAP<UUID, {target}>", "m"
    if nested:
        expression, data_type, projection = (
            f"array(named_struct('m', {expression}))",
            f"ARRAY<STRUCT<m: {data_type}>>",
            "m[0].m",
        )
    converted = typed.selectExpr(f"TRY_CAST({expression} AS {data_type}) AS m").selectExpr(f"{projection} AS m")
    row = converted.selectExpr("hex(map_keys(m)[0]) AS k", "map_values(m)[0] AS v").first()
    assert row.k == VALUE_BYTES.hex().upper()
    assert row.v == ("bad" if target == "STRING" else None)


@pytest.mark.parametrize(("expression", "expected"), [("NULL", None), ("map()", 0)])
def test_uuid_try_cast_typed_null_and_empty_map_keys(spark, expression, expected):
    row = spark.sql(f"SELECT size(TRY_CAST(CAST({expression} AS MAP<UUID, STRING>) AS MAP<UUID, STRING>)) AS n").first()
    assert row.n == expected


@pytest.mark.parametrize("key_type", ["STRING", "BINARY"])
def test_uuid_try_cast_fallible_map_keys_rejected(spark, key_type):
    frame = spark.createDataFrame([(VALUE,)], "k STRING").repartition(2)
    with pytest.raises(Exception, match="null UUID map keys"):
        frame.selectExpr(f"TRY_CAST(map(CAST(k AS {key_type}), 'v') AS MAP<UUID, STRING>)").collect()


@pytest.mark.parametrize("cast", ["CAST", "TRY_CAST"])
@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        (f"array('{VALUE}')", [VALUE_BYTES]),
        (f"array('{VALUE}', NULL)", [VALUE_BYTES, None]),
        ("array()", []),
        ("NULL", None),
    ],
)
def test_uuid_constant_array_cast(spark, cast, expression, expected):
    value = spark.sql(f"SELECT {cast}({expression} AS ARRAY<UUID>) AS a").first().a
    assert (None if value is None else [None if v is None else bytes(v) for v in value]) == expected


def test_uuid_constant_array_invalid_cast(spark):
    assert spark.sql("SELECT TRY_CAST(array('bad') AS ARRAY<UUID>) AS a").first().a == [None]
    with pytest.raises(Exception, match=r"(?i)invalid uuid"):
        spark.sql("SELECT CAST(array('bad') AS ARRAY<UUID>)").collect()


@pytest.mark.parametrize("container", ["struct", "array", "map"])
def test_uuid_constant_nested_array_cast(spark, container):
    expression, target, projection = f"named_struct('a', array('{VALUE}'))", "STRUCT<a: ARRAY<UUID>>", "v.a[0]"
    if container == "array":
        expression, target, projection = f"array(array('{VALUE}'))", "ARRAY<ARRAY<UUID>>", "v[0][0]"
    elif container == "map":
        expression, target, projection = f"map('k', array('{VALUE}'))", "MAP<STRING, ARRAY<UUID>>", "v['k'][0]"
    row = spark.sql(f"SELECT CAST({expression} AS {target}) AS v").selectExpr(f"hex({projection}) AS u").first()
    assert row.u == VALUE_BYTES.hex().upper()


@pytest.mark.parametrize("constant", [False, True])
@pytest.mark.parametrize("container", ["map", "array"])
@pytest.mark.parametrize("route", ["index", "element_at", "try_element_at"])
def test_uuid_nested_collection_lookup(spark, constant, container, route):
    values = [[VALUE, None], [], None, [None]]
    inputs = [{"k": value} if container == "map" else [value] for value in values]
    target = "MAP<STRING, ARRAY<UUID>>" if container == "map" else "ARRAY<ARRAY<UUID>>"
    source = target.replace("UUID", "STRING")
    key = "'k'" if container == "map" else "1"
    expression = f"v[{'0' if container == 'array' else key}]" if route == "index" else f"{route}(v, {key})"
    expected_schema = StructType([StructField("u", ArrayType(BinaryType(), True), True)])
    if constant:
        frames = []
        for value in values:
            inner = (
                "NULL"
                if value is None
                else "array(" + ",".join("NULL" if item is None else f"'{item}'" for item in value) + ")"
            )
            outer = f"map('k', {inner})" if container == "map" else f"array({inner})"
            frames.append(spark.sql(f"SELECT CAST({outer} AS {target}) AS v"))
    else:
        frames = [
            spark.createDataFrame([(value,) for value in inputs], f"v {source}")
            .repartition(2)
            .selectExpr(f"CAST(v AS {target}) AS v")
        ]
    actual = []
    for frame in frames:
        selected = frame.selectExpr(f"{expression} AS u")
        assert selected.schema == expected_schema
        actual.extend(
            None if row.u is None else [None if v is None else bytes(v) for v in row.u] for row in selected.collect()
        )
    expected = [[VALUE_BYTES, None], [], None, [None]]
    assert sorted(actual, key=repr) == sorted(expected, key=repr)


def test_uuid_lookup_missing_and_invalid_index(spark):
    maps = spark.createDataFrame(
        [({}, "k"), (None, "k"), ({"k": [VALUE]}, "missing"), ({"k": [VALUE]}, None)],
        "v MAP<STRING, ARRAY<STRING>>, k STRING",
    )
    maps = maps.repartition(2).selectExpr("CAST(v AS MAP<STRING, ARRAY<UUID>>) AS v", "k")
    for expression in ["v[k]", "element_at(v,k)", "try_element_at(v,k)"]:
        assert [row.u for row in maps.selectExpr(f"{expression} AS u").collect()] == [None] * 4
    frame = spark.sql(f"SELECT CAST(array(array('{VALUE}')) AS ARRAY<ARRAY<UUID>>) AS v")
    assert frame.selectExpr("try_element_at(v,2) AS u").first().u is None
    assert bytes(frame.selectExpr("element_at(v,-1)[0] AS u").first().u) == VALUE_BYTES
    for expression in ["v[1]", "element_at(v,2)"]:
        with pytest.raises(Exception, match=r"(?i)(out of bounds|invalid_array_index)"):
            frame.selectExpr(expression).collect()
    with pytest.raises(Exception, match=r"(?i)(out of bounds|invalid_array_index)"):
        frame.select(frame.v[-1]).collect()
    for expression in ["element_at(v,0)", "try_element_at(v,0)"]:
        with pytest.raises(Exception, match=r"(?i)index 0 is invalid"):
            frame.selectExpr(expression).collect()


def test_uuid_constant_array_insert(spark, tmp_path):
    table = "uuid_constant_arrays"
    location = escape_sql_string_literal(str(tmp_path))
    try:
        spark.sql(f"CREATE TABLE {table} (id INT, a ARRAY<UUID>) USING iceberg LOCATION '{location}'")
        spark.sql(f"INSERT INTO {table} SELECT 1, array('{VALUE}')")
        spark.sql(f"INSERT INTO {table} SELECT 2, CAST(array('{VALUE}', NULL) AS ARRAY<UUID>)")
        spark.sql(f"INSERT INTO {table} SELECT 3, TRY_CAST(array('bad') AS ARRAY<UUID>)")
        spark.sql(f"INSERT INTO {table} VALUES (4, NULL), (5, array())")
        before = _find_latest_metadata(tmp_path)
        with pytest.raises(Exception, match=r"(?i)invalid uuid"):
            spark.sql(f"INSERT INTO {table} SELECT 6, array('bad')").collect()
        assert _find_latest_metadata(tmp_path) == before
        rows = spark.read.format("iceberg").load(str(tmp_path)).orderBy("id").collect()
        assert [
            (row.id, None if row.a is None else [None if v is None else bytes(v) for v in row.a]) for row in rows
        ] == [(1, [VALUE_BYTES]), (2, [VALUE_BYTES, None]), (3, [None]), (4, None), (5, [])]
        assert _current_schema(before)["fields"][1]["type"]["element"] == "uuid"
        for parquet in _data_files(tmp_path):
            assert parquet.schema.column(1).logical_type.type == "UUID"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("container", ["scalar", "struct", "array", "map"])
@pytest.mark.parametrize("source_uuid", [False, True])
def test_uuid_alter_type_preserves_target_identity(spark, sql_catalog, container, source_uuid):
    # Iceberg Java UpdateSchema.updateColumn accepts UUID identity, not fixed[16] -> UUID.
    identifier, name = "default.uuid_alter_type", "uuid_alter_type"
    leaf = UUIDType() if source_uuid else FixedType(UUID_BYTE_LENGTH)
    data_type, column, projection = leaf, "payload", "payload"
    value = f"'{VALUE}'" if source_uuid else f"unhex('{VALUE_BYTES.hex()}')"
    expression = value
    if container == "struct":
        data_type = iceberg_types.StructType(NestedField(3, "u", leaf, required=False, doc="leaf doc"))
        column, projection, expression = "payload.u", "payload.u", f"named_struct('u', {value})"
    elif container == "array":
        data_type = iceberg_types.ListType(element_id=3, element_type=leaf, element_required=False)
        column, projection, expression = "payload.element", "payload[0]", f"array({value})"
    elif container == "map":
        data_type = iceberg_types.MapType(
            key_id=3, key_type=iceberg_types.StringType(), value_id=4, value_type=leaf, value_required=False
        )
        column, projection, expression = "payload.value", "payload['k']", f"map('k', {value})"
    table = sql_catalog.create_table(
        identifier,
        Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "payload", data_type, required=False, doc="retained doc"),
        ),
    )
    location = Path(table.location().removeprefix("file://"))
    try:
        escaped = escape_sql_string_literal(str(location))
        spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{escaped}'")
        spark.sql(f"INSERT INTO {name} SELECT 1, {expression}")
        before = _find_latest_metadata(location)
        if source_uuid:
            spark.sql(f"ALTER TABLE {name} ALTER COLUMN {column} TYPE UUID")
            after = _find_latest_metadata(location)
            assert _current_schema(after)["fields"] == _current_schema(before)["fields"]
            assert after.get("snapshots") == before.get("snapshots")
            assert after.get("current-snapshot-id") == before.get("current-snapshot-id")
            spark.sql(f"INSERT INTO {name} SELECT 2, {expression}")
        else:
            with pytest.raises(Exception, match=r"Cannot change Iceberg column .* from fixed\(16\) to uuid"):
                spark.sql(f"ALTER TABLE {name} ALTER COLUMN {column} TYPE UUID").collect()
            assert _find_latest_metadata(location) == before
        rows = (
            spark.read.format("iceberg")
            .load(str(location))
            .selectExpr("id", f"hex({projection}) AS u")
            .orderBy("id")
            .collect()
        )
        assert [(row.id, row.u) for row in rows] == [
            (i, VALUE_BYTES.hex().upper()) for i in ([1, 2] if source_uuid else [1])
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        sql_catalog.drop_table(identifier)


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
        with pytest.raises(Exception, match=r"(?i)Column 'f' has type"):
            spark.createDataFrame([(2, VALUE, VALUE)], "id LONG, u STRING, f STRING").write.format("iceberg").mode(
                "append"
            ).save(table.location())
    finally:
        sql_catalog.drop_table(identifier)
