"""Constructor type identity and UUID cast names, including persisted output."""

from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg import types as iceberg_types
from pyiceberg.schema import Schema
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing._dev.spark.steps.iceberg import _find_latest_metadata
from pysail.testing.spark.utils.sql import escape_sql_string_literal

VALUE = "6f1c2d3e-4b5a-4c6d-8e7f-0123456789ab"
SECOND = "00000000-0000-0000-0000-000000000001"
VALUE_BYTES = UUID(VALUE).bytes
SECOND_BYTES = UUID(SECOND).bytes
UUID_BYTE_LENGTH = 16


def _to_arrow(frame):
    if hasattr(type(frame), "toArrow"):
        return frame.toArrow()
    # Spark Connect 3.5 collect() uses this Arrow transport before converting rows.
    # Keep the Arrow map/UUID representation rather than round-tripping through Rows.
    query = frame._plan.to_proto(frame.sparkSession.client)  # noqa: SLF001
    table, _ = frame.sparkSession.client.to_table(query)
    return table


def _normalize(value):
    if isinstance(value, UUID):
        return value.bytes
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, dict):
        return {key: _normalize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_normalize(item) for item in value)
    return value


def _shape(data_type):
    if isinstance(data_type, str):
        return data_type
    if data_type["type"] == "list":
        return ("list", _shape(data_type["element"]))
    if data_type["type"] == "map":
        return ("map", _shape(data_type["key"]), _shape(data_type["value"]))
    return ("struct", tuple((field["name"], _shape(field["type"])) for field in data_type["fields"]))


def _leaf_types(shape):
    if isinstance(shape, str):
        return [shape]
    if shape[0] == "struct":
        return [leaf for _, child in shape[1] for leaf in _leaf_types(child)]
    return [leaf for child in shape[1:] for leaf in _leaf_types(child)]


def _field_ids(value):
    if isinstance(value, dict):
        return [
            field_id
            for key, item in value.items()
            for field_id in ([item] if key in {"id", "element-id", "key-id", "value-id"} else _field_ids(item))
        ]
    if isinstance(value, list):
        return [field_id for item in value for field_id in _field_ids(item)]
    return []


def _arrow_field_ids(field):
    ids = [int(field.metadata[b"PARQUET:field_id"])]
    data_type = field.type
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        ids.extend(_arrow_field_ids(data_type.value_field))
    elif pa.types.is_map(data_type):
        ids.extend(_arrow_field_ids(data_type.key_field))
        ids.extend(_arrow_field_ids(data_type.item_field))
    elif pa.types.is_struct(data_type):
        ids.extend(field_id for child in data_type for field_id in _arrow_field_ids(child))
    return ids


def _assert_artifacts(location, expected_types, expected_rows):
    metadata = _find_latest_metadata(location)
    schema = next(schema for schema in metadata["schemas"] if schema["schema-id"] == metadata["current-schema-id"])
    assert {field["name"]: _shape(field["type"]) for field in schema["fields"]} == expected_types
    ids = _field_ids(schema["fields"])
    assert len(ids) == len(set(ids))
    assert all(field_id > 0 for field_id in ids)
    leaves = [leaf for shape in expected_types.values() for leaf in _leaf_types(shape)]
    paths = list(Path(location).rglob("*.parquet"))
    assert paths
    rows = []
    for path in paths:
        parquet = pq.ParquetFile(path)
        assert parquet.schema_arrow.names == list(expected_types)
        parquet_ids = [field_id for field in parquet.schema_arrow for field_id in _arrow_field_ids(field)]
        assert sorted(parquet_ids) == sorted(ids)
        assert len(parquet.schema) == len(leaves)
        for index, leaf in enumerate(leaves):
            column = parquet.schema.column(index)
            if leaf in {"uuid", "fixed[16]"}:
                assert column.physical_type == "FIXED_LEN_BYTE_ARRAY"
                assert column.length == UUID_BYTE_LENGTH
                assert column.logical_type.type == ("UUID" if leaf == "uuid" else "NONE")
            else:
                assert leaf in {"binary", "string"}
                assert column.physical_type == "BYTE_ARRAY"
                assert column.logical_type.type == ("NONE" if leaf == "binary" else "STRING")
        rows.extend(parquet.read().to_pylist())
    assert sorted(_normalize(rows), key=repr) == sorted(expected_rows, key=repr)


def _persist(spark, frame, table, location, entry):
    if entry == "sql":
        frame.createOrReplaceTempView("uuid_constructor_result")
        escaped = escape_sql_string_literal(str(location))
        spark.sql(f"CREATE TABLE {table} USING iceberg LOCATION '{escaped}' AS SELECT * FROM uuid_constructor_result")  # noqa: S608
    else:
        frame.write.format("iceberg").option("path", str(location)).saveAsTable(table)


@pytest.mark.parametrize("entry", ["sql", "dataframe"])
def test_uuid_constant_constructor_ctas(spark, tmp_path, entry):
    table = "uuid_constant_constructor"
    if entry == "sql":
        u = f"CAST('{VALUE}' AS UUID)"
        frame = spark.sql(
            f"SELECT array({u}, NULL) AS a, map('k', {u}) AS m, map({u}, 'v') AS k, "
            f"map_from_arrays(array('k'), CAST(array('{VALUE}') AS ARRAY<UUID>)) AS typed"
        )
    else:
        u = F.lit(VALUE).cast("UUID")
        frame = spark.range(1).select(
            F.array(u, F.lit(None)).alias("a"),
            F.create_map(F.lit("k"), u).alias("m"),
            F.create_map(u, F.lit("v")).alias("k"),
            F.map_from_arrays(F.array(F.lit("k")), F.array(F.lit(VALUE)).cast("ARRAY<UUID>")).alias("typed"),
        )
    expected = [
        {"a": [VALUE_BYTES, None], "m": [("k", VALUE_BYTES)], "k": [(VALUE_BYTES, "v")], "typed": [("k", VALUE_BYTES)]}
    ]
    types = {
        "a": ("list", "uuid"),
        "m": ("map", "string", "uuid"),
        "k": ("map", "uuid", "string"),
        "typed": ("map", "string", "uuid"),
    }
    try:
        assert _normalize(_to_arrow(frame).to_pylist()) == expected
        _persist(spark, frame, table, tmp_path, entry)
        _assert_artifacts(tmp_path, types, expected)
        assert _normalize(_to_arrow(spark.table(table)).to_pylist()) == expected
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        spark.sql("DROP VIEW IF EXISTS uuid_constructor_result")


@pytest.mark.parametrize("entry", ["sql", "dataframe"])
def test_uuid_scanned_constructor_consensus_ctas(spark, sql_catalog, tmp_path, entry):
    identifier, table = "default.uuid_constructor_source", "uuid_scanned_constructor"
    location = tmp_path / "result"
    source = sql_catalog.create_table(
        identifier,
        Schema(
            iceberg_types.NestedField(1, "u", iceberg_types.UUIDType(), required=False),
            iceberg_types.NestedField(2, "u2", iceberg_types.UUIDType(), required=False),
            iceberg_types.NestedField(3, "f", iceberg_types.FixedType(UUID_BYTE_LENGTH), required=False),
            iceberg_types.NestedField(4, "nf", iceberg_types.FixedType(UUID_BYTE_LENGTH), required=False),
            iceberg_types.NestedField(
                5,
                "a",
                iceberg_types.ListType(element_id=6, element_type=iceberg_types.UUIDType(), element_required=False),
                required=False,
            ),
        ),
    )
    try:
        spark.createDataFrame(
            [(VALUE, SECOND, VALUE_BYTES, None, [VALUE, None]), (SECOND, VALUE, SECOND_BYTES, None, [SECOND])],
            "u STRING, u2 STRING, f BINARY, nf BINARY, a ARRAY<STRING>",
        ).repartition(2).write.format("iceberg").mode("append").save(source.location())
        scanned = spark.read.format("iceberg").load(source.location()).repartition(2)
        expressions = {
            "same": F.array("u", "u2"),
            "null_right": F.array(F.col("u"), F.lit(None)),
            "null_left": F.array(F.lit(None), F.col("u")),
            "fixed": F.array("u", "f"),
            "fixed_reverse": F.array("f", "u"),
            "typed_null": F.array("u", "nf"),
            "typed_null_reverse": F.array("nf", "u"),
            "binary": F.array(F.col("u"), F.col("f").cast("BINARY")),
            "binary_reverse": F.array(F.col("f").cast("BINARY"), F.col("u")),
            "nested": F.array(F.col("a"), F.array("f")),
            "nested_reverse": F.array(F.array("f"), F.col("a")),
            "map_value": F.create_map(F.lit("k"), F.col("u")),
            "map_key": F.create_map(F.col("u"), F.lit("v")),
            "typed_value": F.map_from_arrays(F.array(F.lit("k")), F.array("u").cast("ARRAY<UUID>")),
            "typed_key": F.map_from_arrays(F.array("u").cast("ARRAY<UUID>"), F.array(F.lit("v"))),
            "entries": F.map_from_entries(
                F.array(F.struct(F.lit("k").alias("key"), F.col("u").alias("value"))).cast(
                    "ARRAY<STRUCT<key:STRING,value:UUID>>"
                )
            ),
            "mixed_map_value": F.create_map(F.lit("u"), F.col("u"), F.lit("f"), F.col("f")),
            "mixed_map_key": F.create_map(F.col("u2"), F.lit("u"), F.col("f"), F.lit("f")),
        }
        if entry == "sql":
            scanned.createOrReplaceTempView("uuid_constructor_source")
            sql_expressions = [
                "array(u,u2)",
                "array(u,NULL)",
                "array(NULL,u)",
                "array(u,f)",
                "array(f,u)",
                "array(u,nf)",
                "array(nf,u)",
                "array(u,CAST(f AS BINARY))",
                "array(CAST(f AS BINARY),u)",
                "array(a,array(f))",
                "array(array(f),a)",
                "map('k',u)",
                "map(u,'v')",
                "map_from_arrays(array('k'),CAST(array(u) AS ARRAY<UUID>))",
                "map_from_arrays(CAST(array(u) AS ARRAY<UUID>),array('v'))",
                "map_from_entries(CAST(array(named_struct('key','k','value',u)) AS ARRAY<STRUCT<key:STRING,value:UUID>>))",
                "map('u',u,'f',f)",
                "map(u2,'u',f,'f')",
            ]
            selection = ", ".join(f"{expr} AS {name}" for name, expr in zip(expressions, sql_expressions, strict=True))
            frame = spark.sql(f"SELECT {selection} FROM uuid_constructor_source")  # noqa: S608
        else:
            frame = scanned.select(*(expr.alias(name) for name, expr in expressions.items()))
        expected = []
        for u, u2, a in [(VALUE_BYTES, SECOND_BYTES, [VALUE_BYTES, None]), (SECOND_BYTES, VALUE_BYTES, [SECOND_BYTES])]:
            expected.append(
                {
                    "same": [u, u2],
                    "null_right": [u, None],
                    "null_left": [None, u],
                    "fixed": [u, u],
                    "fixed_reverse": [u, u],
                    "typed_null": [u, None],
                    "typed_null_reverse": [None, u],
                    "binary": [u, u],
                    "binary_reverse": [u, u],
                    "nested": [a, [u]],
                    "nested_reverse": [[u], a],
                    "map_value": [("k", u)],
                    "map_key": [(u, "v")],
                    "typed_value": [("k", u)],
                    "typed_key": [(u, "v")],
                    "entries": [("k", u)],
                    "mixed_map_value": [("u", u), ("f", u)],
                    "mixed_map_key": [(u2, "u"), (u, "f")],
                }
            )
        types = {
            "same": ("list", "uuid"),
            "null_right": ("list", "uuid"),
            "null_left": ("list", "uuid"),
            "fixed": ("list", "fixed[16]"),
            "fixed_reverse": ("list", "fixed[16]"),
            "typed_null": ("list", "fixed[16]"),
            "typed_null_reverse": ("list", "fixed[16]"),
            "binary": ("list", "binary"),
            "binary_reverse": ("list", "binary"),
            "nested": ("list", ("list", "fixed[16]")),
            "nested_reverse": ("list", ("list", "fixed[16]")),
            "map_value": ("map", "string", "uuid"),
            "map_key": ("map", "uuid", "string"),
            "typed_value": ("map", "string", "uuid"),
            "typed_key": ("map", "uuid", "string"),
            "entries": ("map", "string", "uuid"),
            "mixed_map_value": ("map", "string", "fixed[16]"),
            "mixed_map_key": ("map", "fixed[16]", "string"),
        }
        assert sorted(_normalize(_to_arrow(frame).to_pylist()), key=repr) == sorted(expected, key=repr)
        _persist(spark, frame, table, location, entry)
        _assert_artifacts(location, types, expected)
        assert sorted(_normalize(_to_arrow(spark.table(table)).to_pylist()), key=repr) == sorted(expected, key=repr)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        spark.sql("DROP VIEW IF EXISTS uuid_constructor_source")
        spark.sql("DROP VIEW IF EXISTS uuid_constructor_result")
        sql_catalog.drop_table(identifier)


@pytest.mark.parametrize("entry", ["sql", "dataframe"])
@pytest.mark.parametrize("cast", ["CAST", "TRY_CAST"])
@pytest.mark.parametrize("target", ["UUID", "ARRAY<UUID>", "MAP<STRING,UUID>", "STRUCT<U:UUID>"])
def test_uuid_null_cast_ctas_names(spark, tmp_path, entry, cast, target):
    # Spark 4.1.3 names complex NULL casts NULL. Full complex cast names are
    # existing Sail behavior; only scalar naming is a Spark parity assertion.
    name = f"{cast}(NULL AS {target})"
    if entry == "sql":
        frame = spark.sql(f"SELECT {name}")
    else:
        value = F.lit(None)
        if cast == "CAST":
            value = value.cast(target)
        elif hasattr(type(value), "try_cast"):
            value = value.try_cast(target)
        else:
            value = F.expr(f"TRY_CAST(NULL AS {target})")
        frame = spark.range(1).select(value)
    types = {
        "UUID": "uuid",
        "ARRAY<UUID>": ("list", "uuid"),
        "MAP<STRING,UUID>": ("map", "string", "uuid"),
        "STRUCT<U:UUID>": ("struct", (("U", "uuid"),)),
    }
    table = "uuid_cast_constructor_name"
    try:
        assert frame.columns == [name]
        assert tuple(frame.first()) == (None,)
        _persist(spark, frame, table, tmp_path, entry)
        _assert_artifacts(tmp_path, {name: types[target]}, [{name: None}])
        assert spark.table(table).columns == [name]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        spark.sql("DROP VIEW IF EXISTS uuid_constructor_result")


@pytest.mark.parametrize("cast", ["CAST", "TRY_CAST"])
@pytest.mark.parametrize("entry", ["sql", "dataframe"])
def test_uuid_cast_preserves_column_alias_and_chain_names(spark, cast, entry):
    # Spark DataFrameSuite SPARK-10743 retains column names through chained casts.
    source = spark.createDataFrame([(VALUE,)], "s STRING").repartition(2)
    if entry == "sql":
        source.createOrReplaceTempView("uuid_cast_source")
        try:
            frames = [
                (spark.sql(f"SELECT {cast}(s AS UUID) FROM uuid_cast_source"), "s"),  # noqa: S608
                (spark.sql(f"SELECT {cast}({cast}(s AS UUID) AS UUID) FROM uuid_cast_source"), "s"),  # noqa: S608
                (spark.sql(f"SELECT {cast}({cast}(s AS UUID) AS BINARY) FROM uuid_cast_source"), "s"),  # noqa: S608
                (spark.sql(f"SELECT {cast}(s AS UUID) AS renamed FROM uuid_cast_source"), "renamed"),  # noqa: S608
                (spark.sql(f"SELECT {cast}('{VALUE}' AS UUID)"), f"{cast}({VALUE} AS UUID)"),
            ]
            for frame, name in frames:
                assert frame.columns == [name]
                assert bytes(frame.first()[0]) == VALUE_BYTES
        finally:
            spark.catalog.dropTempView("uuid_cast_source")
    elif cast == "TRY_CAST" and not hasattr(type(F.col("s")), "try_cast"):
        # SQL expressions are supported by older Connect clients. Project the alias
        # before casting so its name still comes from the input, not a result alias.
        frames = [
            (source.select(F.expr("TRY_CAST(s AS UUID)")), "s"),
            (source.select(F.expr("TRY_CAST(TRY_CAST(s AS UUID) AS UUID)")), "s"),
            (source.select(F.expr("TRY_CAST(s AS UUID)").cast("BINARY")), "s"),
            (source.select(F.col("s").alias("renamed")).select(F.expr("TRY_CAST(renamed AS UUID)")), "renamed"),
            (source.select(F.expr("TRY_CAST(s AS UUID)").alias("renamed")), "renamed"),
            (source.select(F.expr(f"TRY_CAST('{VALUE}' AS UUID)")), f"TRY_CAST({VALUE} AS UUID)"),
        ]
        for frame, name in frames:
            assert frame.columns == [name]
            assert bytes(frame.first()[0]) == VALUE_BYTES
    else:

        def convert(column):
            return column.cast("UUID") if cast == "CAST" else column.try_cast("UUID")

        for expression, name in [
            (convert(F.col("s")), "s"),
            (convert(convert(F.col("s"))), "s"),
            (convert(F.col("s")).cast("BINARY"), "s"),
            (convert(F.col("s").alias("renamed")), "renamed"),
            (convert(F.col("s")).alias("renamed"), "renamed"),
            (convert(F.lit(VALUE)), f"{cast}({VALUE} AS UUID)"),
        ]:
            frame = source.select(expression)
            assert frame.columns == [name]
            assert bytes(frame.first()[0]) == VALUE_BYTES
