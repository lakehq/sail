"""Retained coalesce must not invent UUID identity for ordinary fixed[16]."""

from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg import types as iceberg_types
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing._dev.spark.steps.iceberg import _find_latest_metadata
from pysail.testing.spark.utils.sql import escape_sql_string_literal

U = UUID("6f1c2d3e-4b5a-4c6d-8e7f-0123456789ab").bytes
V = UUID("00000000-0000-0000-0000-000000000001").bytes
FIXED_LENGTH = 16


def _rows(table):
    return sorted(
        [
            {name: value.bytes if isinstance(value, UUID) else value for name, value in row.items()}
            for row in table.to_pylist()
        ],
        key=repr,
    )


def _to_arrow(frame):
    if hasattr(type(frame), "toArrow"):
        return frame.toArrow()
    query = frame._plan.to_proto(frame.sparkSession.client)  # noqa: SLF001
    table, _ = frame.sparkSession.client.to_table(query)
    return table


def _assert_output(spark, frame, table, location, entry, types, expected):
    result = _to_arrow(frame)
    assert _rows(result) == sorted(expected, key=repr)
    # Connect exposes UUID and fixed[16] as public Binary fields. Their logical
    # identity is checked below in the persisted Iceberg and Parquet schemas.
    for field in frame.schema:
        assert field.dataType.simpleString() == "binary"
        assert "ARROW:extension:name" not in field.metadata
        assert "ARROW:extension:metadata" not in field.metadata
    if hasattr(type(frame), "toArrow"):
        for field in result.schema:
            assert field.type == pa.binary()
            assert b"ARROW:extension:name" not in (field.metadata or {})
            assert b"ARROW:extension:metadata" not in (field.metadata or {})
    try:
        if entry == "sql":
            frame.createOrReplaceTempView("uuid_coalesce_result")
            path = escape_sql_string_literal(str(location))
            spark.sql(f"CREATE TABLE {table} USING iceberg LOCATION '{path}' AS SELECT * FROM uuid_coalesce_result")  # noqa: S608
        else:
            frame.write.format("iceberg").option("path", str(location)).saveAsTable(table)
        metadata = _find_latest_metadata(location)
        schema = next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])
        assert {field["name"]: field["type"] for field in schema["fields"]} == types
        paths = list(Path(location).rglob("*.parquet"))
        assert paths
        rows = []
        for path in paths:
            parquet = pq.ParquetFile(path)
            assert parquet.schema_arrow.names == list(types)
            for index, (name, data_type) in enumerate(types.items()):
                column = parquet.schema.column(index)
                assert column.name == name
                assert column.physical_type == "FIXED_LEN_BYTE_ARRAY"
                assert column.length == FIXED_LENGTH
                assert column.logical_type.type == ("UUID" if data_type == "uuid" else "NONE")
                assert parquet.schema_arrow.field(name).type == (
                    pa.uuid() if data_type == "uuid" else pa.binary(FIXED_LENGTH)
                )
            rows.extend(_rows(parquet.read()))
        assert sorted(rows, key=repr) == sorted(expected, key=repr)
        assert _rows(_to_arrow(spark.table(table))) == sorted(expected, key=repr)
        assert _rows(_to_arrow(spark.read.format("iceberg").load(str(location)))) == sorted(expected, key=repr)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        spark.sql("DROP VIEW IF EXISTS uuid_coalesce_result")


@pytest.mark.parametrize("entry", ["sql", "dataframe"])
def test_uuid_retained_coalesce_consensus(spark, sql_catalog, tmp_path, entry):
    schema = Schema(
        iceberg_types.NestedField(1, "u", iceberg_types.UUIDType(), required=False),
        iceberg_types.NestedField(2, "u2", iceberg_types.UUIDType(), required=False),
        iceberg_types.NestedField(3, "f", iceberg_types.FixedType(FIXED_LENGTH), required=False),
        iceberg_types.NestedField(4, "nf", iceberg_types.FixedType(FIXED_LENGTH), required=False),
    )
    identifier = "default.uuid_coalesce_source"
    source = sql_catalog.create_table(identifier, schema)
    rows = [
        {"u": U, "u2": V, "f": b"\xa5" * FIXED_LENGTH, "nf": None},
        {"u": None, "u2": V, "f": b"\x5a" * FIXED_LENGTH, "nf": None},
        {"u": V, "u2": None, "f": None, "nf": None},
        {"u": None, "u2": None, "f": None, "nf": None},
    ]
    try:
        source.append(pa.Table.from_pylist(rows, schema=schema_to_pyarrow(schema)))
        scanned = spark.read.format("iceberg").load(source.location()).repartition(2)
        scanned.createOrReplaceTempView("uuid_coalesce_source")
        # Volatility keeps the coalesce kernel, rather than the CASE rewrite.
        # Cast the CASE result back to UUID because CASE itself loses metadata.
        vu = "CAST(CASE WHEN rand(1)>-1 THEN CAST(u AS BINARY) ELSE NULL END AS UUID)"
        vf = "CASE WHEN rand(1)>-1 THEN f ELSE NULL END"
        vn = "CASE WHEN rand(1)>-1 THEN nf ELSE NULL END"
        expressions = {
            "mixed": (f"coalesce({vu},f)", "fixed[16]", lambda r: r["u"] or r["f"]),
            "reverse": (f"coalesce({vf},u)", "fixed[16]", lambda r: r["f"] or r["u"]),
            "typed_null": (f"coalesce({vu},nf)", "fixed[16]", lambda r: r["u"]),
            "typed_null_reverse": (f"coalesce({vn},u)", "fixed[16]", lambda r: r["u"]),
            "all_uuid": (f"coalesce({vu},u2)", "uuid", lambda r: r["u"] or r["u2"]),
            "untyped_null": (f"coalesce({vu},NULL)", "uuid", lambda r: r["u"]),
            "untyped_null_first": (
                f"coalesce(NULL,{vu},u2)",
                "uuid",
                lambda r: r["u"] or r["u2"],
            ),
            "lazy_mixed": (
                f"coalesce({vu},{vf})",
                "fixed[16]",
                lambda r: r["u"] or r["f"],
            ),
        }
        if entry == "sql":
            selection = ", ".join(f"{expression} AS {name}" for name, (expression, _, _) in expressions.items())
            frame = spark.sql(f"SELECT {selection} FROM uuid_coalesce_source")  # noqa: S608
        else:
            frame = scanned.select(
                *(F.expr(expression).alias(name) for name, (expression, _, _) in expressions.items())
            )
        expected = [{name: value(row) for name, (_, _, value) in expressions.items()} for row in rows]
        types = {name: data_type for name, (_, data_type, _) in expressions.items()}
        _assert_output(
            spark,
            frame,
            "uuid_coalesce_output",
            tmp_path / "result",
            entry,
            types,
            expected,
        )

        # Single-input aggregates retain UUID; do not remove the global type whitelist.
        aggregates = (
            spark.sql("SELECT min(u) AS lo, max(u) AS hi FROM uuid_coalesce_source")
            if entry == "sql"
            else scanned.agg(F.min("u").alias("lo"), F.max("u").alias("hi"))
        )
        _assert_output(
            spark,
            aggregates,
            "uuid_coalesce_aggregates",
            tmp_path / "aggregates",
            entry,
            {"lo": "uuid", "hi": "uuid"},
            [{"lo": V, "hi": U}],
        )
    finally:
        spark.sql("DROP VIEW IF EXISTS uuid_coalesce_source")
        sql_catalog.drop_table(identifier)
