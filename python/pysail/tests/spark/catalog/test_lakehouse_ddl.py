# ruff: noqa: S608
"""Delta and Iceberg DDL in the default memory catalog."""

import datetime as dt
from decimal import Decimal

import pytest

from pysail.testing.spark.ddl import exercise_lakehouse_alter
from pysail.testing.spark.steps.iceberg import _find_latest_metadata


@pytest.mark.parametrize("fmt", ["delta", "iceberg"])
def test_memory_lakehouse_ddl(spark, tmp_path, fmt):
    spark.conf.set("spark.sql.warehouse.dir", str(tmp_path / "warehouse"))
    table = f"default.ddl_{fmt}"
    location = (tmp_path / fmt).as_uri()
    properties = "'format-version'='3'" if fmt == "iceberg" else "'delta.enableTypeWidening'='true'"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, value STRING, part STRING) USING {fmt} "
            f"PARTITIONED BY (part) LOCATION '{location}' TBLPROPERTIES ({properties})"
        )
        exercise_lakehouse_alter(spark, table)
        assert (
            next(r.data_type for r in spark.sql(f"DESCRIBE TABLE {table}").collect() if r.col_name == "id") == "bigint"
        )
        spark.sql(f"DROP TABLE {table}")
        spark.sql(f"CREATE TABLE {table} USING {fmt} LOCATION '{location}'")
        assert spark.table(table).count() == 3  # noqa: PLR2004
        assert spark.table(table).schema["id"].dataType.simpleString() == "bigint"
        spark.sql(f"DROP TABLE {table}")
        spark.sql(f"CREATE TABLE {table} USING {fmt} AS SELECT 4 AS id, 'ctas' AS value")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} (other INT) USING {fmt}")
        assert [tuple(r) for r in spark.table(table).collect()] == [(4, "ctas")]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize(
    ("data_type", "expression", "expected"),
    [
        ("DECIMAL(9, 2)", "1", "1.00"),
        ("DECIMAL(9, 2)", "1.2", "1.20"),
        ("DECIMAL(9, 2)", "1.234", "1.23"),
        ("DECIMAL(9, 2)", "1.235", "1.24"),
        ("DECIMAL(9, 2)", "-1.235", "-1.24"),
        ("DECIMAL(9, 2)", "1e-3", "0.00"),
        ("DECIMAL(38, 2)", "12345678901234567890123456789012345.6", "12345678901234567890123456789012345.60"),
    ],
)
def test_iceberg_decimal_defaults(spark, tmp_path, data_type, expression, expected):
    table = "default.decimal_defaults"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, value {data_type}) USING iceberg "
            f"LOCATION '{tmp_path.as_uri()}' TBLPROPERTIES ('format-version'='3')"
        )
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN value SET DEFAULT {expression}")
        metadata = _find_latest_metadata(tmp_path)
        schema = next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])
        assert schema["fields"][1]["write-default"] == expected
        spark.sql(f"INSERT INTO {table} (id) VALUES (1)")
        spark.sql(f"INSERT INTO {table} VALUES (2, DEFAULT)")
        assert [tuple(r) for r in spark.table(table).orderBy("id").collect()] == [
            (1, Decimal(expected)),
            (2, Decimal(expected)),
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_iceberg_decimal_default_overflow_preserves_metadata(spark, tmp_path):
    table = "default.decimal_default_overflow"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, value DECIMAL(5, 2)) USING iceberg "
            f"LOCATION '{tmp_path.as_uri()}' TBLPROPERTIES ('format-version'='3')"
        )
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN value SET DEFAULT 12.34")
        metadata_dir = tmp_path / "metadata"
        before = {p.name: p.read_bytes() for p in metadata_dir.iterdir() if p.is_file()}
        for expression in ["1000", "-1000", "100000", "999.995", "-999.995", "99999999999999999999999999999999999999"]:
            with pytest.raises(Exception, match="Decimal literal cannot be represented"):
                spark.sql(f"ALTER TABLE {table} ALTER COLUMN value SET DEFAULT {expression}")
            assert {p.name: p.read_bytes() for p in metadata_dir.iterdir() if p.is_file()} == before
        spark.sql(f"INSERT INTO {table} (id) VALUES (1)")
        assert [tuple(r) for r in spark.table(table).collect()] == [(1, Decimal("12.34"))]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize(
    ("data_type", "expression", "expected", "stored"),
    [
        ("DATE", "DATE '2026-01-02'", dt.date(2026, 1, 2), "2026-01-02"),
        ("DATE", "'2026-01-02'", dt.date(2026, 1, 2), "2026-01-02"),
        (
            "TIMESTAMP",
            "TIMESTAMP '2026-01-02 03:04:05.123456+00:00'",
            dt.datetime.fromisoformat("2026-01-02T03:04:05.123456"),
            "2026-01-02T03:04:05.123456+00:00",
        ),
        (
            "TIMESTAMP",
            "'2026-01-02 03:04:05.123456+00:00'",
            dt.datetime.fromisoformat("2026-01-02T03:04:05.123456"),
            "2026-01-02T03:04:05.123456+00:00",
        ),
        (
            "TIMESTAMP_NTZ",
            "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'",
            dt.datetime.fromisoformat("2026-01-02T03:04:05.123456"),
            "2026-01-02T03:04:05.123456",
        ),
    ],
)
@pytest.mark.parametrize("local_timezone", ["UTC"], indirect=True)
@pytest.mark.usefixtures("local_timezone")
def test_iceberg_temporal_defaults(spark, tmp_path, data_type, expression, expected, stored):
    table = "default.temporal_defaults"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, value {data_type}) USING iceberg "
            f"LOCATION '{tmp_path.as_uri()}' TBLPROPERTIES ('format-version'='3')"
        )
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN value SET DEFAULT {expression}")
        metadata = _find_latest_metadata(tmp_path)
        schema = next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])
        assert schema["fields"][1]["write-default"] == stored
        spark.sql(f"INSERT INTO {table} (id) VALUES (1)")
        spark.sql(f"INSERT INTO {table} VALUES (2, DEFAULT)")
        assert [tuple(r) for r in spark.table(table).orderBy("id").collect()] == [(1, expected), (2, expected)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
