# ruff: noqa: S608
"""Iceberg REST DDL and cross-client metadata evolution."""

import datetime as dt
import json
from decimal import Decimal

import pytest
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.types import DoubleType

from pysail.testing.spark.ddl import exercise_lakehouse_alter


@pytest.fixture
def reference_catalog(iceberg_rest_endpoint, seaweedfs_host_endpoint):
    return RestCatalog(
        "reference",
        uri=iceberg_rest_endpoint,
        **{
            "s3.endpoint": seaweedfs_host_endpoint,
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.region": "us-east-1",
        },
    )


def test_rest_lakehouse_ddl(spark, reference_catalog):
    namespace = "rest_ddl"
    table = f"{namespace}.t"
    spark.sql(f"CREATE NAMESPACE {namespace}")
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, value STRING, part STRING) USING iceberg "
            "PARTITIONED BY (bucket(4, id)) TBLPROPERTIES ('format-version'='3')"
        )
        exercise_lakehouse_alter(spark, table)
        reference = reference_catalog.load_table(table)
        assert str(reference.schema().find_field("id").field_type) == "long"
        assert reference.schema().find_field("value").write_default is None
        assert "custom" not in reference.properties
        assert str(reference.spec().fields[0].transform) == "bucket[4]"
        # A reference client's schema commit must remain visible to Sail.
        with reference.update_schema() as update:
            update.add_column("score", DoubleType())
        assert spark.table(table).schema["score"].dataType.simpleString() == "double"
        location = reference.location()
        spark.sql(f"DROP TABLE {table}")
        spark.sql(f"CREATE TABLE {table} USING iceberg LOCATION '{location}'")
        assert spark.table(table).count() == 3  # noqa: PLR2004
        assert reference_catalog.load_table(table).schema() == reference.schema()
        spark.sql(f"DROP TABLE {table}")
        table = f"{namespace}.ctas"
        spark.sql(f"CREATE TABLE {table} USING iceberg AS SELECT 4 AS id")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} (other INT) USING iceberg")
        assert spark.table(table).first().id == 4  # noqa: PLR2004
    finally:
        spark.sql(f"DROP NAMESPACE {namespace} CASCADE")


@pytest.mark.parametrize("operation", ["create", "alter"])
def test_rest_typed_defaults(spark, reference_catalog, operation):
    namespace = f"rest_defaults_{operation}"
    table = f"{namespace}.t"
    columns = [
        ("amount", "DECIMAL(5, 2)", "1.2"),
        ("d", "DATE", "DATE '2026-01-02'"),
        ("ts", "TIMESTAMP_NTZ", "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'"),
    ]
    definitions = ", ".join(
        f"{name} {data_type}" + (f" DEFAULT {value}" if operation == "create" else "")
        for name, data_type, value in columns
    )
    spark.sql(f"CREATE NAMESPACE {namespace}")
    try:
        spark.sql(f"CREATE TABLE {table} (id INT, {definitions}) USING iceberg TBLPROPERTIES ('format-version'='3')")
        if operation == "alter":
            for name, _, value in columns:
                spark.sql(f"ALTER TABLE {table} ALTER COLUMN {name} SET DEFAULT {value}")
        reference = reference_catalog.load_table(table)
        fields = json.loads(reference.schema().model_dump_json())["fields"]
        assert {f["name"]: f["write-default"] for f in fields if f["name"] != "id"} == {
            "amount": "1.20",
            "d": "2026-01-02",
            "ts": "2026-01-02T03:04:05.123456",
        }
        before = reference.metadata_location
        with pytest.raises(Exception, match="Decimal literal cannot be represented"):
            spark.sql(f"ALTER TABLE {table} ALTER COLUMN amount SET DEFAULT 100000")
        assert reference_catalog.load_table(table).metadata_location == before
        with pytest.raises(Exception, match="Decimal literal cannot be represented"):
            spark.sql(
                f"CREATE TABLE {namespace}.invalid (amount DECIMAL(5, 2) DEFAULT 100000) "
                "USING iceberg TBLPROPERTIES ('format-version'='3')"
            )
        assert not reference_catalog.table_exists(f"{namespace}.invalid")
        spark.sql(f"INSERT INTO {table} (id) VALUES (1)")
        assert [tuple(r) for r in spark.table(table).collect()] == [
            (1, Decimal("1.20"), dt.date(2026, 1, 2), dt.datetime.fromisoformat("2026-01-02T03:04:05.123456"))
        ]
    finally:
        spark.sql(f"DROP NAMESPACE {namespace} CASCADE")


def test_rest_invalid_ddl_preserves_metadata(spark, reference_catalog):
    namespace = "rest_ddl_invalid"
    table = f"{namespace}.t"
    spark.sql(f"CREATE NAMESPACE {namespace}")
    try:
        spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg")
        before = reference_catalog.load_table(table).metadata_location
        for operation, error in [
            ("ALTER COLUMN id TYPE STRING", "Cannot change Iceberg column"),
            ("ALTER COLUMN id SET DEFAULT 1", "format-version=3"),
            ("UNSET TBLPROPERTIES ('absent')", "not set"),
            ("SET TBLPROPERTIES ('metadata_location'='s3://invalid/metadata.json')", "reserved property"),
        ]:
            with pytest.raises(Exception, match=error):
                spark.sql(f"ALTER TABLE {table} {operation}")
            assert reference_catalog.load_table(table).metadata_location == before
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('format-version'='3')")
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN id SET DEFAULT 7")
        assert reference_catalog.load_table(table).schema().find_field("id").write_default == 7  # noqa: PLR2004
        spark.sql(f"INSERT INTO {table} VALUES (DEFAULT)")
        assert spark.table(table).first().id == 7  # noqa: PLR2004
    finally:
        spark.sql(f"DROP NAMESPACE {namespace} CASCADE")
