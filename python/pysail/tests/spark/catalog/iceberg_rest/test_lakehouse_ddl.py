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


def test_rest_column_changes_preserve_ids_and_catalog_schema(spark, reference_catalog):
    from pysail.testing.spark.ddl import exercise_lakehouse_column_changes

    namespace = "rest_column_changes"
    table = f"{namespace}.t"
    spark.sql(f"CREATE NAMESPACE {namespace}")
    try:
        spark.sql(f"CREATE TABLE {table} (id INT, value STRING) USING iceberg")
        before = reference_catalog.load_table(table).schema().find_field("value").field_id
        exercise_lakehouse_column_changes(spark, table)
        reference = reference_catalog.load_table(table)
        assert reference.schema().find_field("renamed").field_id == before
        assert reference.schema().find_field("renamed").doc == "renamed column"
        assert reference.scan().to_arrow().sort_by("id").to_pylist() == [
            {"renamed": "one", "id": 1},
            {"renamed": "two", "id": 2},
        ]
    finally:
        spark.sql(f"DROP NAMESPACE {namespace} CASCADE")


def test_rest_table_rename_across_namespaces(spark, reference_catalog):
    source = "rest_rename_source"
    destination = "rest_rename_destination"
    spark.sql(f"CREATE NAMESPACE {source}")
    spark.sql(f"CREATE NAMESPACE {destination}")
    try:
        spark.sql(f"CREATE TABLE {source}.t (id INT) USING iceberg")
        spark.sql(f"INSERT INTO {source}.t VALUES (1)")
        before = reference_catalog.load_table(f"{source}.t").metadata_location
        spark.sql(f"ALTER TABLE {source}.t RENAME TO {destination}.renamed")
        assert not reference_catalog.table_exists(f"{source}.t")
        assert reference_catalog.load_table(f"{destination}.renamed").metadata_location == before
        spark.sql(f"INSERT INTO {destination}.renamed VALUES (2)")
        assert [r.id for r in spark.sql(f"SELECT * FROM {destination}.renamed ORDER BY id").collect()] == [1, 2]
    finally:
        spark.sql(f"DROP NAMESPACE {source} CASCADE")
        spark.sql(f"DROP NAMESPACE {destination} CASCADE")


def test_rest_replace_columns(spark, reference_catalog):
    from pysail.testing.spark.ddl import exercise_lakehouse_replace_columns

    table = "rest_replace.t"
    spark.sql("CREATE NAMESPACE rest_replace")
    try:
        spark.sql(f"CREATE TABLE {table} (id INT, value STRING) USING iceberg")
        before = reference_catalog.load_table(table).schema()
        exercise_lakehouse_replace_columns(spark, table, "iceberg")
        after = reference_catalog.load_table(table).schema()
        assert after.find_field("id").field_id > before.highest_field_id
        assert after.find_field("value").doc == "123"
    finally:
        spark.sql("DROP NAMESPACE rest_replace CASCADE")


def test_rest_set_location(spark, reference_catalog):
    table = "rest_location.t"
    spark.sql("CREATE NAMESPACE rest_location")
    try:
        spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg")
        spark.sql(f"INSERT INTO {table} VALUES (1)")
        location = reference_catalog.load_table(table).location() + "_moved"
        spark.sql(f"ALTER TABLE {table} SET LOCATION '{location}'")
        assert reference_catalog.load_table(table).location() == location
        spark.sql(f"INSERT INTO {table} VALUES (2)")
        assert [r.id for r in spark.table(table).orderBy("id").collect()] == [1, 2]
    finally:
        spark.sql("DROP NAMESPACE rest_location CASCADE")


def test_rest_write_order(spark, reference_catalog):
    from pysail.testing.spark.ddl import exercise_iceberg_write_order

    table = "rest_order.t"
    spark.sql("CREATE NAMESPACE rest_order")
    try:
        spark.sql(f"CREATE TABLE {table} (id INT, value INT) USING iceberg")
        exercise_iceberg_write_order(spark, table)
        loaded = reference_catalog.load_table(table)
        assert len(loaded.sort_order().fields) == 2
        assert loaded.sort_order().fields[0].direction.value == "desc"
        assert loaded.properties["write.distribution-mode"] == "range"
        spark.sql(f"ALTER TABLE {table} WRITE UNORDERED")
        assert reference_catalog.load_table(table).sort_order().order_id == 0
    finally:
        spark.sql("DROP NAMESPACE rest_order CASCADE")


def test_rest_partition_evolution(spark, reference_catalog):
    from pysail.testing.spark.ddl import exercise_iceberg_partition_evolution

    table = "rest_partitions.t"
    spark.sql("CREATE NAMESPACE rest_partitions")
    try:
        spark.sql(f"CREATE TABLE {table} (id INT, part STRING) USING iceberg PARTITIONED BY (part)")
        exercise_iceberg_partition_evolution(spark, table)
        loaded = reference_catalog.load_table(table)
        assert loaded.spec().fields[0].name == "part_prefix"
        assert str(loaded.spec().fields[0].transform) == "truncate[2]"
    finally:
        spark.sql("DROP NAMESPACE rest_partitions CASCADE")
