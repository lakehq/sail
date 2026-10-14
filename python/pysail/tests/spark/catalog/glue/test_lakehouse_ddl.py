"""Lakehouse DDL with Glue schema and metadata-pointer verification."""

import pytest
from pyiceberg.table import StaticTable

from pysail.testing.spark.ddl import exercise_lakehouse_alter
from pysail.tests.spark.catalog.glue.test_iceberg_commit import _glue_client


@pytest.mark.parametrize("fmt", ["delta", "iceberg"])
def test_glue_lakehouse_ddl(spark, tmp_path, moto_endpoint, fmt):
    table = f"ddl_{fmt}"
    name = f"test_db.{table}"
    location = (tmp_path / fmt).as_uri()
    properties = "'format-version'='3'" if fmt == "iceberg" else "'delta.enableTypeWidening'='true'"
    try:
        spark.sql(
            f"CREATE TABLE {name} (id INT, value STRING, part STRING) USING {fmt} "
            f"PARTITIONED BY (part) LOCATION '{location}' TBLPROPERTIES ({properties})"
        )
        exercise_lakehouse_alter(spark, name)
        stored = _glue_client(moto_endpoint).get_table(DatabaseName="test_db", Name=table)["Table"]
        assert next(c["Type"] for c in stored["StorageDescriptor"]["Columns"] if c["Name"] == "id") == "bigint"
        assert "custom" not in stored["Parameters"]
        if fmt == "iceberg":
            assert stored.get("PartitionKeys", []) == []
            reference = StaticTable.from_metadata(stored["Parameters"]["metadata_location"])
            assert str(reference.schema().find_field("id").field_type) == "long"
            assert sorted(reference.scan().to_arrow().to_pylist(), key=lambda r: r["id"]) == [
                {"id": 1, "value": "one", "part": "a"},
                {"id": 2, "value": "fallback", "part": "b"},
                {"id": 3, "value": None, "part": "c"},
            ]
        spark.sql(f"DROP TABLE {name}")
        spark.sql(f"CREATE TABLE {name} USING {fmt} LOCATION '{location}'")
        assert spark.table(name).count() == 3  # noqa: PLR2004
        assert [r.col_name for r in spark.sql(f"DESCRIBE TABLE {name}").collect()][:3] == ["id", "value", "part"]
        spark.sql(f"DROP TABLE {name}")
        ctas_location = (tmp_path / f"ctas_{fmt}").as_uri()
        spark.sql(f"CREATE TABLE {name} USING {fmt} LOCATION '{ctas_location}' AS SELECT 4 AS id")
        assert spark.table(name).first().id == 4  # noqa: PLR2004
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("fmt", ["delta", "iceberg"])
def test_glue_column_changes(spark, tmp_path, moto_endpoint, fmt):
    from pysail.testing.spark.ddl import exercise_lakehouse_column_changes

    table = f"columns_{fmt}"
    name = f"test_db.{table}"
    properties = "TBLPROPERTIES ('delta.columnMapping.mode'='name')" if fmt == "delta" else ""
    try:
        spark.sql(f"CREATE TABLE {name} (id INT, value STRING) USING {fmt} LOCATION '{tmp_path.as_uri()}' {properties}")
        exercise_lakehouse_column_changes(spark, name)
        stored = _glue_client(moto_endpoint).get_table(DatabaseName="test_db", Name=table)["Table"]
        assert [c["Name"] for c in stored["StorageDescriptor"]["Columns"]] == ["renamed", "id"]
        assert stored["StorageDescriptor"]["Columns"][0]["Comment"] == "renamed column"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("fmt", ["delta", "iceberg"])
def test_glue_replace_columns(spark, tmp_path, moto_endpoint, fmt):
    from pysail.testing.spark.ddl import exercise_lakehouse_replace_columns

    table = f"replace_{fmt}"
    name = f"test_db.{table}"
    properties = "TBLPROPERTIES ('delta.columnMapping.mode'='name')" if fmt == "delta" else ""
    try:
        spark.sql(f"CREATE TABLE {name} (id INT, value STRING) USING {fmt} LOCATION '{tmp_path.as_uri()}' {properties}")
        exercise_lakehouse_replace_columns(spark, name, fmt)
        stored = _glue_client(moto_endpoint).get_table(DatabaseName="test_db", Name=table)["Table"]
        assert [c["Name"] for c in stored["StorageDescriptor"]["Columns"]] == ["value", "id", "extra"]
        assert stored["StorageDescriptor"]["Columns"][0]["Comment"] == "123"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_glue_iceberg_set_location(spark, tmp_path, moto_endpoint):
    table = "test_db.location_table"
    try:
        spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg LOCATION '{(tmp_path / 'original').as_uri()}'")
        spark.sql(f"INSERT INTO {table} VALUES (1)")
        location = (tmp_path / "moved").as_uri()
        spark.sql(f"ALTER TABLE {table} SET LOCATION '{location}'")
        spark.sql(f"INSERT INTO {table} VALUES (2)")
        stored = _glue_client(moto_endpoint).get_table(DatabaseName="test_db", Name="location_table")["Table"]
        assert stored["StorageDescriptor"]["Location"] == location
        reference = StaticTable.from_metadata(stored["Parameters"]["metadata_location"])
        assert reference.location() == location
        assert sorted(reference.scan().to_arrow()["id"].to_pylist()) == [1, 2]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_glue_iceberg_partition_evolution(spark, tmp_path, moto_endpoint):
    from pysail.testing.spark.ddl import exercise_iceberg_partition_evolution

    table = "test_db.partition_evolution"
    try:
        spark.sql(f"CREATE TABLE {table} (id INT, part STRING) USING iceberg PARTITIONED BY (part) LOCATION '{tmp_path.as_uri()}'")
        exercise_iceberg_partition_evolution(spark, table)
        stored = _glue_client(moto_endpoint).get_table(DatabaseName="test_db", Name="partition_evolution")["Table"]
        assert stored.get("PartitionKeys", []) == []
        reference = StaticTable.from_metadata(stored["Parameters"]["metadata_location"])
        assert reference.spec().fields[0].name == "part_prefix"
        assert sorted(reference.scan().to_arrow()["id"].to_pylist()) == [1, 3, 4]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
