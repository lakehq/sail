# ruff: noqa: S608
"""Named-table semantics follow Iceberg 1.11 / Spark 4.1 TestDeleteFrom.

Path addressing is additionally tested as a Sail capability.
"""

import pyarrow as pa
import pytest
from pyiceberg.manifest import ManifestContent
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField
from pyspark.sql.types import StructType

from pysail.testing.spark.steps.iceberg import _current_snapshot, _find_latest_metadata
from pysail.tests.spark.iceberg.test_iceberg_equality_delete import _append_equality_delete_snapshot
from pysail.tests.spark.iceberg.test_iceberg_merge import _current_manifest_entries


@pytest.mark.parametrize("format_version", [1, 2, 3])
@pytest.mark.parametrize("mode", ["copy-on-write", "merge-on-read"])
@pytest.mark.parametrize("by_path", [False, True])
def test_truncate_metadata_only_preserves_history(spark, tmp_path, format_version, mode, by_path):
    name = "iceberg_truncate"
    path = tmp_path / "table"
    target = f"iceberg.`{path.as_uri()}`" if by_path else name
    moved = []
    try:
        spark.sql(f"""CREATE TABLE {name} (id INT, p STRING) USING iceberg PARTITIONED BY (p)
            LOCATION '{path.as_uri()}' TBLPROPERTIES ('format-version'='{format_version}',
            'write.delete.mode'='{mode}', 'note'='keep')""")
        empty = _find_latest_metadata(path)
        result = spark.sql(f"TRUNCATE TABLE {target}")
        assert result.schema == StructType([])
        assert result.collect() == []
        first = _find_latest_metadata(path)
        assert len(first["snapshots"]) == len(empty.get("snapshots", [])) + 1
        assert _current_snapshot(first)["summary"]["operation"] == "delete"
        spark.sql(f"INSERT INTO {name} VALUES (1, 'a'), (2, 'b'), (3, NULL)")
        before = _find_latest_metadata(path)
        schema = spark.table(name).schema
        files = set(path.rglob("*.parquet"))
        assert files
        for index, file in enumerate(sorted(files)):
            backup = tmp_path / f"saved-{index}.parquet"
            file.rename(backup)
            moved.append((file, backup))
        result = spark.sql(f"TRUNCATE TABLE {target}")
        assert result.schema == StructType([])
        assert result.collect() == []
        for file, backup in moved:
            backup.rename(file)
        moved.clear()
        after = _find_latest_metadata(path)
        assert after["snapshots"][:-1] == before["snapshots"]
        assert after["schemas"] == before["schemas"]
        assert after["properties"] == before["properties"]
        assert after["partition-specs"] == before["partition-specs"]
        assert _current_snapshot(after)["summary"]["operation"] == "delete"
        assert spark.table(name).schema == schema
        assert spark.table(name).collect() == []
        assert set(path.rglob("*.parquet")) == files
        historical = (
            spark.read.format("iceberg").option("snapshotId", before["current-snapshot-id"]).load(path.as_uri())
        )
        assert [row.id for row in historical.orderBy("id").collect()] == [1, 2, 3]
        spark.sql(f"TRUNCATE TABLE {target}").collect()
        repeated = _find_latest_metadata(path)
        assert repeated["snapshots"][:-1] == after["snapshots"]
        assert _current_snapshot(repeated)["summary"]["operation"] == "delete"
        spark.sql(f"INSERT INTO {name} VALUES (4, 'c')")
        assert [row.id for row in spark.table(name).collect()] == [4]
    finally:
        for file, backup in moved:
            backup.rename(file)
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("format_version", [2, 3])
def test_truncate_removes_position_deletes_and_deletion_vectors(spark, tmp_path, format_version):
    name = "iceberg_truncate_deletes"
    path = tmp_path / name
    try:
        spark.sql(f"""CREATE TABLE {name} (id INT) USING iceberg LOCATION '{path.as_uri()}'
            TBLPROPERTIES ('format-version'='{format_version}', 'write.delete.mode'='merge-on-read')""")
        spark.sql(f"INSERT INTO {name} VALUES (1), (2), (3)")
        spark.sql(f"DELETE FROM {name} WHERE id=1").collect()
        assert _current_manifest_entries(path, ManifestContent.DELETES)
        before = _find_latest_metadata(path)
        files = {file for file in path.rglob("*") if file.is_file()}
        spark.sql(f"TRUNCATE TABLE {name}").collect()
        assert not _current_manifest_entries(path, ManifestContent.DATA)
        assert not _current_manifest_entries(path, ManifestContent.DELETES)
        summary = _current_snapshot(_find_latest_metadata(path))["summary"]
        assert int(summary["removed-delete-files"]) > 0
        assert summary["total-position-deletes"] == "0"
        if format_version == 3:  # noqa: PLR2004
            assert int(summary["removed-dvs"]) > 0
        assert all(file.exists() for file in files)
        historical = (
            spark.read.format("iceberg").option("snapshotId", before["current-snapshot-id"]).load(path.as_uri())
        )
        assert [row.id for row in historical.orderBy("id").collect()] == [2, 3]
        spark.sql(f"INSERT INTO {name} VALUES (1)")
        assert [row.id for row in spark.table(name).collect()] == [1]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_truncate_removes_unreferenced_equality_deletes(spark, sql_catalog):
    identifier = "default.truncate_equality"
    name = "iceberg_truncate_equality"
    table = sql_catalog.create_table(identifier, Schema(NestedField(1, "id", LongType(), required=False)))
    try:
        table.append(pa.table({"id": [1, 2, 3]}))
        path = _append_equality_delete_snapshot(table, pa.table({"id": [1]}), [1])
        spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{path.as_uri()}'")
        assert _current_manifest_entries(path, ManifestContent.DELETES)
        files = set(path.rglob("*.parquet"))
        spark.sql(f"TRUNCATE TABLE {name}").collect()
        assert spark.table(name).collect() == []
        assert not _current_manifest_entries(path, ManifestContent.DELETES)
        summary = _current_snapshot(_find_latest_metadata(path))["summary"]
        assert summary["removed-equality-delete-files"] == "1"
        assert summary["total-equality-deletes"] == "0"
        assert set(path.rglob("*.parquet")) == files
        spark.sql(f"INSERT INTO {name} VALUES (1)")
        assert [row.id for row in spark.table(name).collect()] == [1]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        sql_catalog.drop_table(identifier)


def test_truncate_resolves_iceberg_namespace_as_named_table(spark, tmp_path):
    catalog = spark.sql("SELECT current_catalog()").first()[0]
    name = f"`{catalog}`.iceberg.truncate_namespace"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{catalog}`.iceberg")
    try:
        spark.sql(f"CREATE TABLE {name} (id INT) USING iceberg LOCATION '{(tmp_path / 'named').as_uri()}'")
        spark.sql(f"INSERT INTO {name} VALUES (1)")
        spark.sql("TRUNCATE TABLE iceberg.truncate_namespace").collect()
        assert spark.table(name).collect() == []
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        spark.sql(f"DROP DATABASE `{catalog}`.iceberg")


def test_truncate_rejects_view_shadowing_and_partition_clause(spark, tmp_path):
    name = "iceberg_truncate_guard"
    path = tmp_path / name
    try:
        spark.sql(f"CREATE TABLE {name} (id INT, p INT) USING iceberg PARTITIONED BY (p) LOCATION '{path.as_uri()}'")
        spark.sql(f"INSERT INTO {name} VALUES (1, 1), (2, 2)")
        before = _find_latest_metadata(path)
        with pytest.raises(Exception, match=r"(?i)syntax|parse|partition"):
            spark.sql(f"TRUNCATE TABLE {name} PARTITION (p=1)").collect()
        spark.sql(f"CREATE TEMP VIEW {name} AS SELECT 9 AS id")
        try:
            with pytest.raises(Exception, match=r"expects a table.*view"):
                spark.sql(f"TRUNCATE TABLE {name}").collect()
        finally:
            spark.sql(f"DROP VIEW {name}")
        with pytest.raises(Exception, match=r"(?i)not found|does not exist"):
            spark.sql("TRUNCATE TABLE iceberg_truncate_missing").collect()
        assert _find_latest_metadata(path) == before
        assert [row.id for row in spark.table(name).orderBy("id").collect()] == [1, 2]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
