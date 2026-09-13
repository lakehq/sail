# ruff: noqa: S608

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.manifest import ManifestContent
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField

from pysail.testing.spark.steps.iceberg import _current_snapshot, _find_latest_metadata, _latest_metadata_path
from pysail.tests.spark.iceberg.test_iceberg_merge import _current_manifest_entries, _local_file_path


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("predicate", ["part = 'A'", "id < 3", "true"])
def test_cow_metadata_delete_does_not_read_parquet(spark, tmp_path, format_version, predicate):
    name = "cow_metadata_delete"
    path = tmp_path / name
    moved = []
    try:
        spark.sql(f"""CREATE TABLE {name} (id INT, part STRING) USING iceberg
            PARTITIONED BY (part) LOCATION '{path.as_uri()}'
            TBLPROPERTIES ('format-version'='{format_version}')""")
        spark.sql(f"INSERT INTO {name} VALUES (1,'A'),(2,'A')")
        spark.sql(f"INSERT INTO {name} VALUES (3,'B'),(4,'B')")
        before = _find_latest_metadata(path)
        for index, file in enumerate(path.rglob("*.parquet")):
            backup = tmp_path / f"saved-{index}.parquet"
            file.rename(backup)
            moved.append((file, backup))
        assert moved
        spark.sql(f"DELETE FROM {name} WHERE {predicate}").collect()
        for file, backup in moved:
            backup.rename(file)
        moved.clear()
        expected = [] if predicate == "true" else [(3, "B"), (4, "B")]
        assert [tuple(row) for row in spark.table(name).orderBy("id").collect()] == expected
        after = _find_latest_metadata(path)
        assert _current_snapshot(after)["summary"]["operation"] == "delete"
        assert after["snapshots"][:-1] == before["snapshots"]
        historical = (
            spark.read.format("iceberg").option("snapshotId", before["current-snapshot-id"]).load(path.as_uri())
        )
        assert [tuple(row) for row in historical.orderBy("id").collect()] == [(1, "A"), (2, "A"), (3, "B"), (4, "B")]
    finally:
        for file, backup in moved:
            backup.rename(file)
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("operation", ["delete", "update", "merge"])
def test_cow_prunes_unrelated_files_without_filtering_survivors(spark, tmp_path, format_version, operation):
    name = "cow_pruned_files"
    path = tmp_path / name
    moved = []
    untouched_id = 3
    try:
        spark.sql(f"""CREATE TABLE {name} (id INT, value INT, part STRING) USING iceberg
            PARTITIONED BY (part) LOCATION '{path.as_uri()}'
            TBLPROPERTIES ('format-version'='{format_version}')""")
        spark.sql(f"INSERT INTO {name} VALUES (1,10,'A'),(2,20,'A')")
        spark.sql(f"INSERT INTO {name} VALUES (3,30,'B'),(4,40,'B')")
        for index, entry in enumerate(_current_manifest_entries(path, ManifestContent.DATA)):
            file = _local_file_path(entry.data_file.file_path)
            if untouched_id in pq.ParquetFile(file).read(columns=["id"]).column("id").to_pylist():
                backup = tmp_path / f"saved-{index}.parquet"
                file.rename(backup)
                moved.append((file, backup))
        assert moved
        statements = {
            "delete": f"DELETE FROM {name} WHERE id=1",
            "update": f"UPDATE {name} SET value=100 WHERE id=1",
            "merge": f"""MERGE INTO {name} t USING (SELECT 1 AS id) s
                ON t.id=s.id AND t.part='A' WHEN MATCHED THEN UPDATE SET value=100""",
        }
        spark.sql(statements[operation]).collect()
        for file, backup in moved:
            backup.rename(file)
        moved.clear()
        expected = [(2, 20, "A"), (3, 30, "B"), (4, 40, "B")]
        if operation != "delete":
            expected.insert(0, (1, 100, "A"))
        assert [tuple(row) for row in spark.table(name).orderBy("id").collect()] == expected
    finally:
        for file, backup in moved:
            backup.rename(file)
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("operation", ["delete", "update", "merge"])
def test_cow_rewrites_only_affected_files_and_preserves_history(spark, tmp_path, format_version, operation):
    name = "cow_file_history"
    path = tmp_path / name
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    try:
        spark.sql(f"""
            CREATE TABLE {name} (id BIGINT, value BIGINT, part STRING)
            USING iceberg PARTITIONED BY (part) LOCATION '{path.as_uri()}'
            TBLPROPERTIES ('format-version' = '{format_version}', 'write.{operation}.mode' = 'copy-on-write')
        """)
        spark.sql(f"INSERT INTO {name} VALUES (1, 10, 'A'), (2, 20, 'A')")
        spark.sql(f"INSERT INTO {name} VALUES (3, 30, 'B'), (4, 40, 'B')")
        before = _find_latest_metadata(path)
        entries = _current_manifest_entries(path, ManifestContent.DATA)
        before_by_path = {entry.data_file.file_path: entry for entry in entries}
        affected = {
            file_path
            for file_path in before_by_path
            if 1 in pq.ParquetFile(_local_file_path(file_path)).read(columns=["id"]).column("id").to_pylist()
        }
        assert len(affected) == 1
        assert len(before_by_path) > len(affected)
        statements = {
            "delete": f"DELETE FROM {name} WHERE id = 1",
            "update": f"UPDATE {name} SET value = 100, part = 'C' WHERE id = 1",
            "merge": f"""MERGE INTO {name} AS t USING (SELECT 1L AS id) AS s ON t.id = s.id
                        WHEN MATCHED THEN UPDATE SET value = 100, part = 'C'""",
        }
        spark.sql(statements[operation]).collect()
        expected = [(2, 20, "A"), (3, 30, "B"), (4, 40, "B")]
        if operation != "delete":
            expected.insert(0, (1, 100, "C"))
        assert [tuple(row) for row in spark.sql(f"SELECT * FROM {name} ORDER BY id").collect()] == expected
        after = _find_latest_metadata(path)
        for key in (
            "table-uuid",
            "format-version",
            "schemas",
            "current-schema-id",
            "last-column-id",
            "partition-specs",
            "default-spec-id",
            "last-partition-id",
            "sort-orders",
            "default-sort-order-id",
        ):
            assert after.get(key) == before.get(key), key
        assert after["snapshots"][:-1] == before["snapshots"]
        snapshot = _current_snapshot(after)
        assert snapshot["parent-snapshot-id"] == before["current-snapshot-id"]
        assert snapshot["summary"]["operation"] == "overwrite"
        after_entries = _current_manifest_entries(path, ManifestContent.DATA)
        after_by_path = {entry.data_file.file_path: entry for entry in after_entries}
        assert set(before_by_path) - set(after_by_path) == affected
        assert set(after_by_path) - set(before_by_path)
        assert _current_manifest_entries(path, ManifestContent.DELETES) == []
        for file_path in set(before_by_path) & set(after_by_path):
            assert after_by_path[file_path].sequence_number == before_by_path[file_path].sequence_number
            assert after_by_path[file_path].file_sequence_number == before_by_path[file_path].file_sequence_number
        if format_version == 2:  # noqa: PLR2004
            assert after["last-sequence-number"] == before["last-sequence-number"] + 1
            assert after["refs"]["main"]["snapshot-id"] == snapshot["snapshot-id"]
            for file_path in set(after_by_path) - set(before_by_path):
                assert after_by_path[file_path].sequence_number == snapshot["sequence-number"]
        historical = (
            spark.read.format("iceberg").option("snapshotId", before["current-snapshot-id"]).load(path.as_uri())
        )
        assert [tuple(row) for row in historical.orderBy("id").collect()] == [
            (1, 10, "A"),
            (2, 20, "A"),
            (3, 30, "B"),
            (4, 40, "B"),
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("operation", ["delete", "update", "merge"])
@pytest.mark.parametrize("explicit_mode", [False, True], ids=["default", "explicit"])
def test_cow_empty_table_and_insert_only_merge(spark, tmp_path, operation, explicit_mode):
    name = "cow_empty"
    path = tmp_path / name
    mode_property = f"TBLPROPERTIES ('write.{operation}.mode' = 'copy-on-write')" if explicit_mode else ""
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    try:
        spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg LOCATION '{path.as_uri()}' {mode_property}")
        metadata_path = _latest_metadata_path(path)
        statements = {
            "delete": f"DELETE FROM {name} WHERE id = 1",
            "update": f"UPDATE {name} SET id = 2",
            "merge": f"""MERGE INTO {name} AS t USING (SELECT 1L AS id) AS s ON t.id = s.id
                        WHEN MATCHED THEN DELETE""",
        }
        spark.sql(statements[operation]).collect()
        assert _latest_metadata_path(path) == metadata_path
        assert list(path.rglob("*.parquet")) == []
        spark.sql(f"""MERGE INTO {name} AS t USING (SELECT 1L AS id) AS s ON t.id = s.id
                      WHEN NOT MATCHED THEN INSERT *""").collect()
        assert [tuple(row) for row in spark.table(name).collect()] == [(1,)]
        assert _current_snapshot(_find_latest_metadata(path))["summary"]["operation"] == "append"
        assert _current_manifest_entries(path, ManifestContent.DELETES) == []
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_cow_applies_existing_equality_and_position_deletes(spark, tmp_path):
    name = "cow_existing_deletes"
    path = tmp_path / name
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    try:
        spark.sql(f"""CREATE TABLE {name} (id BIGINT, value BIGINT) USING iceberg LOCATION '{path.as_uri()}'
                      TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read',
                                     'write.merge.mode' = 'merge-on-read')""")
        for rows in ["(1, 10), (2, 20)", "(3, 30), (4, 40)", "(5, 50), (6, 60)"]:
            spark.sql(f"INSERT INTO {name} VALUES {rows}")
        spark.sql(f"DELETE FROM {name} WHERE id IN (2, 3)").collect()
        spark.sql(f"""MERGE INTO {name} AS t USING (SELECT 4L AS id) AS s ON t.id = s.id
                      WHEN MATCHED THEN DELETE""").collect()
        deletes_before = {
            entry.data_file.file_path for entry in _current_manifest_entries(path, ManifestContent.DELETES)
        }
        assert len(deletes_before) == 2  # noqa: PLR2004
        spark.sql(f"UPDATE {name} SET id = 3 WHERE id = 1").collect()
        assert [tuple(row) for row in spark.sql(f"SELECT * FROM {name} ORDER BY id").collect()] == [
            (3, 10),
            (5, 50),
            (6, 60),
        ]
        deletes_after = {
            entry.data_file.file_path for entry in _current_manifest_entries(path, ManifestContent.DELETES)
        }
        assert deletes_after == deletes_before
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('write.delete.mode' = 'copy-on-write')")
        spark.sql(f"DELETE FROM {name} WHERE id = 5").collect()
        assert [tuple(row) for row in spark.sql(f"SELECT * FROM {name} ORDER BY id").collect()] == [(3, 10), (6, 60)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_cow_rewrites_complete_file_when_match_is_in_a_later_batch(spark, sql_catalog):
    name = "cow_later_batch"
    identifier = f"default.{name}"
    count = 9000
    table = sql_catalog.create_table(identifier, Schema(NestedField(1, "id", LongType(), required=False)))
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    try:
        table.append(pa.table({"id": pa.array(range(count), type=pa.int64())}))
        path = _local_file_path(table.location())
        before = {entry.data_file.file_path for entry in _current_manifest_entries(path, ManifestContent.DATA)}
        assert len(before) == 1
        spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{path.as_uri()}'")
        spark.sql(f"UPDATE {name} SET id = {count} WHERE id = {count - 1}").collect()
        assert [row.id for row in spark.table(name).orderBy("id").collect()] == [*range(count - 1), count]
        after = {entry.data_file.file_path for entry in _current_manifest_entries(path, ManifestContent.DATA)}
        assert before.isdisjoint(after)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        sql_catalog.drop_table(identifier)


@pytest.mark.parametrize("operation", ["delete", "update", "merge"])
@pytest.mark.parametrize(
    ("table_properties", "error"),
    [
        ("'format-version' = '3'", "row lineage"),
        ("'write.{operation}.mode' = 'invalid'", "Unknown Iceberg row-level operation mode"),
    ],
)
def test_cow_rejects_unsupported_mode_or_format_before_writing(spark, tmp_path, operation, table_properties, error):
    name = "cow_rejected"
    path = tmp_path / name
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    try:
        properties = table_properties.format(operation=operation)
        spark.sql(
            f"CREATE TABLE {name} (id BIGINT) USING iceberg LOCATION '{path.as_uri()}' TBLPROPERTIES ({properties})"
        )
        before = _latest_metadata_path(path)
        statements = {
            "delete": f"DELETE FROM {name}",
            "update": f"UPDATE {name} SET id = 2",
            "merge": f"""MERGE INTO {name} AS t USING (SELECT 1L AS id) AS s ON t.id = s.id
                        WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT *""",
        }
        with pytest.raises(Exception, match=error):
            spark.sql(statements[operation]).collect()
        assert _latest_metadata_path(path) == before
        assert list(path.rglob("*.parquet")) == []
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("operation", ["delete", "update", "merge"])
def test_cow_preserves_evolved_schema_and_writes_current_partition_spec(spark, sql_catalog, operation):
    from pyiceberg.transforms import BucketTransform
    from pyiceberg.types import StringType

    name = "cow_evolution"
    identifier = f"default.{name}"
    table = sql_catalog.create_table(
        identifier,
        Schema(NestedField(1, "id", LongType(), required=False), NestedField(2, "value", LongType(), required=False)),
    )
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    try:
        table.append(pa.table({"id": [1, 2], "value": [10, 20]}))
        with table.update_schema() as update:
            update.rename_column("value", "amount").add_column("extra", StringType())
        with table.update_spec() as update:
            update.add_field("id", BucketTransform(4))
        table.append(pa.table({"id": [3, 4], "amount": [30, 40], "extra": ["three", "four"]}))
        path = _local_file_path(table.location())
        before = _find_latest_metadata(path)
        before_entries = _current_manifest_entries(path, ManifestContent.DATA)
        before_paths = {entry.data_file.file_path for entry in before_entries}
        spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{path.as_uri()}'")
        statements = {
            "delete": f"DELETE FROM {name} WHERE id = 1",
            "update": f"UPDATE {name} SET id = 9, amount = amount + 100, extra = 'nine' WHERE id = 1",
            "merge": f"""MERGE INTO {name} AS t USING (SELECT 1L AS id) AS s ON t.id = s.id
                        WHEN MATCHED THEN UPDATE SET id = 9, amount = t.amount + 100, extra = 'nine'""",
        }
        spark.sql(statements[operation]).collect()
        expected = [(2, 20, None), (3, 30, "three"), (4, 40, "four")]
        if operation != "delete":
            expected.append((9, 110, "nine"))
        assert [tuple(row) for row in spark.table(name).orderBy("id").collect()] == expected
        after = _find_latest_metadata(path)
        assert after["schemas"] == before["schemas"]
        assert after["partition-specs"] == before["partition-specs"]
        assert after["current-schema-id"] == before["current-schema-id"]
        assert after["default-spec-id"] == before["default-spec-id"]
        assert _current_snapshot(after)["schema-id"] == before["current-schema-id"]
        added_entries = [
            entry
            for entry in _current_manifest_entries(path, ManifestContent.DATA)
            if entry.data_file.file_path not in before_paths
        ]
        assert added_entries
        for entry in added_entries:
            assert entry.data_file.spec_id == before["default-spec-id"]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        sql_catalog.drop_table(identifier)
