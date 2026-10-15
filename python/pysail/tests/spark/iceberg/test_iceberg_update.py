# ruff: noqa: S608

import pyarrow.parquet as pq
import pytest
from pyiceberg.manifest import DataFileContent, ManifestContent
from pyiceberg.table import StaticTable

from pysail.testing.spark.steps.iceberg import _current_snapshot, _find_latest_metadata, _latest_metadata_path
from pysail.tests.spark.iceberg.test_iceberg_merge import _current_manifest_entries, _local_file_path


def _read_position_deletes(path, original_files, granularity, sequence_number):
    positions = set()
    for entry in _current_manifest_entries(path, ManifestContent.DELETES):
        assert entry.data_file.content == DataFileContent.POSITION_DELETES
        assert entry.data_file.file_format == "PARQUET"
        assert entry.sequence_number == sequence_number
        delete_file = pq.ParquetFile(_local_file_path(entry.data_file.file_path))
        assert delete_file.schema_arrow.names == ["file_path", "pos"]
        deleted_rows = delete_file.read().to_pylist()
        if granularity == "file":
            assert len({row["file_path"] for row in deleted_rows}) == 1
        for row in deleted_rows:
            assert entry.data_file.partition == original_files[row["file_path"]].data_file.partition
            positions.add((row["file_path"], row["pos"]))
    return positions


@pytest.mark.parametrize("partitioned", [False, True], ids=["unpartitioned", "partitioned"])
@pytest.mark.parametrize("granularity", ["file", "partition"])
def test_v2_mor_update_preserves_files_positions_and_history(spark, tmp_path, partitioned, granularity):
    name = "v2_mor_update"
    path = tmp_path / name
    partition_clause = "PARTITIONED BY (part)" if partitioned else ""
    try:
        spark.sql(f"""CREATE TABLE {name} (id INT, value INT, part STRING) USING iceberg
            {partition_clause} LOCATION '{path.as_uri()}' TBLPROPERTIES (
                'format-version' = '2', 'write.update.mode' = 'merge-on-read',
                'write.delete.granularity' = '{granularity}')""")
        spark.sql(f"""INSERT INTO {name} SELECT /*+ COALESCE(1) */ * FROM VALUES
            (1, 10, 'A'), (2, 20, 'A'), (1, 10, 'A'), (3, 30, NULL)""")
        spark.sql(f"INSERT INTO {name} SELECT /*+ COALESCE(1) */ * FROM VALUES (4, 40, 'A'), (5, 50, 'B')")
        before = _find_latest_metadata(path)
        original_rows = spark.table(name).orderBy("id").collect()
        original_files = {
            entry.data_file.file_path: entry for entry in _current_manifest_entries(path, ManifestContent.DATA)
        }
        expected_deletes = set()
        for file_path in original_files:
            rows = pq.ParquetFile(_local_file_path(file_path)).read().to_pylist()
            expected_deletes.update((file_path, pos) for pos, row in enumerate(rows) if row["id"] in (1, 3, 4))

        spark.sql(f"UPDATE {name} SET id = value, value = id, part = 'C' WHERE id IN (1, 3, 4)").collect()
        expected = [(2, 20, "A"), (5, 50, "B"), (10, 1, "C"), (10, 1, "C"), (30, 3, "C"), (40, 4, "C")]
        assert [tuple(row) for row in spark.table(name).orderBy("id").collect()] == expected
        after = _find_latest_metadata(path)
        assert after["format-version"] == before["format-version"]
        assert after["snapshots"][:-1] == before["snapshots"]
        snapshot = _current_snapshot(after)
        assert snapshot["parent-snapshot-id"] == before["current-snapshot-id"]
        assert snapshot["sequence-number"] == before["last-sequence-number"] + 1
        assert snapshot["summary"]["operation"] == "overwrite"
        assert int(snapshot["summary"]["added-position-deletes"]) == len(expected_deletes)
        assert "added-dvs" not in snapshot["summary"]
        current_files = {
            entry.data_file.file_path: entry for entry in _current_manifest_entries(path, ManifestContent.DATA)
        }
        assert original_files.keys() < current_files.keys()
        for file_path, entry in original_files.items():
            assert current_files[file_path] == entry

        actual_deletes = _read_position_deletes(path, original_files, granularity, snapshot["sequence-number"])
        assert actual_deletes == expected_deletes
        external = StaticTable.from_metadata(str(_latest_metadata_path(path)))
        assert [tuple(row.values()) for row in external.scan().to_arrow().sort_by("id").to_pylist()] == expected
        historical = (
            spark.read.format("iceberg").option("snapshotId", before["current-snapshot-id"]).load(path.as_uri())
        )
        assert historical.orderBy("id").collect() == original_rows

        # Update both surviving original rows and newly written rows after position deletes.
        spark.sql(f"UPDATE {name} SET value = value + 100, part = NULL WHERE id IN (2, 10)").collect()
        expected = [(2, 120, None), (5, 50, "B"), (10, 101, None), (10, 101, None), (30, 3, "C"), (40, 4, "C")]
        assert [tuple(row) for row in spark.table(name).orderBy("id").collect()] == expected
        external = StaticTable.from_metadata(str(_latest_metadata_path(path)))
        assert [tuple(row.values()) for row in external.scan().to_arrow().sort_by("id").to_pylist()] == expected
        files = set(path.rglob("*.parquet"))
        spark.sql(f"UPDATE {name} SET value = 0 WHERE id = -1").collect()
        assert set(path.rglob("*.parquet")) == files
        assert [tuple(row) for row in spark.table(name).orderBy("id").collect()] == expected
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_v2_mor_update_empty_table_and_all_rows(spark, tmp_path):
    name = "v2_mor_update_all"
    path = tmp_path / name
    try:
        spark.sql(f"""CREATE TABLE {name} (id INT, value INT) USING iceberg LOCATION '{path.as_uri()}'
            TBLPROPERTIES ('format-version' = '2', 'write.update.mode' = 'merge-on-read')""")
        spark.sql(f"UPDATE {name} SET value = 0").collect()
        assert spark.table(name).collect() == []
        assert list(path.rglob("*.parquet")) == []
        spark.sql(f"INSERT INTO {name} VALUES (1, 10), (2, NULL), (3, 30)")
        spark.sql(f"UPDATE {name} SET id = id + 10, value = id").collect()
        assert [tuple(row) for row in spark.table(name).orderBy("id").collect()] == [(11, 1), (12, 2), (13, 3)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_v1_mor_update_rejected_without_side_effects(spark, tmp_path):
    name = "v1_mor_update"
    path = tmp_path / name
    try:
        spark.sql(f"""CREATE TABLE {name} (id INT) USING iceberg LOCATION '{path.as_uri()}'
            TBLPROPERTIES ('format-version' = '1', 'write.update.mode' = 'merge-on-read')""")
        spark.sql(f"INSERT INTO {name} VALUES (1)")
        before = _find_latest_metadata(path)
        files = set(path.rglob("*.parquet"))
        with pytest.raises(Exception, match="position delete writes require table format-version 2"):
            spark.sql(f"UPDATE {name} SET id = 2").collect()
        assert _find_latest_metadata(path) == before
        assert set(path.rglob("*.parquet")) == files
        assert [tuple(row) for row in spark.table(name).collect()] == [(1,)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
