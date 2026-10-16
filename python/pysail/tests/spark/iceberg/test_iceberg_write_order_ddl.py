# ruff: noqa: S608
from urllib.parse import unquote, urlparse

import pyarrow.parquet as pq
import pytest
from pyiceberg.table import StaticTable

from pysail.testing.spark.ddl import exercise_iceberg_write_order
from pysail.testing.spark.steps.iceberg import _find_latest_metadata, _latest_metadata_path


def current_order(metadata):
    return next(order for order in metadata["sort-orders"] if order["order-id"] == metadata["default-sort-order-id"])


def test_write_order_metadata_and_data_files(spark, tmp_path):
    table = "write_order"
    try:
        spark.sql(f"CREATE TABLE {table} (id INT, value INT) USING iceberg LOCATION '{tmp_path.as_uri()}'")
        exercise_iceberg_write_order(spark, table)
        metadata = _find_latest_metadata(tmp_path)
        order = current_order(metadata)
        assert order["fields"] == [
            {"source-id": 1, "transform": "identity", "direction": "desc", "null-order": "nulls-last"},
            {"source-id": 2, "transform": "identity", "direction": "asc", "null-order": "nulls-first"},
        ]
        assert metadata["properties"]["write.distribution-mode"] == "range"
        # Inspect current data files, not removed files left behind by UPDATE.
        pointer = max((tmp_path / "metadata").glob("*.metadata.json"), key=lambda p: p.stat().st_mtime_ns)
        reference = StaticTable.from_metadata(str(pointer))
        for task in reference.scan().plan_files():
            assert task.file.sort_order_id == order["order-id"]
            rows = pq.ParquetFile(unquote(urlparse(task.file.file_path).path)).read().to_pylist()
            assert rows == sorted(rows, key=lambda r: (r["id"] is None, -(r["id"] or 0), r["value"]))
        spark.sql(f"ALTER TABLE {table} WRITE UNORDERED")
        assert current_order(_find_latest_metadata(tmp_path))["order-id"] == 0
        spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY id DESC NULLS LAST, value ASC")
        assert current_order(_find_latest_metadata(tmp_path))["order-id"] == order["order-id"]
        spark.sql(f"ALTER TABLE {table} WRITE LOCALLY ORDERED BY (value DESC)")
        local = _find_latest_metadata(tmp_path)
        assert local["properties"]["write.distribution-mode"] == "range"
        assert current_order(local)["fields"][0]["source-id"] == 2  # noqa: PLR2004
        spark.sql(f"ALTER TABLE {table} WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY bucket(4, id)")
        distributed = _find_latest_metadata(tmp_path)
        assert distributed["properties"]["write.distribution-mode"] == "hash"
        assert current_order(distributed)["fields"][0]["transform"] == "bucket[4]"
        spark.sql(f"INSERT INTO {table} VALUES (5, 5)")
        assert spark.table(table).count() == 5  # noqa: PLR2004
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("clause", ["ORDERED BY absent", "ORDERED BY bucket(0, id)", "UNORDERED ORDERED BY id"])
def test_invalid_write_order_is_atomic(spark, tmp_path, clause):
    table = "invalid_write_order"
    try:
        spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg LOCATION '{tmp_path.as_uri()}'")
        before = _find_latest_metadata(tmp_path)
        with pytest.raises(Exception, match=r"(?i)exist|bucket|duplicate|positive"):
            spark.sql(f"ALTER TABLE {table} WRITE {clause}")
        assert _find_latest_metadata(tmp_path) == before
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("mode", ["copy-on-write", "merge-on-read"])
@pytest.mark.parametrize("version", [2, 3])
def test_sorted_partitioned_table_row_level_operations(spark, tmp_path, mode, version):
    table = "sorted_dml"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, value INT) USING iceberg PARTITIONED BY (bucket(2, id)) "
            f"LOCATION '{tmp_path.as_uri()}' TBLPROPERTIES ('write.update.mode'='{mode}', "
            f"'write.delete.mode'='{mode}', 'write.merge.mode'='{mode}', 'format-version'='{version}')"
        )
        spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY id DESC NULLS LAST")
        spark.sql(f"INSERT INTO {table} VALUES (1, 1), (2, 2), (3, 3)")
        spark.sql(f"DELETE FROM {table} WHERE id=2")
        spark.sql(
            f"MERGE INTO {table} t USING (SELECT * FROM VALUES (1, 10), (4, 40) AS s(id, value)) s ON t.id=s.id "
            "WHEN MATCHED THEN UPDATE SET value=s.value WHEN NOT MATCHED THEN INSERT *"
        )
        spark.sql(f"UPDATE {table} SET value=30 WHERE id=3")
        assert [tuple(row) for row in spark.table(table).orderBy("id").collect()] == [(1, 10), (3, 30), (4, 40)]
        if version == 2 and mode == "merge-on-read":  # noqa: PLR2004
            reference = StaticTable.from_metadata(str(_latest_metadata_path(tmp_path)))
            assert sorted(reference.scan().to_arrow().to_pylist(), key=lambda row: row["id"]) == [
                {"id": 1, "value": 10},
                {"id": 3, "value": 30},
                {"id": 4, "value": 40},
            ]
            assert any(task.delete_files for task in reference.scan().plan_files())
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
