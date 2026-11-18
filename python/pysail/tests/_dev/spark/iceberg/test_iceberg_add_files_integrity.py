import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def test_add_files_preserves_filename_with_space(spark, tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    pq.write_table(pa.table({"id": [7]}), source / "with space.parquet")
    pq.write_table(pa.table({"id": [8]}), source / "with%20space.parquet")
    spark.sql(f"CREATE TABLE space_target (id BIGINT) USING iceberg LOCATION '{tmp_path / 'target'}'")
    try:
        spark.sql(f"CALL system.add_files('space_target', 'parquet.`{source}`')").collect()
        assert [row.id for row in spark.table("space_target").orderBy("id").collect()] == [7, 8]
    finally:
        spark.sql("DROP TABLE space_target")


def test_add_files_preserves_unknown_parent_totals(spark, tmp_path):
    from pysail.testing._dev.spark.steps.iceberg import (
        _find_latest_metadata,
        _latest_metadata_path,
        _write_metadata_file,
    )

    source = tmp_path / "source"
    source.mkdir()
    pq.write_table(pa.table({"id": [3]}), source / "data.parquet")
    target = tmp_path / "target"
    spark.sql(f"CREATE TABLE totals_target (id BIGINT) USING iceberg LOCATION '{target}'")
    try:
        spark.sql("INSERT INTO totals_target VALUES (1), (2)")
        metadata = _find_latest_metadata(target)
        metadata["snapshots"][-1]["summary"].pop("total-records", None)
        _write_metadata_file(_latest_metadata_path(target), metadata)
        spark.sql(f"CALL system.add_files('totals_target', 'parquet.`{source}`')").collect()
        total = _find_latest_metadata(target)["snapshots"][-1]["summary"].get("total-records")
        assert total in (None, "3")
    finally:
        spark.sql("DROP TABLE totals_target")


def test_add_files_source_directory(spark, tmp_path):
    source = tmp_path / "source"
    staging = source / "staging"
    staging.mkdir(parents=True)
    pq.write_table(pa.table({"id": [1]}), source / "data.parquet")
    pq.write_table(pa.table({"id": [2]}), staging / "extra.parquet")
    spark.sql(f"CREATE TABLE directory_target (id BIGINT) USING iceberg LOCATION '{tmp_path / 'target'}'")
    try:
        with pytest.raises(Exception, match=r"No such file|not found|cannot find"):
            spark.sql(f"CALL system.add_files('directory_target', 'parquet.`{tmp_path / 'missing'}`')").collect()
        assert spark.table("directory_target.snapshots").count() == 0
        result = spark.sql(f"CALL system.add_files('directory_target', 'parquet.`{source}`')").first()
        assert tuple(result) == (1, None)
        assert [row.id for row in spark.table("directory_target").collect()] == [1]
    finally:
        spark.sql("DROP TABLE directory_target")


def test_expire_imported_file_preserves_percent_named_neighbor(spark, tmp_path):
    from pysail.tests._dev.spark.iceberg.test_iceberg_expire_snapshots import FUTURE, expire

    source = tmp_path / "with space.parquet"
    neighbor = tmp_path / "with%20space.parquet"
    pq.write_table(pa.table({"id": [1]}), source)
    neighbor.write_bytes(b"unreferenced neighbor")
    spark.sql(f"CREATE TABLE expire_import (id BIGINT) USING iceberg LOCATION '{tmp_path / 'target'}'")
    try:
        spark.sql(f"CALL system.add_files('expire_import', 'parquet.`{source}`')").collect()
        spark.sql("DELETE FROM expire_import")
        assert expire(spark, "expire_import", f", older_than => {FUTURE}, retain_last => 1")[0] == 1
        assert neighbor.read_bytes() == b"unreferenced neighbor"
        assert not source.exists()
    finally:
        spark.sql("DROP TABLE expire_import")


def test_first_import_after_v3_upgrade_assigns_parent_lineage(spark, tmp_path):
    from pysail.testing._dev.spark.steps.iceberg import _current_row_lineage, _find_latest_metadata

    source = tmp_path / "import.parquet"
    target = tmp_path / "target"
    pq.write_table(pa.table({"id": [3]}), source)
    spark.sql(
        f"CREATE TABLE upgraded_import (id BIGINT) USING iceberg LOCATION '{target}' "
        "TBLPROPERTIES ('format-version'='2')"
    )
    try:
        spark.sql("INSERT INTO upgraded_import SELECT /*+ COALESCE(1) */ * FROM VALUES (1), (2)")
        spark.sql("ALTER TABLE upgraded_import SET TBLPROPERTIES ('format-version'='3')")
        spark.sql(f"CALL system.add_files('upgraded_import', 'parquet.`{source}`')").collect()
        metadata = _find_latest_metadata(target)
        snapshot = metadata["snapshots"][-1]
        lineage = _current_row_lineage(target)
        assert snapshot["added-rows"] == len(lineage)
        assert metadata["next-row-id"] == snapshot["first-row-id"] + snapshot["added-rows"]
        assert {lineage[i][0] for i in (1, 2)} == {0, 1}
        assert lineage[3][0] == max(lineage[i][0] for i in (1, 2)) + 1
        spark.sql("INSERT INTO upgraded_import VALUES (4)")
        assert _current_row_lineage(target)[4][0] == metadata["next-row-id"]
    finally:
        spark.sql("DROP TABLE upgraded_import")
