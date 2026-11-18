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
