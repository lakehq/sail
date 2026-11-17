# ruff: noqa: S608
"""Expiration of legacy filesystem metadata without a table UUID."""

from pysail.testing._dev.spark.steps.iceberg import _find_latest_metadata
from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests._dev.spark.iceberg.test_iceberg_expire_snapshots import FUTURE, ZERO, expire
from pysail.tests._dev.spark.iceberg.test_iceberg_merge import _local_file_path
from pysail.tests._dev.spark.iceberg.test_iceberg_procedures import _edit_latest_metadata


def test_expiration_initializes_legacy_uuid_before_cleanup(spark, tmp_path):
    name = "expire_legacy_uuid"
    path = tmp_path / name
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT) USING iceberg "
        f"LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES ('format-version'='1')"
    )
    try:
        for value in [1, 2]:
            spark.sql(f"INSERT OVERWRITE {name} VALUES ({value})")
        current_files = {_local_file_path(row.file_path) for row in spark.table(f"{name}.files").collect()}
        obsolete = set(path.rglob("*.parquet")) - current_files
        assert obsolete
        _edit_latest_metadata(path, lambda metadata: metadata.pop("table-uuid", None))
        before = _find_latest_metadata(path)
        assert "table-uuid" not in before

        counts = expire(spark, name, f", older_than => {FUTURE}, retain_last => 1")
        assert counts[0] == len(obsolete)
        assert counts[4] == len(before["snapshots"]) - 1
        assert all(not file.exists() for file in obsolete)
        assert all(file.exists() for file in current_files)
        after = _find_latest_metadata(path)
        assert after["table-uuid"]
        assert after["snapshots"] == [before["snapshots"][-1]]
        assert [row.id for row in spark.read.format("iceberg").load(str(path)).collect()] == [2]
        assert expire(spark, name, f", older_than => {FUTURE}") == ZERO
        assert _find_latest_metadata(path)["table-uuid"] == after["table-uuid"]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
