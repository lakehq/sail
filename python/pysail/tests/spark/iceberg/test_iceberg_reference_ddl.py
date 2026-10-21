# ruff: noqa: S608
import pytest

from pysail.testing.spark.steps.iceberg import _find_latest_metadata


@pytest.mark.parametrize("version", [1, 2, 3])
def test_reference_lifecycle_preserves_data_and_history(spark, tmp_path, version):
    table = "reference_lifecycle"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id BIGINT) USING iceberg LOCATION '{tmp_path.as_uri()}' "
            f"TBLPROPERTIES ('format-version'='{version}')"
        )
        spark.sql(f"INSERT INTO {table} VALUES (1)")
        first = _find_latest_metadata(tmp_path)["current-snapshot-id"]
        spark.sql(f"INSERT INTO {table} VALUES (2)")
        before = _find_latest_metadata(tmp_path)
        second = before["current-snapshot-id"]
        spark.sql(
            f"ALTER TABLE {table} CREATE BRANCH audit AS OF VERSION {first} "
            "RETAIN 7 DAYS WITH SNAPSHOT RETENTION 2 SNAPSHOTS 3 HOURS"
        )
        spark.sql(f"ALTER TABLE {table} CREATE TAG release AS OF VERSION {first} RETAIN 10 MINUTES")
        created = _find_latest_metadata(tmp_path)
        assert created["refs"]["audit"] == {
            "snapshot-id": first,
            "type": "branch",
            "max-ref-age-ms": 7 * 24 * 3600 * 1000,
            "min-snapshots-to-keep": 2,
            "max-snapshot-age-ms": 3 * 3600 * 1000,
        }
        assert created["refs"]["release"] == {
            "snapshot-id": first,
            "type": "tag",
            "max-ref-age-ms": 10 * 60 * 1000,
        }
        assert created["snapshots"] == before["snapshots"]
        assert created["snapshot-log"] == before["snapshot-log"]
        assert created["current-snapshot-id"] == second
        assert [tuple(row) for row in spark.sql(f"SELECT * FROM {table} VERSION AS OF 'release'").collect()] == [(1,)]
        spark.sql(f"ALTER TABLE {table} CREATE BRANCH IF NOT EXISTS audit")
        spark.sql(f"ALTER TABLE {table} CREATE TAG IF NOT EXISTS release")
        assert _find_latest_metadata(tmp_path) == created
        spark.sql(f"ALTER TABLE {table} REPLACE BRANCH audit AS OF VERSION {second}")
        spark.sql(f"ALTER TABLE {table} CREATE OR REPLACE TAG release AS OF VERSION {second}")
        replaced = _find_latest_metadata(tmp_path)
        assert replaced["refs"]["audit"] == {**created["refs"]["audit"], "snapshot-id": second}
        assert replaced["refs"]["release"] == {**created["refs"]["release"], "snapshot-id": second}
        history = replaced["snapshot-log"]
        spark.sql(f"ALTER TABLE {table} REPLACE BRANCH main WITH SNAPSHOT RETENTION 2 HOURS")
        updated_history = _find_latest_metadata(tmp_path)["snapshot-log"]
        assert updated_history[:-1] == history
        assert updated_history[-1]["snapshot-id"] == second
        spark.sql(f"ALTER TABLE {table} REPLACE BRANCH main AS OF VERSION {first}")
        assert [tuple(row) for row in spark.table(table).collect()] == [(1,)]
        spark.sql(f"CALL system.fast_forward('{table}', 'main', 'audit')").collect()
        assert spark.table(table).count() == 2  # noqa: PLR2004
        spark.sql(f"ALTER TABLE {table} DROP BRANCH audit")
        spark.sql(f"ALTER TABLE {table} DROP TAG release")
        dropped = _find_latest_metadata(tmp_path)
        assert set(dropped["refs"]) == {"main"}
        spark.sql(f"ALTER TABLE {table} DROP BRANCH IF EXISTS audit")
        spark.sql(f"ALTER TABLE {table} DROP TAG IF EXISTS release")
        assert _find_latest_metadata(tmp_path) == dropped
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("version", [1, 2, 3])
def test_create_branch_on_empty_table(spark, tmp_path, version):
    table = "empty_reference"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT) USING iceberg LOCATION '{tmp_path.as_uri()}' "
            f"TBLPROPERTIES ('format-version'='{version}')"
        )
        with pytest.raises(Exception, match="main has no snapshot"):
            spark.sql(f"ALTER TABLE {table} CREATE TAG release")
        spark.sql(f"ALTER TABLE {table} CREATE BRANCH audit")
        metadata = _find_latest_metadata(tmp_path)
        assert metadata.get("current-snapshot-id") in (None, -1)
        assert "main" not in metadata.get("refs", {})
        assert metadata["refs"]["audit"]["snapshot-id"] == metadata["snapshots"][0]["snapshot-id"]
        assert metadata["snapshots"][0]["summary"]["operation"] == "append"
        assert spark.sql(f"SELECT * FROM {table} VERSION AS OF 'audit'").count() == 0
        if version == 3:  # noqa: PLR2004
            assert metadata["snapshots"][0]["first-row-id"] == 0
            assert metadata["snapshots"][0]["added-rows"] == 0
            assert metadata["next-row-id"] == 0
        spark.sql(f"ALTER TABLE {table} CREATE BRANCH IF NOT EXISTS audit")
        assert _find_latest_metadata(tmp_path) == metadata
        with pytest.raises(Exception, match="main has no snapshot"):
            spark.sql(f"ALTER TABLE {table} REPLACE BRANCH audit")
        spark.sql(f"INSERT INTO {table} VALUES (1)")
        assert spark.sql(f"SELECT * FROM {table} VERSION AS OF 'audit'").count() == 0
        assert spark.table(table).count() == 1
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize(
    ("operation", "error"),
    [
        ("CREATE BRANCH audit", "already exists"),
        ("CREATE TAG audit", "already exists"),
        ("REPLACE TAG audit", "different type"),
        ("DROP TAG audit", "different type"),
        ("CREATE OR REPLACE TAG audit", "different type"),
        ("REPLACE BRANCH missing", "does not exist"),
        ("DROP BRANCH missing", "does not exist"),
        ("DROP BRANCH main", "Cannot remove main"),
        ("CREATE BRANCH missing AS OF VERSION 0", "Cannot find"),
        ("CREATE BRANCH invalid RETAIN 0 DAYS", "greater than 0"),
        ("CREATE BRANCH invalid WITH SNAPSHOT RETENTION 0 SNAPSHOTS", "greater than 0"),
        ("CREATE OR REPLACE BRANCH IF NOT EXISTS invalid", "IF NOT EXISTS"),
        ("CREATE TAG invalid WITH SNAPSHOT RETENTION 2 SNAPSHOTS", "only supported for branches"),
        ("CREATE BRANCH invalid RETAIN 9223372036854775807 DAYS", "out of range"),
        ("CREATE BRANCH invalid WITH SNAPSHOT RETENTION 2147483648 SNAPSHOTS", "retention count"),
    ],
)
def test_invalid_reference_changes_are_atomic(spark, tmp_path, operation, error):
    table = "invalid_reference"
    try:
        spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg LOCATION '{tmp_path.as_uri()}'")
        spark.sql(f"INSERT INTO {table} VALUES (1)")
        spark.sql(f"ALTER TABLE {table} CREATE BRANCH audit")
        before = _find_latest_metadata(tmp_path)
        files = set(tmp_path.rglob("*"))
        with pytest.raises(Exception, match=error):
            spark.sql(f"ALTER TABLE {table} {operation}")
        assert _find_latest_metadata(tmp_path) == before
        assert set(tmp_path.rglob("*")) == files
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_quoted_references_and_retention_updates(spark, tmp_path):
    table = "quoted_references"
    try:
        spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg LOCATION '{tmp_path.as_uri()}'")
        spark.sql(f"INSERT INTO {table} VALUES (1)")
        spark.sql(f"ALTER TABLE {table} CREATE OR REPLACE BRANCH `Audit.V1` WITH SNAPSHOT RETENTION 2 HOURS")
        spark.sql(f"ALTER TABLE {table} CREATE OR REPLACE TAG `Release.V1`")
        spark.sql(f"ALTER TABLE {table} REPLACE BRANCH `Audit.V1` RETAIN 1 DAYS WITH SNAPSHOT RETENTION 3 SNAPSHOTS")
        metadata = _find_latest_metadata(tmp_path)
        assert metadata["refs"]["Audit.V1"] == {
            "snapshot-id": metadata["current-snapshot-id"],
            "type": "branch",
            "max-ref-age-ms": 86400000,
            "min-snapshots-to-keep": 3,
            "max-snapshot-age-ms": 7200000,
        }
        assert spark.sql(f"SELECT * FROM {table} VERSION AS OF 'Audit.V1'").count() == 1
        spark.sql(f"ALTER TABLE {table} DROP BRANCH `Audit.V1`")
        spark.sql(f"ALTER TABLE {table} DROP TAG `Release.V1`")
        assert set(_find_latest_metadata(tmp_path)["refs"]) == {"main"}
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("table_format", ["parquet", "delta"])
def test_references_reject_other_formats(spark, tmp_path, table_format):
    table = "non_iceberg_reference"
    try:
        spark.sql(f"CREATE TABLE {table} (id INT) USING {table_format} LOCATION '{tmp_path.as_uri()}'")
        with pytest.raises(Exception, match="require an Iceberg table"):
            spark.sql(f"ALTER TABLE {table} CREATE BRANCH audit")
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
