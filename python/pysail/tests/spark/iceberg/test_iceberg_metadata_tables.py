import pytest

from pysail.testing.spark.utils.sql import escape_sql_string_literal

EXPECTED_SNAPSHOT_COUNT = 2


def test_iceberg_snapshots_metadata_table(spark, tmp_path):
    table = "sail.default.iceberg_metadata_snapshots"
    location = escape_sql_string_literal((tmp_path / "iceberg_metadata_snapshots").as_uri())
    spark.sql(f"CREATE TABLE {table} (id BIGINT) USING iceberg LOCATION '{location}'")
    try:
        spark.sql(f"INSERT INTO {table} VALUES (1)")  # noqa: S608
        spark.sql(f"INSERT INTO {table} VALUES (2)")  # noqa: S608

        rows_sql = f"SELECT id FROM {table} ORDER BY id"  # noqa: S608
        assert [row.id for row in spark.sql(rows_sql).collect()] == [1, 2]

        snapshots_sql = f"SELECT * FROM {table}.snapshots ORDER BY committed_at"  # noqa: S608
        snapshots = spark.sql(snapshots_sql)
        assert snapshots.schema.simpleString() == (
            "struct<committed_at:timestamp,snapshot_id:bigint,parent_id:bigint,operation:string,"
            "manifest_list:string,summary:map<string,string>>"
        )
        assert [(field.name, field.nullable) for field in snapshots.schema.fields] == [
            ("committed_at", False),
            ("snapshot_id", False),
            ("parent_id", True),
            ("operation", True),
            ("manifest_list", True),
            ("summary", True),
        ]
        assert snapshots.schema["summary"].dataType.valueContainsNull is False
        rows = snapshots.collect()
        assert len(rows) == EXPECTED_SNAPSHOT_COUNT
        assert rows[0].committed_at <= rows[1].committed_at
        assert rows[0].parent_id is None
        assert rows[1].parent_id == rows[0].snapshot_id
        assert [row.operation for row in rows] == ["append", "append"]

        latest_sql = f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"  # noqa: S608
        latest = spark.sql(latest_sql).collect()
        assert len(latest) == 1
        assert latest[0].snapshot_id == rows[-1].snapshot_id

        # Iceberg's snapshots metadata table is a static list of every known
        # snapshot, so scan time travel deliberately does not filter its rows.
        historical_sql = (
            f"SELECT snapshot_id FROM {table}.snapshots VERSION AS OF {rows[0].snapshot_id} ORDER BY committed_at"  # noqa: S608
        )
        historical = spark.sql(historical_sql).collect()
        assert [row.snapshot_id for row in historical] == [row.snapshot_id for row in rows]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_iceberg_unsupported_metadata_table(spark, tmp_path):
    table = "sail.default.iceberg_metadata_unsupported"
    location = escape_sql_string_literal((tmp_path / "iceberg_metadata_unsupported").as_uri())
    spark.sql(f"CREATE TABLE {table} (id BIGINT) USING iceberg LOCATION '{location}'")
    try:
        with pytest.raises(Exception, match="unsupported Iceberg metadata table: history"):
            spark.sql(f"SELECT * FROM {table}.history").collect()  # noqa: S608
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_snapshots_metadata_table_rejects_non_iceberg_table(spark):
    table = "sail.default.non_iceberg_metadata_snapshots"
    spark.sql(f"CREATE TABLE {table} (id BIGINT) USING parquet")
    try:
        with pytest.raises(Exception, match=r"Iceberg metadata table 'snapshots'.*non-Iceberg table"):
            spark.sql(f"SELECT * FROM {table}.snapshots").collect()  # noqa: S608
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_snapshots_metadata_table_rejects_persistent_view(spark):
    view = "sail.default.non_iceberg_metadata_view"
    spark.sql(f"CREATE VIEW {view} AS SELECT 1 AS id")
    try:
        with pytest.raises(Exception, match=r"Iceberg metadata table 'snapshots'.*non-Iceberg view"):
            spark.sql(f"SELECT * FROM {view}.snapshots").collect()  # noqa: S608
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {view}")
