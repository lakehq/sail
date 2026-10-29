# ruff: noqa: S608
import json

import pytest

from pysail.testing.spark.steps.iceberg import _find_latest_metadata


@pytest.fixture(params=["parquet", "delta", "iceberg"])
def comment_table(spark, tmp_path, request):
    fmt = request.param
    table = "default.`comment.target`"
    properties = "'delta.columnMapping.mode'='name', " if fmt == "delta" else ""
    spark.sql(
        f"CREATE TABLE {table} (id INT COMMENT 'original column', `a.b` STRING COMMENT 'unchanged') "
        f"USING {fmt} LOCATION '{tmp_path.as_uri()}' COMMENT 'original table' "
        f"TBLPROPERTIES ({properties}'custom'='preserved')"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1, 'old')")
    try:
        yield table, tmp_path, fmt
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def _storage_metadata(location, fmt):
    if fmt == "delta":
        metadata = None
        for log in sorted((location / "_delta_log").glob("*.json")):
            for line in log.read_text().splitlines():
                metadata = json.loads(line).get("metaData", metadata)
        return metadata
    if fmt == "iceberg":
        return _find_latest_metadata(location)
    return None


def _storage_schema(metadata, fmt):
    if fmt == "delta":
        return json.loads(metadata["schemaString"])
    return next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])


def test_comment_lifecycle(spark, comment_table):
    table, location, fmt = comment_table
    before = _storage_metadata(location, fmt)
    for value in [None, "updated comment", "", "NULL", None, None]:
        literal = "NULL" if value is None else f"'{value}'"
        spark.sql(f"COMMENT ON TABLE {table} IS {literal}")
        spark.sql(f"COMMENT ON COLUMN sail.{table}.id IS {literal}")

        description = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
        assert next((row.data_type for row in description if row.col_name == "Comment"), None) == value
        assert next(row.comment for row in description if row.col_name == "id") == value
        assert next(row.comment for row in description if row.col_name == "a.b") == "unchanged"
        assert spark.catalog.getTable(table).description == value
        assert next(column.description for column in spark.catalog.listColumns(table) if column.name == "id") == value
        assert [tuple(row) for row in spark.table(table).collect()] == [(1, "old")]

        metadata = _storage_metadata(location, fmt)
        if metadata is not None:
            schema = _storage_schema(metadata, fmt)
            original = _storage_schema(before, fmt)
            field = schema["fields"][0]
            if fmt == "delta":
                assert metadata.get("description") == value
                assert "comment" not in metadata["configuration"]
                assert metadata["configuration"]["custom"] == "preserved"
                assert field["metadata"].get("comment") == value
                assert {k: v for k, v in field["metadata"].items() if k != "comment"} == {
                    k: v for k, v in original["fields"][0]["metadata"].items() if k != "comment"
                }
            else:
                assert metadata["properties"].get("comment") == value
                assert metadata["properties"]["custom"] == "preserved"
                assert field.get("doc") == value
                assert field["id"] == original["fields"][0]["id"]
            assert schema["fields"][1] == original["fields"][1]


def test_comment_on_quoted_column_and_case_sensitive_resolution(spark, comment_table):
    table, location, fmt = comment_table
    key = "spark.sql.caseSensitive"
    previous = spark.conf.get(key)
    try:
        spark.conf.set(key, "false")
        spark.sql("COMMENT ON COLUMN `comment.target`.ID IS 'identifier'")
        spark.sql(f"COMMENT ON COLUMN {table}.`a.b` IS 'quoted column'")
        columns = {row.col_name: row.comment for row in spark.sql(f"DESCRIBE {table}").collect()}
        assert columns == {"id": "identifier", "a.b": "quoted column"}

        spark.conf.set(key, "true")
        before = _storage_metadata(location, fmt)
        with pytest.raises(Exception, match=r"(?i)does not exist|not found"):
            spark.sql(f"COMMENT ON COLUMN {table}.ID IS 'wrong case'")
        assert _storage_metadata(location, fmt) == before
        spark.sql(f"COMMENT ON COLUMN {table}.`a.b` IS NULL")
        assert next(row.comment for row in spark.sql(f"DESCRIBE {table}").collect() if row.col_name == "a.b") is None
    finally:
        spark.conf.set(key, previous)


def test_invalid_comment_target_preserves_metadata(spark, comment_table):
    table, location, fmt = comment_table
    before = _storage_metadata(location, fmt)
    for sql, error in [
        (f"COMMENT ON COLUMN {table}.missing IS NULL", "does not exist"),
        ("COMMENT ON COLUMN id IS 'unqualified'", "table-qualified column name"),
        ("COMMENT ON TABLE missing_comment_table IS NULL", "not found"),
        ("COMMENT ON COLUMN missing_comment_table.id IS NULL", "not found"),
    ]:
        with pytest.raises(Exception, match=error):
            spark.sql(sql)
        assert _storage_metadata(location, fmt) == before
    description = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
    assert next(row.data_type for row in description if row.col_name == "Comment") == "original table"
    assert next(row.comment for row in description if row.col_name == "id") == "original column"


@pytest.mark.parametrize("kind", ["VIEW", "TEMPORARY VIEW", "GLOBAL TEMPORARY VIEW"])
def test_comment_rejects_views(spark, tmp_path, kind):
    table = "default.comment_view"
    view = "global_temp.comment_view" if kind == "GLOBAL TEMPORARY VIEW" else "comment_view"
    try:
        if kind == "TEMPORARY VIEW":
            spark.sql(
                f"CREATE TABLE {table} (id INT COMMENT 'original column') USING parquet "
                f"LOCATION '{tmp_path.as_uri()}' COMMENT 'original table'"
            )
        spark.sql(f"CREATE {kind} comment_view AS SELECT 1 AS id")
        for target in [f"TABLE {view}", f"COLUMN {view}.id"]:
            with pytest.raises(Exception, match=r"requires a table.*is a view"):
                spark.sql(f"COMMENT ON {target} IS 'rejected'")
        if kind == "TEMPORARY VIEW":
            description = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
            assert next(row.data_type for row in description if row.col_name == "Comment") == "original table"
            assert next(row.comment for row in description if row.col_name == "id") == "original column"
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {view}")
        if kind == "TEMPORARY VIEW":
            spark.sql(f"DROP TABLE IF EXISTS {table}")
