import json

import pytest

from pysail.testing.spark.ddl import exercise_iceberg_identifier_fields


def metadata(path):
    version = (path / "metadata" / "version-hint.text").read_text().strip()
    return json.loads((path / "metadata" / f"v{version}.metadata.json").read_text())


def current_schema(value):
    return next(s for s in value["schemas"] if s["schema-id"] == value["current-schema-id"])


def test_nested_identifier_fields(spark, tmp_path):
    table = "nested_identifiers"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT NOT NULL, nested STRUCT<key: INT NOT NULL> NOT NULL) "
            f"USING iceberg LOCATION '{tmp_path.as_uri()}'"
        )
        before = current_schema(metadata(tmp_path))
        field_id = before["fields"][1]["type"]["fields"][0]["id"]
        spark.sql(f"ALTER TABLE {table} SET IDENTIFIER FIELDS id, nested.key")
        assert set(current_schema(metadata(tmp_path))["identifier-field-ids"]) == {before["fields"][0]["id"], field_id}
        spark.sql(f"ALTER TABLE {table} DROP IDENTIFIER FIELDS id")
        assert current_schema(metadata(tmp_path))["identifier-field-ids"] == [field_id]
        spark.sql(f"ALTER TABLE {table} RENAME COLUMN nested.key TO renamed")
        assert current_schema(metadata(tmp_path))["identifier-field-ids"] == [field_id]
        spark.sql(f"ALTER TABLE {table} DROP IDENTIFIER FIELDS nested.renamed")
        assert not current_schema(metadata(tmp_path)).get("identifier-field-ids")
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("version", [1, 2, 3])
def test_identifier_evolution(spark, tmp_path, version):
    table = "identifier_evolution"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT NOT NULL, key INT NOT NULL, value STRING) "
            f"USING iceberg LOCATION '{tmp_path.as_uri()}' TBLPROPERTIES ('format-version'='{version}')"
        )
        before = metadata(tmp_path)
        exercise_iceberg_identifier_fields(spark, table)
        after = metadata(tmp_path)
        initial = current_schema(before)
        current = current_schema(after)
        field_id = initial["fields"][0]["id"]
        assert current["identifier-field-ids"] == [field_id]
        assert current["fields"][0]["id"] == field_id
        assert after["last-column-id"] == before["last-column-id"]
        assert initial in after["schemas"]
        assert {tuple(sorted(s.get("identifier-field-ids", []))) for s in after["schemas"]} >= {
            (),
            (field_id,),
            (initial["fields"][1]["id"],),
        }
        for operation in ["ALTER COLUMN renamed DROP NOT NULL", "DROP COLUMN renamed"]:
            with pytest.raises(Exception, match="identifier"):
                spark.sql(f"ALTER TABLE {table} {operation}")
            assert metadata(tmp_path) == after
        spark.sql(f"ALTER TABLE {table} DROP IDENTIFIER FIELDS renamed")
        assert not current_schema(metadata(tmp_path)).get("identifier-field-ids")
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN renamed DROP NOT NULL")
        spark.sql(f"INSERT INTO {table} SELECT NULL, 30, 'nullable'")
        assert spark.table(table).count() == 3
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize(
    "operation,error",
    [
        ("SET IDENTIFIER FIELDS missing", "not found"),
        ("SET IDENTIFIER FIELDS optional", "optional"),
        ("SET IDENTIFIER FIELDS f", "float or double"),
        ("SET IDENTIFIER FIELDS required_struct", "primitive"),
        ("SET IDENTIFIER FIELDS nested.key", "optional"),
        ("SET IDENTIFIER FIELDS items.key", "nested"),
        ("DROP IDENTIFIER FIELDS missing", "not found"),
        ("DROP IDENTIFIER FIELDS optional", "not an identifier"),
        ("DROP IDENTIFIER FIELDS id, id", "not an identifier"),
    ],
)
def test_invalid_identifiers_are_atomic(spark, tmp_path, operation, error):
    table = "identifier_invalid"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT NOT NULL, optional STRING, f DOUBLE NOT NULL, "
            "required_struct STRUCT<key: INT NOT NULL> NOT NULL, nested STRUCT<key: INT NOT NULL>, items ARRAY<STRUCT<key: INT NOT NULL>> NOT NULL) "
            f"USING iceberg LOCATION '{tmp_path.as_uri()}'"
        )
        spark.sql(f"ALTER TABLE {table} SET IDENTIFIER FIELDS id")
        before = metadata(tmp_path)
        with pytest.raises(Exception, match=error):
            spark.sql(f"ALTER TABLE {table} {operation}")
        assert metadata(tmp_path) == before
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
