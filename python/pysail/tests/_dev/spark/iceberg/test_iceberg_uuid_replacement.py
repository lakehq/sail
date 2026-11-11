"""UUID identity survives catalog replacement, persisted output, and reopening."""

# ruff: noqa: S608

from uuid import UUID

import pyarrow.parquet as pq
import pytest
from pyiceberg.manifest import ManifestContent

from pysail.testing._dev.spark.steps.iceberg import _find_latest_metadata
from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests._dev.spark.iceberg.test_iceberg_uuid_constructors import _normalize, _shape, _to_arrow
from pysail.tests._dev.spark.iceberg.utils import _current_manifest_entries, _local_file_path


@pytest.mark.parametrize("api", ["sql", "dataframe"])
def test_uuid_replacement_preserves_catalog_and_storage_identity(spark, tmp_path, api):
    name = "uuid_replacement_identity"
    value = "6f1c2d3e-4b5a-4c6d-8e7f-0123456789ab"
    location = escape_sql_string_literal(str(tmp_path))
    expected = [{"u": UUID(value).bytes, "a": [UUID(value).bytes, None], "b": b"plain"}]
    try:
        spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{location}' AS SELECT 1 AS old")
        before = _find_latest_metadata(tmp_path)
        source = spark.range(1, numPartitions=2).selectExpr(
            f"CAST('{value}' AS UUID) AS u",
            f"array(CAST('{value}' AS UUID), NULL) AS a",
            "CAST('plain' AS BINARY) AS b",
        )
        if api == "sql":
            source.createOrReplaceTempView("uuid_replacement_source")
            spark.sql(f"REPLACE TABLE {name} USING iceberg AS SELECT * FROM uuid_replacement_source").collect()
        else:
            source.writeTo(name).using("iceberg").replace()
        after = _find_latest_metadata(tmp_path)
        assert after["table-uuid"] == before["table-uuid"]
        assert after["current-snapshot-id"] != before["current-snapshot-id"]
        schema = next(s for s in after["schemas"] if s["schema-id"] == after["current-schema-id"])
        assert {f["name"]: _shape(f["type"]) for f in schema["fields"]} == {
            "u": "uuid",
            "a": ("list", "uuid"),
            "b": "binary",
        }
        entries = _current_manifest_entries(tmp_path, ManifestContent.DATA)
        assert entries
        artifact_rows = []
        for entry in entries:
            parquet = pq.ParquetFile(_local_file_path(entry.data_file.file_path))
            assert parquet.schema_arrow.names == ["u", "a", "b"]
            assert [parquet.schema.column(i).logical_type.type for i in range(3)] == ["UUID", "UUID", "NONE"]
            assert [parquet.schema.column(i).physical_type for i in range(3)] == [
                "FIXED_LEN_BYTE_ARRAY",
                "FIXED_LEN_BYTE_ARRAY",
                "BYTE_ARRAY",
            ]
            artifact_rows.extend(parquet.read().to_pylist())
        assert _normalize(artifact_rows) == expected
        assert _normalize(_to_arrow(spark.table(name)).to_pylist()) == expected
        assert _normalize(_to_arrow(spark.read.format("iceberg").load(str(tmp_path))).to_pylist()) == expected
        spark.sql(f"DROP TABLE {name}")
        spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{location}'")
        assert _normalize(_to_arrow(spark.table(name)).to_pylist()) == expected
        # Appending through the published/reopened definition must still preserve UUID.
        source.writeTo(name).append()
        assert _normalize(_to_arrow(spark.table(name)).to_pylist()) == expected * 2
    finally:
        spark.sql("DROP VIEW IF EXISTS uuid_replacement_source")
        spark.sql(f"DROP TABLE IF EXISTS {name}")
