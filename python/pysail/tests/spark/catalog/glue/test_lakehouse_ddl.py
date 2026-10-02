"""Lakehouse DDL with Glue schema and metadata-pointer verification."""

import pytest
from pyiceberg.table import StaticTable

from pysail.testing.spark.ddl import exercise_lakehouse_alter
from pysail.tests.spark.catalog.glue.test_iceberg_commit import _glue_client


@pytest.mark.parametrize("fmt", ["delta", "iceberg"])
def test_glue_lakehouse_ddl(spark, tmp_path, moto_endpoint, fmt):
    table = f"ddl_{fmt}"
    name = f"test_db.{table}"
    location = (tmp_path / fmt).as_uri()
    properties = "'format-version'='3'" if fmt == "iceberg" else "'delta.enableTypeWidening'='true'"
    try:
        spark.sql(
            f"CREATE TABLE {name} (id INT, value STRING, part STRING) USING {fmt} "
            f"PARTITIONED BY (part) LOCATION '{location}' TBLPROPERTIES ({properties})"
        )
        exercise_lakehouse_alter(spark, name)
        stored = _glue_client(moto_endpoint).get_table(DatabaseName="test_db", Name=table)["Table"]
        assert next(c["Type"] for c in stored["StorageDescriptor"]["Columns"] if c["Name"] == "id") == "bigint"
        assert "custom" not in stored["Parameters"]
        if fmt == "iceberg":
            assert stored.get("PartitionKeys", []) == []
            reference = StaticTable.from_metadata(stored["Parameters"]["metadata_location"])
            assert str(reference.schema().find_field("id").field_type) == "long"
            assert sorted(reference.scan().to_arrow().to_pylist(), key=lambda r: r["id"]) == [
                {"id": 1, "value": "one", "part": "a"},
                {"id": 2, "value": "fallback", "part": "b"},
                {"id": 3, "value": None, "part": "c"},
            ]
        spark.sql(f"DROP TABLE {name}")
        spark.sql(f"CREATE TABLE {name} USING {fmt} LOCATION '{location}'")
        assert spark.table(name).count() == 3  # noqa: PLR2004
        assert [r.col_name for r in spark.sql(f"DESCRIBE TABLE {name}").collect()][:3] == ["id", "value", "part"]
        spark.sql(f"DROP TABLE {name}")
        ctas_location = (tmp_path / f"ctas_{fmt}").as_uri()
        spark.sql(f"CREATE TABLE {name} USING {fmt} LOCATION '{ctas_location}' AS SELECT 4 AS id")
        assert spark.table(name).first().id == 4  # noqa: PLR2004
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
