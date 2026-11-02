import pytest
from pyiceberg.table import StaticTable

from pysail.tests.spark.catalog.glue.test_iceberg_commit import _glue_client


@pytest.mark.parametrize("fmt", ["parquet", "delta", "iceberg"])
def test_comments_update_glue_metadata(spark, tmp_path, moto_endpoint, fmt):
    table = f"comment_{fmt}"
    name = f"test_db.{table}"
    client = _glue_client(moto_endpoint)
    try:
        spark.sql(
            f"CREATE TABLE {name} (id INT COMMENT 'original column') USING {fmt} "
            f"LOCATION '{tmp_path.as_uri()}' COMMENT 'original table'"
        )
        for value in [None, "updated", "", "NULL", None]:
            literal = "NULL" if value is None else f"'{value}'"
            spark.sql(f"COMMENT ON TABLE {name} IS {literal}")
            spark.sql(f"COMMENT ON COLUMN {name}.id IS {literal}")
            stored = client.get_table(DatabaseName="test_db", Name=table)["Table"]
            assert stored.get("Description") == value
            assert stored["StorageDescriptor"]["Columns"][0].get("Comment") == value
            description = spark.sql(f"DESCRIBE TABLE EXTENDED {name}").collect()
            assert next((row.data_type for row in description if row.col_name == "Comment"), None) == value
            assert next(row.comment for row in description if row.col_name == "id") == value
            if fmt == "iceberg":
                reference = StaticTable.from_metadata(stored["Parameters"]["metadata_location"])
                assert reference.schema().find_field("id").doc == value
                assert reference.properties.get("comment") == value
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
