# ruff: noqa: S608
"""TRUNCATE publishes Iceberg metadata through the catalog commit authority."""

from pysail.tests.spark.catalog.glue.test_iceberg_commit import _load_metadata_json, _metadata_location


def test_truncate_advances_glue_metadata_pointer(spark, moto_endpoint, tmp_path):
    database = "glue_iceberg_truncate_db"
    name = f"{database}.truncate_t"
    path = tmp_path / "truncate_t"
    spark.sql(f"CREATE DATABASE {database}")
    try:
        spark.sql(f"CREATE TABLE {name} (id INT) USING iceberg LOCATION '{path.as_uri()}'")
        spark.sql(f"INSERT INTO {name} VALUES (1), (2)")
        before_location = _metadata_location(moto_endpoint, database, "truncate_t")
        before = _load_metadata_json(before_location)
        files = set(path.rglob("*.parquet"))
        spark.sql(f"TRUNCATE TABLE {name}").collect()
        after_location = _metadata_location(moto_endpoint, database, "truncate_t")
        assert after_location != before_location
        after = _load_metadata_json(after_location)
        assert after["snapshots"][:-1] == before["snapshots"]
        assert after["snapshots"][-1]["summary"]["operation"] == "delete"
        assert spark.table(name).collect() == []
        assert set(path.rglob("*.parquet")) == files
        spark.sql(f"TRUNCATE TABLE {name}").collect()
        assert _metadata_location(moto_endpoint, database, "truncate_t") != after_location
        spark.sql(f"INSERT INTO {name} VALUES (3)")
        assert [row.id for row in spark.table(name).collect()] == [3]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
