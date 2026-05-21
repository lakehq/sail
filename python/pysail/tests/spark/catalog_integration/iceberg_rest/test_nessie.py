"""Nessie-backed Iceberg REST catalog integration tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_create_table_with_nessie_catalog(nessie_spark: SparkSession) -> None:
    """Create a table through Nessie's Iceberg REST endpoint."""
    namespace = "nessie_create_table"
    table = f"{namespace}.t1"

    try:
        nessie_spark.sql(f"CREATE DATABASE IF NOT EXISTS {namespace}").collect()
        nessie_spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg").collect()

        rows = nessie_spark.sql(f"SHOW TABLES IN {namespace} LIKE 't1'").collect()

        assert len(rows) == 1
        assert rows[0].database == namespace
        assert rows[0].tableName == "t1"
        assert rows[0].isTemporary is False
    finally:
        nessie_spark.sql(f"DROP DATABASE IF EXISTS {namespace} CASCADE").collect()
