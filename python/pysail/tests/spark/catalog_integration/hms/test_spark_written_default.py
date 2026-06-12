# ruff: noqa: S608
"""Writes to an HMS table whose Spark schema metadata carries column defaults.

Spark serializes the full ``StructField.metadata`` map into the
``spark.sql.sources.schema`` table property, so a table created by Spark
with ``text STRING DEFAULT "hello"`` stores ``CURRENT_DEFAULT`` as the
JSON-encoded string ``"hello"``. Regression test for
https://github.com/lakehq/sail/issues/2055: the decoded default must be
treated as a string value, not as a column reference.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from pysail.tests.spark.catalog_integration.hms.conftest import HMS_S3_BUCKET

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _create_spark_style_table_with_default(catalog, database: str, table: str) -> None:
    """Register an HMS table the way Spark would, including a column default."""
    from hive_metastore.ttypes import FieldSchema, SerDeInfo, StorageDescriptor, Table

    schema_json = json.dumps(
        {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": True, "metadata": {}},
                {
                    "name": "text",
                    "type": "string",
                    "nullable": True,
                    # The SQL text of the default `"hello"` (a double-quoted
                    # Spark string literal), as Spark stores it.
                    "metadata": {"CURRENT_DEFAULT": '"hello"', "EXISTS_DEFAULT": '"hello"'},
                },
            ],
        }
    )
    hms_table = Table(
        tableName=table,
        dbName=database,
        owner="spark",
        sd=StorageDescriptor(
            cols=[
                FieldSchema(name="id", type="int", comment=None),
                FieldSchema(name="text", type="string", comment=None),
            ],
            location=f"s3://{HMS_S3_BUCKET}/{database}/{table}",
            inputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            outputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            serdeInfo=SerDeInfo(
                serializationLib="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                parameters={},
            ),
        ),
        partitionKeys=[],
        parameters={
            "EXTERNAL": "TRUE",
            "spark.sql.sources.provider": "parquet",
            "spark.sql.sources.schema": schema_json,
        },
        tableType="EXTERNAL_TABLE",
    )
    with catalog._client as client:  # noqa: SLF001
        client.create_table(hms_table)


def test_insert_into_table_with_spark_written_default(
    hms_spark: SparkSession,
    pyiceberg_hive_catalog,
    hms_database: str,
) -> None:
    table = "spark_default"
    _create_spark_style_table_with_default(pyiceberg_hive_catalog, hms_database, table)

    # Inserting with all columns provided still resolves every column
    # default; the JSON-encoded string default must not be misread as a
    # column reference.
    hms_spark.sql(f"INSERT INTO {hms_database}.{table} VALUES (1, 'x')")
    rows = hms_spark.sql(f"SELECT id, text FROM {hms_database}.{table} ORDER BY id").collect()
    assert [(row.id, row.text) for row in rows] == [(1, "x")]

    # Omitting the column applies the default value.
    hms_spark.sql(f"INSERT INTO {hms_database}.{table} (id) VALUES (2)")
    rows = hms_spark.sql(f"SELECT id, text FROM {hms_database}.{table} ORDER BY id").collect()
    assert [(row.id, row.text) for row in rows] == [(1, "x"), (2, "hello")]
