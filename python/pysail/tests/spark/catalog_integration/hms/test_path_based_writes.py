"""Path-based writes while an HMS catalog is configured.

Path-based writes do not involve the catalog, but they are the documented
workaround in https://github.com/lakehq/sail/issues/2055 for users whose
catalog-based writes fail, so they must keep working when the default
catalog is a Hive Metastore. The storage backend is incidental; the
fixture's S3-compatible store is used because it is already wired up.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import pytest

from pysail.tests.spark.catalog_integration.hms.conftest import HMS_S3_BUCKET

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.mark.parametrize("format_", ["parquet", "iceberg", "delta"])
def test_write_save_path_create(hms_spark: SparkSession, format_: str) -> None:
    path = f"s3://{HMS_S3_BUCKET}/path_writes/{format_}_{uuid.uuid4().hex}"

    df = hms_spark.sql("SELECT 1 AS id, 'hello' AS text")
    df.write.format(format_).save(path)
    rows = hms_spark.read.format(format_).load(path).collect()
    assert [(row.id, row.text) for row in rows] == [(1, "hello")]


@pytest.mark.parametrize("format_", ["parquet", "iceberg", "delta"])
def test_write_save_path(hms_spark: SparkSession, format_: str) -> None:
    path = f"s3://{HMS_S3_BUCKET}/path_writes/{format_}_{uuid.uuid4().hex}"

    df = hms_spark.sql("SELECT 1 AS id, 'hello' AS text")
    df.write.format(format_).save(path)
    rows = hms_spark.read.format(format_).load(path).collect()
    assert [(row.id, row.text) for row in rows] == [(1, "hello")]

    df = hms_spark.sql("SELECT 2 AS id, 'world' AS text")
    df.write.format(format_).mode("append").save(path)
    rows = sorted(hms_spark.read.format(format_).load(path).collect())
    assert [(row.id, row.text) for row in rows] == [(1, "hello"), (2, "world")]
