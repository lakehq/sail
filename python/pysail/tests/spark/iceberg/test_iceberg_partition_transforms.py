"""Tests for partition transforms with the DataFrameWriterV2 API.

These tests verify that partition transforms are correctly parsed when using
PySpark's writeTo().partitionedBy() API. The memory catalog does not support
partition transforms, so these tests verify parsing by checking the error message.
"""

import pytest

from pysail.tests.spark.utils import is_jvm_spark

try:
    from pyspark.sql.functions import partitioning

    HAS_PARTITIONING = True
except ImportError:
    HAS_PARTITIONING = False
    partitioning = None


@pytest.mark.skipif(is_jvm_spark(), reason="Spark does not handle v1 and v2 tables properly")
@pytest.mark.skipif(not HAS_PARTITIONING, reason="partitioning module not available in this Spark version")
@pytest.mark.parametrize(
    ("transform_name", "transform_func"),
    [
        ("years", lambda p: p.years("dt")),
        ("months", lambda p: p.months("dt")),
        ("days", lambda p: p.days("dt")),
        ("hours", lambda p: p.hours("ts")),
    ],
    ids=["years", "months", "days", "hours"],
)
def test_partition_transform_time_based(spark, tmp_path, transform_name, transform_func):
    """Test that time-based partition transforms are correctly parsed from PySpark V2 API."""
    location = str(tmp_path / f"t_{transform_name}")
    df = spark.createDataFrame(
        [(1, "2023-03-15 10:30:00")],
        schema="id INT, ts STRING",
    )
    df = df.selectExpr("id", "to_date(ts) as dt", "to_timestamp(ts) as ts")

    with pytest.raises(Exception, match=r"partition transforms are not supported by memory catalog"):
        df.writeTo(f"t_{transform_name}").option("path", location).partitionedBy(transform_func(partitioning)).create()


@pytest.mark.skipif(is_jvm_spark(), reason="Spark does not handle v1 and v2 tables properly")
@pytest.mark.skipif(not HAS_PARTITIONING, reason="partitioning module not available in this Spark version")
def test_partition_transform_bucket(spark, tmp_path):
    """Test that bucket partition transform is correctly parsed from PySpark V2 API."""
    location = str(tmp_path / "t_bucket")
    df = spark.createDataFrame([(1, "a")], schema="id INT, name STRING")

    with pytest.raises(Exception, match=r"partition transforms are not supported by memory catalog"):
        df.writeTo("t_bucket").option("path", location).partitionedBy(partitioning.bucket(10, "id")).create()
