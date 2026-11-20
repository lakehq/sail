# SPDX-License-Identifier: Apache-2.0

"""One read of paths in different object stores must read each path from its own store."""

from __future__ import annotations

import io
from typing import TYPE_CHECKING

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import functions as F  # noqa: N812

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession


def _parquet_bytes(ids: list[int]) -> bytes:
    buffer = io.BytesIO()
    pq.write_table(pa.table({"id": pa.array(ids, type=pa.int64())}), buffer)
    return buffer.getvalue()


@pytest.fixture(scope="module")
def s3(moto_endpoint: str):
    client = boto3.client("s3", region_name="us-east-1", endpoint_url=moto_endpoint)
    files = {
        # Different keys in each bucket.
        ("bucket-a", "only/a.parquet"): [1, 2, 3],
        ("bucket-b", "only/b.parquet"): [4, 5],
        # The same key in both buckets, with different rows.
        ("bucket-a", "same/part-0.parquet"): [10, 11],
        ("bucket-b", "same/part-0.parquet"): [20, 21, 22],
        # Partitioned directories.
        ("bucket-a", "part/p=1/part-0.parquet"): [100],
        ("bucket-b", "part/p=2/part-0.parquet"): [200, 201],
    }
    for bucket in sorted({bucket for bucket, _ in files}):
        client.create_bucket(Bucket=bucket)
    for (bucket, key), ids in files.items():
        client.put_object(Bucket=bucket, Key=key, Body=_parquet_bytes(ids))
    return client


def _ids(spark: SparkSession, *paths: str) -> list[int]:
    return sorted(row.id for row in spark.read.parquet(*paths).collect())


@pytest.mark.usefixtures("s3")
def test_read_different_keys_in_two_buckets(spark: SparkSession) -> None:
    paths = ["s3://bucket-a/only/", "s3://bucket-b/only/"]
    assert _ids(spark, *paths) == [1, 2, 3, 4, 5]
    assert _ids(spark, *reversed(paths)) == [1, 2, 3, 4, 5]
    df = spark.read.parquet(*paths)
    assert df.count() == 5  # noqa: PLR2004
    assert df.filter(F.col("id") > 2).count() == 3  # noqa: PLR2004
    assert df.limit(4).count() == 4  # noqa: PLR2004
    assert sorted(df.inputFiles()) == ["s3://bucket-a/only/a.parquet", "s3://bucket-b/only/b.parquet"]


@pytest.mark.usefixtures("s3")
def test_read_same_key_in_two_buckets(spark: SparkSession) -> None:
    assert _ids(spark, "s3://bucket-a/same/", "s3://bucket-b/same/") == [10, 11, 20, 21, 22]
    assert _ids(spark, "s3://bucket-a/same/part-0.parquet", "s3://bucket-b/same/part-0.parquet") == [
        10,
        11,
        20,
        21,
        22,
    ]
    assert spark.read.parquet("s3://bucket-b/same/", "s3://bucket-a/same/").count() == 5  # noqa: PLR2004


@pytest.mark.usefixtures("s3")
def test_read_partitioned_directories_in_two_buckets(spark: SparkSession) -> None:
    df = spark.read.parquet("s3://bucket-a/part/", "s3://bucket-b/part/")
    # Sail infers partition values as strings, so compare them as integers.
    rows = sorted((row.id, int(row.p)) for row in df.collect())
    assert rows == [(100, 1), (200, 2), (201, 2)]
    assert df.filter(F.col("p") == 2).count() == 2  # noqa: PLR2004


@pytest.mark.usefixtures("s3")
def test_read_local_and_s3_paths(spark: SparkSession, tmp_path: Path) -> None:
    local = tmp_path / "local"
    local.mkdir()
    pq.write_table(pa.table({"id": pa.array([7, 8], type=pa.int64())}), local / "part-0.parquet")
    assert _ids(spark, local.as_uri(), "s3://bucket-b/only/") == [4, 5, 7, 8]
    assert _ids(spark, "s3://bucket-b/only/", local.as_uri()) == [4, 5, 7, 8]
