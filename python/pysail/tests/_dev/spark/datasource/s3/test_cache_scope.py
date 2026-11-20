# SPDX-License-Identifier: Apache-2.0

"""File caches must keep files at the same path in different S3 buckets apart."""

from __future__ import annotations

import io
from typing import TYPE_CHECKING

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import functions as F  # noqa: N812

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_KEY = "data/part-0.parquet"


def _parquet_bytes(ids: list[int]) -> bytes:
    buffer = io.BytesIO()
    pq.write_table(
        pa.table({"id": pa.array(ids, type=pa.int64())}),
        buffer,
        compression="none",
        use_dictionary=False,
    )
    return buffer.getvalue()


@pytest.fixture(scope="module")
def buckets(moto_endpoint: str) -> dict[str, list[int]]:
    """Create two buckets with a Parquet file of the same key and size but different rows."""
    contents = {"bucket-a": [1, 2, 3], "bucket-b": [4, 5, 6]}
    files = {bucket: _parquet_bytes(ids) for bucket, ids in contents.items()}
    assert len(set(map(len, files.values()))) == 1, "the files must have the same size"
    s3 = boto3.client("s3", region_name="us-east-1", endpoint_url=moto_endpoint)
    for bucket, data in files.items():
        s3.create_bucket(Bucket=bucket)
        s3.put_object(Bucket=bucket, Key=_KEY, Body=data)
    return contents


@pytest.mark.parametrize("path", ["data/", _KEY])
def test_read_same_path_in_different_buckets(spark: SparkSession, buckets: dict[str, list[int]], path: str) -> None:
    # Read each bucket twice, so that the second read of each bucket can use cached entries.
    for _ in range(2):
        for bucket, ids in buckets.items():
            df = spark.read.parquet(f"s3://{bucket}/{path}")
            assert sorted(row.id for row in df.collect()) == ids
            assert df.count() == len(ids)
            assert df.filter(F.col("id") > 3).count() == sum(1 for x in ids if x > 3)
