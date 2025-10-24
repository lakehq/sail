"""
Example: Creating a custom Python data source for Lakesail.

This demonstrates how to implement the three-method interface to create
a custom data source that works with Lakesail's generic Python bridge.
"""

from collections.abc import Iterator
from typing import Any

import pyarrow as pa
from pyspark.sql import SparkSession


class RestAPIDataSource:
    """
    Example custom data source that reads from a REST API.

    This demonstrates the minimal interface required:
    - infer_schema(options) -> pa.Schema
    - plan_partitions(options) -> List[dict]
    - read_partition(partition_spec, options) -> Iterator[pa.RecordBatch]
    """

    def infer_schema(self, _options: dict[str, str]) -> pa.Schema:
        """
        Infer schema by fetching a sample from the API.

        In a real implementation, you might:
        - Fetch a small sample to determine field types
        - Use API metadata/documentation
        - Allow users to provide schema via options
        """
        # For this example, we'll use a hardcoded schema
        # In practice, you'd inspect actual API response
        return pa.schema(
            [
                ("id", pa.int64()),
                ("name", pa.string()),
                ("email", pa.string()),
                ("created_at", pa.timestamp("us")),
                ("is_active", pa.bool_()),
            ]
        )

    def plan_partitions(self, options: dict[str, str]) -> list[dict[str, Any]]:
        """
        Plan how to partition the API data.

        For REST APIs, common partition strategies:
        - Pagination (one partition per page)
        - Date ranges (one partition per day/week/month)
        - Category/shard keys
        """
        num_partitions = int(options.get("numPartitions", "1"))

        # Simple pagination strategy
        # In a real implementation, you might query the API for total count
        return [
            {
                "partition_id": i,
                "offset": i * 1000,  # Assuming 1000 items per partition
                "limit": 1000,
                "endpoint": options.get("endpoint", ""),
            }
            for i in range(num_partitions)
        ]

    def read_partition(
        self,
        partition_spec: dict[str, Any],
        options: dict[str, str],
    ) -> Iterator[pa.RecordBatch]:
        """
        Read one partition from the API.

        This yields Arrow RecordBatches that will be transferred to Rust
        via zero-copy FFI and executed in parallel by DataFusion.
        """
        endpoint = options.get("endpoint", "")
        api_key = options.get("apiKey", "")
        offset = partition_spec["offset"]
        limit = partition_spec["limit"]

        # Fetch data from API
        # (This is a mock example - replace with actual API call)
        # Example request (uncomment for a real implementation):
        # url = f"{endpoint}?offset={offset}&limit={limit}"
        # headers = {"Authorization": f"Bearer {api_key}"}
        # response = requests.get(url, headers=headers, timeout=30)
        # data = response.json()

        # Mock data for example
        data = [
            {
                "id": offset + i,
                "name": f"User {offset + i}",
                "email": f"user{offset + i}@example.com",
                "created_at": "2024-01-01T00:00:00Z",
                "is_active": True,
                "source": f"{endpoint}?offset={offset}&limit={limit}",
                "api_key_present": bool(api_key),
            }
            for i in range(min(limit, 100))  # Mock: return up to 100 records
        ]

        # Convert to Arrow
        table = pa.Table.from_pylist(data)

        # Yield batches (could yield multiple batches for streaming)
        yield from table.to_batches(max_chunksize=1000)


class S3DataSource:
    """
    Example: Reading from S3 with custom partitioning logic.

    This shows how you could build a data source for S3 that:
    - Lists objects matching a prefix
    - Partitions by file
    - Reads files using boto3/s3fs
    """

    def infer_schema(self, _options: dict[str, str]) -> pa.Schema:
        """Infer schema from first file in S3."""
        # In practice, you'd:
        # 1. Use boto3 to list objects
        # 2. Download/stream first file
        # 3. Use PyArrow to read it and get schema

        # Mock schema for example
        return pa.schema(
            [
                ("id", pa.int64()),
                ("value", pa.float64()),
                ("timestamp", pa.timestamp("us")),
            ]
        )

    def plan_partitions(self, options: dict[str, str]) -> list[dict[str, Any]]:
        """Create one partition per S3 object."""
        prefix = options.get("prefix", "")
        bucket = options.get("bucket", "")

        # In practice, use boto3:
        # import boto3
        # s3 = boto3.client('s3')
        # response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        # objects = response.get('Contents', [])

        # Mock: pretend we found 10 files
        return [
            {
                "partition_id": i,
                "object_key": f"{prefix}/part-{i:05d}.parquet",
                "bucket": bucket,
            }
            for i in range(10)
        ]

    def read_partition(
        self,
        partition_spec: dict[str, Any],
        options: dict[str, str],
    ) -> Iterator[pa.RecordBatch]:
        """Read one S3 object."""
        object_key = partition_spec["object_key"]
        bucket = options.get("bucket", partition_spec.get("bucket", ""))

        # In practice:
        # import s3fs
        # import pyarrow.parquet as pq
        # s3 = s3fs.S3FileSystem()
        # table = pq.read_table(f"s3://{bucket}/{object_key}", filesystem=s3)

        # Mock data for example
        data = {
            "bucket": [bucket] * 3,
            "object_key": [object_key] * 3,
            "id": [1, 2, 3],
            "value": [1.0, 2.0, 3.0],
        }
        table = pa.Table.from_pydict(data)

        yield from table.to_batches()


def example_rest_api_datasource():
    """Use the custom REST API data source."""
    spark = SparkSession.builder.appName("Custom REST API Example").getOrCreate()

    # First, ensure the RestAPIDataSource class is importable
    # (In practice, you'd put it in a proper Python package)

    df = (
        spark.read.format("python")
        .option("python_module", "__main__")
        .option("python_class", "RestAPIDataSource")
        .option("endpoint", "https://api.example.com/users")
        .option("apiKey", "your-api-key")
        .option("numPartitions", "5")
        .load()
    )

    df.show()
    spark.stop()


if __name__ == "__main__":
    print("Custom Data Source Examples")
    print("=" * 70)
    print("\nThis demonstrates how to create custom data sources by implementing:")
    print("  - infer_schema(options) -> pa.Schema")
    print("  - plan_partitions(options) -> List[dict]")
    print("  - read_partition(partition_spec, options) -> Iterator[pa.RecordBatch]")
    print("\nExamples included:")
    print("  1. RestAPIDataSource - Read from REST APIs")
    print("  2. S3DataSource - Read from S3 with custom partitioning")
    print("\nYou can use these patterns to build data sources for:")
    print("  - NoSQL databases (MongoDB, DynamoDB, etc.)")
    print("  - Cloud storage (GCS, Azure Blob, etc.)")
    print("  - APIs (GraphQL, gRPC, etc.)")
    print("  - Data warehouses (Snowflake, BigQuery, etc.)")
    print("  - Streaming platforms (Kafka, Kinesis - batch reads)")
    print("  - Custom file formats")
    print("=" * 70)
