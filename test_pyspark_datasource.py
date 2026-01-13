# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "pyspark[connect]==4.1.0",
# ]
# ///
"""
Pure PySpark script to test client-side DataSource registration with Sail.

This script:
1. Connects to a running Sail server via Spark Connect
2. Registers a custom Python DataSource
3. Reads from it and shows results

Usage:
    # First, start the Sail server:
    cargo run --features python -p sail-cli -- spark server

    # Then run this script:
    uv run test_pyspark_datasource.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
import pyarrow as pa


# =============================================================================
# Define a simple DataSource
# =============================================================================

class DummyDataSource(DataSource):
    """A dummy data source that returns static data."""

    @classmethod
    def name(cls):
        return "dummy"

    def schema(self):
        return pa.schema([
            ("id", pa.int32()),
            ("name", pa.string()),
            ("value", pa.float64()),
        ])

    def reader(self, schema):
        return DummyReader()


class DummyReader(DataSourceReader):
    """Reader that returns static rows as PyArrow RecordBatch."""

    def partitions(self):
        # Single partition for simplicity
        return [InputPartition(0)]

    def read(self, partition):
        # Create a PyArrow RecordBatch with test data
        schema = pa.schema([
            ("id", pa.int32()),
            ("name", pa.string()),
            ("value", pa.float64()),
        ])
        batch = pa.RecordBatch.from_pydict({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "value": [100.5, 200.0, 150.75, 300.25, 50.0],
        }, schema=schema)
        yield batch


# =============================================================================
# Main script
# =============================================================================

def main():
    # Connect to Sail server via Spark Connect
    print("Connecting to Sail server...")
    spark = SparkSession.builder \
        .remote("sc://localhost:50051") \
        .getOrCreate()

    print(f"Spark version: {spark.version}")

    # Register the custom datasource
    print("\nRegistering DummyDataSource...")
    spark.dataSource.register(DummyDataSource)
    print("✓ DataSource registered!")

    # Read from the datasource
    print("\nReading from 'dummy' format...")
    df = spark.read.format("dummy").load()

    # Show the data
    print("\n=== df.show() ===")
    df.show()

    # Collect and print
    print("\n=== df.collect() ===")
    rows = df.collect()
    for row in rows:
        print(f"  id={row.id}, name={row.name}, value={row.value}")

    # Run some queries
    print("\n=== Filtered query (value > 100) ===")
    df.filter("value > 100").show()

    # Schema info
    print("\n=== Schema ===")
    df.printSchema()

    # Cleanup
    spark.stop()
    print("\n✓ Done!")


if __name__ == "__main__":
    main()
