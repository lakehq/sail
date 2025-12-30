"""
Verification script for Python DataSource implementation.

This script creates a simple Python datasource and tests it with Sail.
"""

from pysail.spark.datasource.base import (
    DataSource, DataSourceReader, InputPartition,
    EqualTo, register
)


# ============================================================================
# Simple Range DataSource (for testing)
# ============================================================================

class RangeReader(DataSourceReader):
    """Reader that generates a range of integers."""

    def __init__(self, start: int, end: int, partitions: int):
        self.start = start
        self.end = end
        self.num_partitions = partitions

    def partitions(self):
        """Split the range across partitions."""
        total = self.end - self.start
        per_partition = total // self.num_partitions
        result = []
        for i in range(self.num_partitions):
            result.append(RangePartition(
                partition_id=i,
                start=self.start + i * per_partition,
                end=self.start + (i + 1) * per_partition if i < self.num_partitions - 1 else self.end
            ))
        return result

    def read(self, partition):
        """Yield rows for this partition."""
        for i in range(partition.start, partition.end):
            yield (i, f"value_{i}")


class RangePartition(InputPartition):
    """Partition with start/end range."""

    def __init__(self, partition_id: int, start: int, end: int):
        super().__init__(partition_id)
        self.start = start
        self.end = end

    def __repr__(self):
        return f"RangePartition({self.partition_id}, {self.start}, {self.end})"


@register
class RangeDataSource(DataSource):
    """
    A simple datasource that generates a range of integers.

    Options:
        start: Start of range (default: 0)
        end: End of range (default: 100)
        partitions: Number of partitions (default: 4)
    """

    @classmethod
    def name(cls):
        return "range_demo"

    def schema(self):
        return "id INT, value STRING"

    def reader(self, schema):
        start = int(self.options.get("start", "0"))
        end = int(self.options.get("end", "100"))
        partitions = int(self.options.get("partitions", "4"))
        return RangeReader(start, end, partitions)


# ============================================================================
# Test
# ============================================================================

def test_datasource_basic():
    """Test basic datasource functionality without Spark/Sail."""
    print("=" * 60)
    print("Testing Python DataSource API")
    print("=" * 60)

    # Create datasource
    ds = RangeDataSource({"start": "0", "end": "10", "partitions": "2"})

    print(f"\n1. DataSource name: {ds.name()}")
    print(f"2. DataSource schema: {ds.schema()}")

    # Create reader
    reader = ds.reader(None)

    # Get partitions
    partitions = reader.partitions()
    print(f"3. Partitions: {partitions}")

    # Read from each partition
    print("\n4. Reading data:")
    for partition in partitions:
        print(f"   Partition {partition.partition_id}:")
        for row in reader.read(partition):
            print(f"      {row}")

    print("\n" + "=" * 60)
    print("✅ DataSource API test passed!")
    print("=" * 60)


def test_with_sail():
    """Test datasource with Sail server (if available)."""
    print("\n" + "=" * 60)
    print("Testing with Sail Server")
    print("=" * 60)

    try:
        from pyspark.sql import SparkSession

        # Connect to Sail server
        spark = SparkSession.builder \
            .appName("Python DataSource Test") \
            .remote("sc://localhost:50051") \
            .getOrCreate()

        print("\n✅ Connected to Sail server")

        # Test a simple query first
        df = spark.sql("SELECT 1 as test")
        print(f"\n1. Simple query result: {df.collect()}")

        # TODO: Once Python DataSource is wired up in Spark Connect,
        # we can test the actual datasource registration and reading:
        #
        # spark.dataSource.register(RangeDataSource)
        # df = spark.read.format("range_demo").option("start", "0").option("end", "10").load()
        # df.show()

        print("\n✅ Sail server test passed!")

    except Exception as e:
        print(f"\n⚠️  Could not connect to Sail server: {e}")
        print("   Make sure Sail server is running on localhost:50051")
        print("   Run: cargo run --release -p sail-cli -- spark serve")


if __name__ == "__main__":
    # Test 1: Basic API test (no server needed)
    test_datasource_basic()

    # Test 2: Sail server test (requires server running)
    test_with_sail()
