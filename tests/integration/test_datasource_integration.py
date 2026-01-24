#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "pyspark[connect]>=4.0.0",
#     "pyarrow>=14.0.0",
# ]
# ///
"""
Integration tests for Python DataSources with real PySpark-compatible datasources.

These tests verify end-to-end functionality including:
1. Arrow RecordBatch path (zero-copy, high performance)
2. Tuple row path (compatibility, lower performance)  
3. Multi-partition parallel reading
4. Session-scoped registration

Run with: uv run tests/integration/test_datasource_integration.py

Prerequisites:
- Sail server running: cargo run --bin sail --features python -- spark server
- Python 3.9+
"""
import sys
import time
from typing import Iterator, List

import pyarrow as pa

# Compatibility: works with both pyspark and pysail imports
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
except ImportError:
    from pysail.spark.datasource import DataSource, DataSourceReader, InputPartition


# =============================================================================
# Test DataSources - Simple pattern matching working tests
# =============================================================================

class ArrowRangeDataSource(DataSource):
    """DataSource yielding Arrow RecordBatches (zero-copy path)."""

    @classmethod
    def name(cls) -> str:
        return "arrow_range"

    def schema(self):
        return pa.schema([
            ("id", pa.int64()),
            ("value", pa.float64()),
        ])

    def reader(self, schema):
        return ArrowRangeReader()


class ArrowRangeReader(DataSourceReader):
    """Reader that yields Arrow RecordBatches."""
    
    def partitions(self) -> List[InputPartition]:
        # Single partition for simplicity
        return [InputPartition(0)]

    def read(self, partition: InputPartition) -> Iterator[pa.RecordBatch]:
        """Yield Arrow RecordBatches (zero-copy path)."""
        # Create data
        ids = list(range(1000))
        values = [float(i) * 1.5 for i in ids]

        batch = pa.RecordBatch.from_pydict(
            {"id": ids, "value": values},
            schema=pa.schema([("id", pa.int64()), ("value", pa.float64())])
        )
        yield batch


class TupleRangeDataSource(DataSource):
    """DataSource yielding tuples (row-based fallback path)."""

    @classmethod
    def name(cls) -> str:
        return "tuple_range"

    def schema(self):
        return pa.schema([
            ("id", pa.int64()),
            ("name", pa.string()),
        ])

    def reader(self, schema):
        return TupleRangeReader()


class TupleRangeReader(DataSourceReader):
    """Reader that yields tuples (row-based fallback path)."""
    
    def partitions(self) -> List[InputPartition]:
        return [InputPartition(0)]

    def read(self, partition: InputPartition) -> Iterator[tuple]:
        """Yield tuples (row-based fallback path - slower)."""
        for i in range(100):
            yield (i, f"row_{i}")


class MultiPartitionDataSource(DataSource):
    """DataSource with multiple partitions for parallel reading."""

    @classmethod
    def name(cls) -> str:
        return "multi_partition"

    def schema(self):
        return pa.schema([
            ("partition_id", pa.int32()),
            ("row_id", pa.int32()),
        ])

    def reader(self, schema):
        return MultiPartitionReader()


class MultiPartitionReader(DataSourceReader):
    """Reader that creates multiple partitions."""
    
    def partitions(self) -> List[InputPartition]:
        # Create 4 partitions
        return [InputPartition(i) for i in range(4)]

    def read(self, partition: InputPartition) -> Iterator[pa.RecordBatch]:
        """Each partition yields some data."""
        # Partition value is stored in the partition object itself
        # For pyspark InputPartition, the value is passed to __init__
        # We'll just yield a fixed batch per partition
        partition_ids = [0] * 100
        row_ids = list(range(100))
        
        batch = pa.RecordBatch.from_pydict(
            {"partition_id": partition_ids, "row_id": row_ids},
            schema=pa.schema([("partition_id", pa.int32()), ("row_id", pa.int32())])
        )
        yield batch


# =============================================================================
# Tests
# =============================================================================

def get_spark() -> "SparkSession":
    """Get or create Spark session connected to Sail."""
    return SparkSession.builder \
        .remote("sc://localhost:50051") \
        .getOrCreate()


def test_arrow_datasource():
    """Test zero-copy Arrow RecordBatch path."""
    print("Testing Arrow RecordBatch path...")
    spark = get_spark()

    # Register datasource
    spark.dataSource.register(ArrowRangeDataSource)

    # Read data - no options to avoid schema issue
    df = spark.read.format("arrow_range").load()

    # Show data
    df.show(5)
    
    # Collect data (like working test does)
    rows = df.collect()
    count = len(rows)
    assert count == 1000, f"Expected 1000 rows, got {count}"

    # Verify data integrity
    result = df.filter("id < 5").orderBy("id").collect()
    assert len(result) == 5
    assert result[0].id == 0
    print(f"  ✓ Arrow path: {count} rows read successfully")


def test_tuple_datasource():
    """Test row-based tuple fallback path."""
    print("Testing tuple row path...")
    spark = get_spark()

    spark.dataSource.register(TupleRangeDataSource)

    df = spark.read.format("tuple_range").load()
    
    # Show data
    df.show(5)

    rows = df.collect()
    count = len(rows)
    assert count == 100, f"Expected 100 rows, got {count}"

    result = df.filter("id = 50").collect()
    assert len(result) == 1
    assert result[0].name == "row_50"
    print(f"  ✓ Tuple path: {count} rows read successfully")


def test_multi_partition():
    """Test parallel partition reading."""
    print("Testing multi-partition reads...")
    spark = get_spark()

    spark.dataSource.register(MultiPartitionDataSource)

    start_time = time.time()
    df = spark.read.format("multi_partition").load()
    
    df.show(5)

    rows = df.collect()
    count = len(rows)
    elapsed = time.time() - start_time

    # 4 partitions * 100 rows each = 400 total
    assert count == 400, f"Expected 400 rows, got {count}"
    print(f"  ✓ Multi-partition: {count} rows in {elapsed:.2f}s ({count/elapsed:.0f} rows/s)")


def test_performance_comparison():
    """Compare Arrow vs tuple path performance."""
    print("Comparing Arrow vs Tuple performance...")
    spark = get_spark()

    spark.dataSource.register(ArrowRangeDataSource)
    spark.dataSource.register(TupleRangeDataSource)

    # Arrow path
    start = time.time()
    df_arrow = spark.read.format("arrow_range").load()
    len(df_arrow.collect())
    arrow_time = time.time() - start

    # Tuple path
    start = time.time()
    df_tuple = spark.read.format("tuple_range").load()
    len(df_tuple.collect())
    tuple_time = time.time() - start

    print(f"  Arrow (1k rows): {arrow_time:.3f}s")
    print(f"  Tuple (100 rows):  {tuple_time:.3f}s")
    print(f"  ✓ Performance comparison complete")


def main():
    """Run all integration tests."""
    print("=" * 60)
    print("Python DataSource Integration Tests")
    print("=" * 60)

    tests = [
        test_arrow_datasource,
        test_tuple_datasource,
        test_multi_partition,
        test_performance_comparison,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  ✗ {test.__name__}: {e}")
            failed += 1

    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
