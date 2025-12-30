"""
Example Python DataSources for Sail.

These examples demonstrate how to implement custom data sources in Python.
They can also be used for testing and demonstration purposes.
"""

from typing import Iterator, List, Tuple, Union

try:
    import pyarrow as pa
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False
    pa = None

from .base import (
    DataSource,
    DataSourceReader,
    InputPartition,
    register,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Filter,
)


# ============================================================================
# RangeDataSource - Generates sequential integers
# ============================================================================

class RangeInputPartition(InputPartition):
    """Partition for RangeDataSource containing a range of values."""

    def __init__(self, partition_id: int, start: int, end: int):
        super().__init__(partition_id)
        self.start = start
        self.end = end

    def __repr__(self):
        return f"RangeInputPartition({self.partition_id}, start={self.start}, end={self.end})"


class RangeDataSourceReader(DataSourceReader):
    """Reader for RangeDataSource that generates sequential integers."""

    def __init__(self, start: int, end: int, num_partitions: int, step: int = 1):
        self.start = start
        self.end = end
        self.num_partitions = max(1, num_partitions)
        self.step = step
        self._filters: List[Filter] = []

    def pushFilters(self, filters: List[Filter]) -> Iterator[Filter]:
        """
        Accept filters that can be applied to the range.

        We can handle simple comparison filters on the 'id' column.
        """
        for f in filters:
            if isinstance(f, (EqualTo, GreaterThan, GreaterThanOrEqual,
                            LessThan, LessThanOrEqual)):
                # Check if it's filtering on the 'id' column
                if f.column == ("id",):
                    self._filters.append(f)
                    continue
            # Yield filters we can't handle
            yield f

    def partitions(self) -> List[InputPartition]:
        """Split the range into partitions for parallel reading."""
        total_values = (self.end - self.start) // self.step
        values_per_partition = max(1, total_values // self.num_partitions)

        partitions = []
        current_start = self.start

        for i in range(self.num_partitions):
            if current_start >= self.end:
                break

            # Calculate partition end
            if i == self.num_partitions - 1:
                # Last partition gets remaining values
                partition_end = self.end
            else:
                partition_end = min(
                    current_start + values_per_partition * self.step,
                    self.end
                )

            partitions.append(RangeInputPartition(i, current_start, partition_end))
            current_start = partition_end

        return partitions if partitions else [RangeInputPartition(0, self.start, self.end)]

    def read(self, partition: InputPartition) -> Iterator[Tuple]:
        """Generate rows for a partition."""
        if not isinstance(partition, RangeInputPartition):
            raise ValueError(f"Expected RangeInputPartition, got {type(partition)}")

        # Apply any pushed filters to adjust the range
        start = partition.start
        end = partition.end

        for f in self._filters:
            if isinstance(f, EqualTo) and isinstance(f.value, int):
                # EqualTo: only yield if value is in range
                if start <= f.value < end:
                    yield (f.value,)
                return
            elif isinstance(f, GreaterThan) and isinstance(f.value, int):
                start = max(start, f.value + 1)
            elif isinstance(f, GreaterThanOrEqual) and isinstance(f.value, int):
                start = max(start, f.value)
            elif isinstance(f, LessThan) and isinstance(f.value, int):
                end = min(end, f.value)
            elif isinstance(f, LessThanOrEqual) and isinstance(f.value, int):
                end = min(end, f.value + 1)

        # Generate values
        for i in range(start, end, self.step):
            yield (i,)


@register
class RangeDataSource(DataSource):
    """
    A data source that generates sequential integers.

    This is useful for testing and demonstration. It supports:
    - Parallel reading via partitions
    - Filter pushdown for comparison operators
    - Configurable range and step

    Options:
        start: Starting value (default: 0)
        end: Ending value (exclusive, required)
        step: Step between values (default: 1)
        numPartitions: Number of partitions (default: 4)

    Example:
        df = spark.read.format("range") \\
            .option("end", "1000") \\
            .option("numPartitions", "8") \\
            .load()
    """

    @classmethod
    def name(cls) -> str:
        return "range"

    def schema(self) -> str:
        """Return schema as DDL string."""
        return "id BIGINT"

    def reader(self, schema) -> DataSourceReader:
        """Create a reader for this range."""
        start = int(self.options.get("start", "0"))
        end = int(self.options.get("end", "10"))
        step = int(self.options.get("step", "1"))
        num_partitions = int(self.options.get("numPartitions", "4"))

        return RangeDataSourceReader(start, end, num_partitions, step)


# ============================================================================
# ConstantDataSource - Returns constant rows (for testing)
# ============================================================================

class ConstantDataSourceReader(DataSourceReader):
    """Reader that returns a fixed number of constant rows."""

    def __init__(self, num_rows: int, value: str):
        self.num_rows = num_rows
        self.value = value

    def read(self, partition: InputPartition) -> Iterator[Tuple]:
        """Generate constant rows."""
        for i in range(self.num_rows):
            yield (i, self.value)


@register
class ConstantDataSource(DataSource):
    """
    A data source that returns constant values.

    Useful for testing schema handling and basic data flow.

    Options:
        numRows: Number of rows to generate (default: 10)
        value: The constant string value (default: "hello")

    Example:
        df = spark.read.format("constant") \\
            .option("numRows", "100") \\
            .option("value", "test") \\
            .load()
    """

    @classmethod
    def name(cls) -> str:
        return "constant"

    def schema(self) -> str:
        """Return schema as DDL string."""
        return "id INT, value STRING"

    def reader(self, schema) -> DataSourceReader:
        """Create a reader."""
        num_rows = int(self.options.get("numRows", "10"))
        value = self.options.get("value", "hello")
        return ConstantDataSourceReader(num_rows, value)


# ============================================================================
# Exports
# ============================================================================

__all__ = [
    "RangeDataSource",
    "RangeDataSourceReader",
    "RangeInputPartition",
    "ConstantDataSource",
    "ConstantDataSourceReader",
]
