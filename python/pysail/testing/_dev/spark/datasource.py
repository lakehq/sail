from collections.abc import Iterator
from typing import Any

import pyarrow as pa
from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    EqualTo,
    Filter,
    GreaterThan,
    GreaterThanOrEqual,
    InputPartition,
    LessThan,
    LessThanOrEqual,
)


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

    BATCH_SIZE = 8192

    def __init__(self, start: int, end: int, num_partitions: int, step: int = 1):
        self.start = start
        self.end = end
        self.num_partitions = max(1, num_partitions)
        self.step = step
        self._filters: list[Filter] = []

    def pushFilters(self, filters: list[Filter]) -> Iterator[Filter]:  # noqa: N802
        """
        Accept filters that can be applied to the range.

        We can handle simple comparison filters on the 'id' column.
        """
        for f in filters:
            if isinstance(
                f, EqualTo | GreaterThan | GreaterThanOrEqual | LessThan | LessThanOrEqual
            ) and f.attribute == ("id",):
                self._filters.append(f)
            else:
                yield f

    def partitions(self) -> list[InputPartition]:
        """Split the range into partitions for parallel reading."""
        total_values = (self.end - self.start) // self.step
        values_per_partition = max(1, total_values // self.num_partitions)

        partitions = []
        current_start = self.start

        for i in range(self.num_partitions):
            if current_start >= self.end:
                break

            if i == self.num_partitions - 1:
                partition_end = self.end
            else:
                partition_end = min(current_start + values_per_partition * self.step, self.end)

            partitions.append(RangeInputPartition(i, current_start, partition_end))
            current_start = partition_end

        return partitions if partitions else [RangeInputPartition(0, self.start, self.end)]

    def read(self, partition: InputPartition) -> Iterator[Any]:
        """Generate RecordBatches for a partition."""
        if not isinstance(partition, RangeInputPartition):
            msg = f"Expected RangeInputPartition, got {type(partition)}"
            raise TypeError(msg)

        start = partition.start
        end = partition.end

        for f in self._filters:
            if isinstance(f, EqualTo) and isinstance(f.value, int):
                if start <= f.value < end:
                    yield pa.RecordBatch.from_pydict({"id": [f.value]}, schema=pa.schema([("id", pa.int64())]))
                return
            elif isinstance(f, GreaterThan) and isinstance(f.value, int):
                start = max(start, f.value + 1)
            elif isinstance(f, GreaterThanOrEqual) and isinstance(f.value, int):
                start = max(start, f.value)
            elif isinstance(f, LessThan) and isinstance(f.value, int):
                end = min(end, f.value)
            elif isinstance(f, LessThanOrEqual) and isinstance(f.value, int):
                end = min(end, f.value + 1)

        schema = pa.schema([("id", pa.int64())])
        batch_values: list[int] = []
        for i in range(start, end, self.step):
            batch_values.append(i)
            if len(batch_values) >= self.BATCH_SIZE:
                yield pa.RecordBatch.from_pydict({"id": batch_values}, schema=schema)
                batch_values = []

        if batch_values:
            yield pa.RecordBatch.from_pydict({"id": batch_values}, schema=schema)


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
    """

    @classmethod
    def name(cls) -> str:
        return "range"

    def schema(self):
        # Allow testing DDL string fallback via options
        if self.options.get("use_ddl_schema", "false").lower() == "true":
            return "id BIGINT"
        # PyArrow Schema preferred; DDL string fallback also works for basic types
        return pa.schema([("id", pa.int64())])

    def reader(self, schema) -> DataSourceReader:  # noqa: ARG002
        """Create a reader for this range."""
        start = int(self.options.get("start", "0"))
        end = int(self.options.get("end", "10"))
        step = int(self.options.get("step", "1"))
        num_partitions = int(self.options.get("numPartitions", "4"))

        return RangeDataSourceReader(start, end, num_partitions, step)


# ============================================================================
# Basic API tests that don't require a Spark session
# ============================================================================
