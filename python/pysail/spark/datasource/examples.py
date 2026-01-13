"""
Example Python DataSources for Sail.

These examples demonstrate how to implement custom data sources in Python.
They can also be used for testing and demonstration purposes.
"""

from typing import Iterator, List, Any

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

    BATCH_SIZE = 8192

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
            if isinstance(f, (EqualTo, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual)):
                if f.column == ("id",):
                    self._filters.append(f)
                    continue
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
            raise ValueError(f"Expected RangeInputPartition, got {type(partition)}")

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
        batch_values: List[int] = []
        for i in range(start, end, self.step):
            batch_values.append(i)
            if len(batch_values) >= self.BATCH_SIZE:
                yield pa.RecordBatch.from_pydict({"id": batch_values}, schema=schema)
                batch_values = []

        if batch_values:
            yield pa.RecordBatch.from_pydict({"id": batch_values}, schema=schema)


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

    def schema(self):
        # PyArrow Schema preferred; DDL string fallback also works for basic types
        if HAS_PYARROW:
            return pa.schema([("id", pa.int64())])
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

    BATCH_SIZE = 8192

    def __init__(self, num_rows: int, value: str):
        self.num_rows = num_rows
        self.value = value

    def read(self, partition: InputPartition) -> Iterator[Any]:
        """Generate RecordBatches with constant rows."""
        schema = pa.schema([("id", pa.int32()), ("value", pa.string())])
        ids: List[int] = []
        values: List[str] = []

        for i in range(self.num_rows):
            ids.append(i)
            values.append(self.value)
            if len(ids) >= self.BATCH_SIZE:
                yield pa.RecordBatch.from_pydict({"id": ids, "value": values}, schema=schema)
                ids = []
                values = []

        if ids:
            yield pa.RecordBatch.from_pydict({"id": ids, "value": values}, schema=schema)


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

    def schema(self):
        if HAS_PYARROW:
            return pa.schema([("id", pa.int32()), ("value", pa.string())])
        return "id INT, value STRING"

    def reader(self, schema) -> DataSourceReader:
        """Create a reader."""
        num_rows = int(self.options.get("numRows", "10"))
        value = self.options.get("value", "hello")
        return ConstantDataSourceReader(num_rows, value)


# ============================================================================
# FlappyBirdDataSource - Fun test datasource
# ============================================================================


class FlappyBirdReader(DataSourceReader):
    """Reader for FlappyBirdDataSource."""

    BATCH_SIZE = 8192

    def __init__(self, num_birds: int, difficulty: str):
        self.num_birds = num_birds
        self.difficulty = difficulty

    def read(self, partition: InputPartition) -> Iterator[Any]:
        """Generate Flappy Bird game data as RecordBatches."""
        import random

        random.seed(42 + partition.partition_id)

        difficulties = {"easy": 100, "medium": 200, "hard": 300}
        max_score = difficulties.get(self.difficulty, 150)

        birds_per_partition = self.num_birds // 4 + (1 if partition.partition_id < self.num_birds % 4 else 0)
        start_id = sum(
            self.num_birds // 4 + (1 if i < self.num_birds % 4 else 0) for i in range(partition.partition_id)
        )

        bird_names = ["Flappy", "Tweety", "Birdy", "Wingman", "Feathers", "Swoopy", "Chirpy", "Pecky"]

        schema = pa.schema(
            [
                ("bird_id", pa.int32()),
                ("name", pa.string()),
                ("score", pa.int32()),
                ("pipes_passed", pa.int32()),
                ("is_alive", pa.bool_()),
            ]
        )

        bird_ids: List[int] = []
        names: List[str] = []
        scores: List[int] = []
        pipes_list: List[int] = []
        alive_list: List[bool] = []

        for i in range(birds_per_partition):
            bird_id = start_id + i
            name = f"{bird_names[bird_id % len(bird_names)]}_{bird_id}"
            score = random.randint(0, max_score)
            pipes_passed = score // 10
            is_alive = random.random() > 0.3

            bird_ids.append(bird_id)
            names.append(name)
            scores.append(score)
            pipes_list.append(pipes_passed)
            alive_list.append(is_alive)

            if len(bird_ids) >= self.BATCH_SIZE:
                yield pa.RecordBatch.from_pydict(
                    {
                        "bird_id": bird_ids,
                        "name": names,
                        "score": scores,
                        "pipes_passed": pipes_list,
                        "is_alive": alive_list,
                    },
                    schema=schema,
                )
                bird_ids = []
                names = []
                scores = []
                pipes_list = []
                alive_list = []

        if bird_ids:
            yield pa.RecordBatch.from_pydict(
                {
                    "bird_id": bird_ids,
                    "name": names,
                    "score": scores,
                    "pipes_passed": pipes_list,
                    "is_alive": alive_list,
                },
                schema=schema,
            )


@register
class FlappyBirdDataSource(DataSource):
    """
    A fun datasource that generates Flappy Bird game data.

    Options:
        numBirds: Number of birds to generate (default: 20)
        difficulty: Game difficulty - easy, medium, hard (default: medium)

    Example:
        df = spark.read.format("flappy_bird") \\
            .option("numBirds", "100") \\
            .option("difficulty", "hard") \\
            .load()
    """

    @classmethod
    def name(cls) -> str:
        return "flappy_bird"

    def schema(self):
        if HAS_PYARROW:
            return pa.schema(
                [
                    ("bird_id", pa.int32()),
                    ("name", pa.string()),
                    ("score", pa.int32()),
                    ("pipes_passed", pa.int32()),
                    ("is_alive", pa.bool_()),
                ]
            )
        return "bird_id INT, name STRING, score INT, pipes_passed INT, is_alive BOOLEAN"

    def reader(self, schema) -> DataSourceReader:
        num_birds = int(self.options.get("numBirds", "20"))
        difficulty = self.options.get("difficulty", "medium")
        return FlappyBirdReader(num_birds, difficulty)


# ============================================================================
# Exports
# ============================================================================

__all__ = [
    "RangeDataSource",
    "RangeDataSourceReader",
    "RangeInputPartition",
    "ConstantDataSource",
    "ConstantDataSourceReader",
    "FlappyBirdDataSource",
    "FlappyBirdReader",
]
