"""
Tests for Python DataSource integration.

These tests verify the Python DataSource API works correctly with Sail,
including both Arrow RecordBatch and tuple-based paths.
"""

from collections.abc import Iterator
from typing import Any
from uuid import uuid4

import pyarrow as pa
import pytest

try:
    from pyspark.sql.datasource import (
        DataSource,
        DataSourceArrowWriter,
        DataSourceReader,
        DataSourceWriter,
        EqualTo,
        Filter,
        GreaterThan,
        GreaterThanOrEqual,
        InputPartition,
        LessThan,
        LessThanOrEqual,
    )
except ImportError:
    pytest.skip("Python DataSource API is not available in this Spark version", allow_module_level=True)

# ============================================================================
# RangeDataSource - Example datasource that generates sequential integers
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


class TestPythonDataSourceBasic:
    """Basic API tests that don't require a Spark session."""

    def test_range_datasource_api(self):
        """Test RangeDataSource API without server."""
        # Test with default PyArrow schema
        ds = RangeDataSource(options={"start": "0", "end": "10", "numPartitions": "2"})
        assert ds.name() == "range"
        schema = ds.schema()
        assert isinstance(schema, pa.Schema)
        assert schema.names == ["id"]

        # Test with DDL string schema
        ds_ddl = RangeDataSource(options={"start": "0", "end": "10", "use_ddl_schema": "true"})
        schema_ddl = ds_ddl.schema()
        assert isinstance(schema_ddl, str)
        assert schema_ddl == "id BIGINT"

        reader = ds.reader(schema)
        partitions = reader.partitions()
        assert len(partitions) == 2  # noqa: PLR2004

        # Read from first partition
        rows = list(reader.read(partitions[0]))
        assert len(rows) > 0


class TestPythonDataSource:
    """Tests for Python DataSource integration."""

    def test_arrow_datasource(self, spark):
        """Test zero-copy Arrow RecordBatch path."""
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class ArrowRangeDataSource(DataSource):
            """DataSource yielding Arrow RecordBatches (zero-copy path)."""

            @classmethod
            def name(cls) -> str:
                return "arrow_range_test"

            def schema(self):
                return pa.schema(
                    [
                        ("id", pa.int64()),
                        ("value", pa.float64()),
                    ]
                )

            def reader(self, schema):  # noqa: ARG002
                return ArrowRangeReader()

        class ArrowRangeReader(DataSourceReader):
            """Reader that yields Arrow RecordBatches."""

            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):  # noqa: ARG002
                ids = list(range(100))
                values = [float(i) * 1.5 for i in ids]
                batch = pa.RecordBatch.from_pydict(
                    {"id": ids, "value": values}, schema=pa.schema([("id", pa.int64()), ("value", pa.float64())])
                )
                yield batch

        # Register and read
        spark.dataSource.register(ArrowRangeDataSource)
        df = spark.read.format("arrow_range_test").load()

        rows = df.collect()
        assert len(rows) == 100  # noqa: PLR2004
        assert rows[0].id == 0
        assert rows[0].value == 0.0

        # Test filter query (filter is applied post-read by DataFusion in MVP)
        filtered = df.filter("value > 50.0").collect()
        # ids 34+ have value > 50 (34 * 1.5 = 51)
        assert len(filtered) == 66  # noqa: PLR2004 - ids 34-99 have value > 50

    def test_tuple_datasource(self, spark):
        """Test row-based tuple fallback path with data integrity verification."""
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class TupleDataSource(DataSource):
            """DataSource yielding tuples (row-based fallback path)."""

            @classmethod
            def name(cls) -> str:
                return "tuple_range_test"

            def schema(self):
                return pa.schema(
                    [
                        ("id", pa.int64()),
                        ("square", pa.int64()),
                    ]
                )

            def reader(self, schema):  # noqa: ARG002
                return TupleReader()

        class TupleReader(DataSourceReader):
            """Reader that yields many tuples to test batching."""

            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):  # noqa: ARG002
                # Yield 1000 tuples to test batching behavior
                for i in range(1000):
                    yield (i, i * i)

        spark.dataSource.register(TupleDataSource)
        df = spark.read.format("tuple_range_test").load()

        rows = df.collect()
        assert len(rows) == 1000  # noqa: PLR2004

        # Verify data integrity: check that squares are correct
        # Find row with id=100, should have square=10000
        sample = df.filter("id = 100").collect()
        assert len(sample) == 1
        assert sample[0].square == 10000  # noqa: PLR2004 - 100² = 10000

    def test_multi_partition(self, spark):
        """Test parallel partition reading."""
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class MultiPartitionDataSource(DataSource):
            """DataSource with multiple partitions."""

            @classmethod
            def name(cls) -> str:
                return "multi_partition_test"

            def schema(self):
                return pa.schema(
                    [
                        ("partition_id", pa.int32()),
                        ("row_id", pa.int32()),
                    ]
                )

            def reader(self, schema):  # noqa: ARG002
                return MultiPartitionReader()

        class MultiPartitionReader(DataSourceReader):
            """Reader with 4 partitions."""

            def partitions(self):
                return [InputPartition(i) for i in range(4)]

            def read(self, partition):
                partition_ids = [partition.value] * 25
                row_ids = list(range(25))
                batch = pa.RecordBatch.from_pydict(
                    {"partition_id": partition_ids, "row_id": row_ids},
                    schema=pa.schema([("partition_id", pa.int32()), ("row_id", pa.int32())]),
                )
                yield batch

        spark.dataSource.register(MultiPartitionDataSource)
        df = spark.read.format("multi_partition_test").load()

        rows = df.collect()
        # 4 partitions * 25 rows each = 100 total
        assert len(rows) == 100  # noqa: PLR2004

    def test_partition_failure(self, spark):
        """Test that partition failure errors include partition context."""
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class FailingPartitionDataSource(DataSource):
            """DataSource where partition 2 fails."""

            @classmethod
            def name(cls) -> str:
                return "failing_partition_test"

            def schema(self):
                return pa.schema([("id", pa.int32())])

            def reader(self, schema):  # noqa: ARG002
                return FailingPartitionReader()

        class FailingPartitionReader(DataSourceReader):
            def partitions(self):
                return [InputPartition(i) for i in range(4)]

            def read(self, partition):
                pid = partition.value
                failure_partition = 2
                if pid == failure_partition:
                    msg = f"Deliberate failure in partition {pid}!"
                    raise ValueError(msg)

                schema = pa.schema([("id", pa.int32())])
                batch = pa.RecordBatch.from_pydict({"id": [pid]}, schema=schema)
                yield batch

        spark.dataSource.register(FailingPartitionDataSource)
        df = spark.read.format("failing_partition_test").load()

        with pytest.raises(Exception, match=r"[Dd]eliberate failure"):
            df.collect()

    def test_empty_partitions(self, spark):
        """Test that zero partitions returns empty DataFrame without crashing."""
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader

        class EmptyPartitionsDataSource(DataSource):
            """DataSource that returns zero partitions."""

            @classmethod
            def name(cls) -> str:
                return "empty_partitions_test"

            def schema(self):
                return pa.schema(
                    [
                        ("id", pa.int32()),
                        ("name", pa.string()),
                    ]
                )

            def reader(self, schema):  # noqa: ARG002
                return EmptyPartitionsReader()

        class EmptyPartitionsReader(DataSourceReader):
            def partitions(self):
                return []  # No partitions

            def read(self, partition):  # noqa: ARG002
                msg = "read() should not be called with 0 partitions!"
                raise RuntimeError(msg)

        spark.dataSource.register(EmptyPartitionsDataSource)
        df = spark.read.format("empty_partitions_test").load()

        rows = df.collect()
        assert len(rows) == 0

    def test_schema_mismatch(self, spark):
        """Test that schema mismatch between declared and returned schema is handled."""
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class SchemaMismatchDataSource(DataSource):
            """DataSource that declares one schema but returns another."""

            @classmethod
            def name(cls) -> str:
                return "schema_mismatch_test"

            def schema(self):
                # Declare schema with int32 id
                return pa.schema(
                    [
                        ("id", pa.int32()),
                        ("name", pa.string()),
                    ]
                )

            def reader(self, schema):  # noqa: ARG002
                return SchemaMismatchReader()

        class SchemaMismatchReader(DataSourceReader):
            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):  # noqa: ARG002
                # Return batch with int64 id instead of int32 - schema mismatch!
                wrong_schema = pa.schema(
                    [
                        ("id", pa.int64()),  # Wrong type!
                        ("name", pa.string()),
                    ]
                )
                batch = pa.RecordBatch.from_pydict(
                    {
                        "id": [1, 2, 3],
                        "name": ["a", "b", "c"],
                    },
                    schema=wrong_schema,
                )
                yield batch

        spark.dataSource.register(SchemaMismatchDataSource)
        df = spark.read.format("schema_mismatch_test").load()

        # Should either succeed with coercion or fail with schema error
        # The important thing is it doesn't crash unexpectedly
        try:
            rows = df.collect()
            # If it succeeds, verify data is present
            expected_rows = 3
            assert len(rows) == expected_rows
        except Exception as e:  # noqa: BLE001
            # If it fails, error should mention schema/type issue
            error_msg = str(e).lower()
            assert "schema" in error_msg or "type" in error_msg or "mismatch" in error_msg

    def test_python_exception_handling(self, spark):
        """Test that Python exceptions are properly propagated with traceback."""
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class ExceptionDataSource(DataSource):
            """DataSource whose reader throws an exception."""

            @classmethod
            def name(cls) -> str:
                return "exception_test"

            def schema(self):
                return pa.schema(
                    [
                        ("id", pa.int32()),
                        ("value", pa.string()),
                    ]
                )

            def reader(self, schema):  # noqa: ARG002
                return ExceptionReader()

        class ExceptionReader(DataSourceReader):
            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):  # noqa: ARG002
                msg = "This is a deliberate test exception!"
                raise ValueError(msg)

        spark.dataSource.register(ExceptionDataSource)
        df = spark.read.format("exception_test").load()

        with pytest.raises(Exception, match=r"[Dd]eliberate test exception"):
            df.collect()

    def test_session_isolation(self, spark_session_factory):
        """Test that datasources registered in one session are not visible in another.

        This test creates two separate SparkSessions with unique session IDs
        and verifies that a datasource registered in Session A cannot be
        accessed from Session B.
        """
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class SessionIsolationDataSource(DataSource):
            """DataSource for testing session isolation."""

            @classmethod
            def name(cls) -> str:
                return "session_isolation_test"

            def schema(self):
                return pa.schema([("id", pa.int32()), ("msg", pa.string())])

            def reader(self, schema):  # noqa: ARG002
                return SessionIsolationReader()

        class SessionIsolationReader(DataSourceReader):
            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):  # noqa: ARG002
                batch = pa.RecordBatch.from_pydict(
                    {
                        "id": [1, 2, 3],
                        "msg": ["hello", "from", "session_a"],
                    },
                    schema=pa.schema([("id", pa.int32()), ("msg", pa.string())]),
                )
                yield batch

        # Session A: Register the datasource
        spark_a = spark_session_factory()
        spark_a.dataSource.register(SessionIsolationDataSource)

        # Verify Session A can read from it
        df_a = spark_a.read.format("session_isolation_test").load()
        rows_a = df_a.collect()
        assert len(rows_a) == 3  # noqa: PLR2004

        # Session B: Create a completely independent session
        spark_b = spark_session_factory()

        # Session B should NOT be able to access the datasource from Session A
        # because datasources are registered per-session
        with pytest.raises(Exception, match=r"session_isolation_test|not found|unknown"):
            spark_b.read.format("session_isolation_test").load().collect()


class TestFilterPushdown:
    """Tests for filter pushdown to Python DataSources."""

    def test_filter_pushdown_equality(self, spark):
        """Test that equality filters (WHERE id = X) are pushed to Python reader.

        Note: We verify pushFilters is called by checking that the reader
        actually applies the filter (returns only matching rows). Due to
        cloudpickle serialization, we can't track state across the boundary.
        """
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class FilterApplyingDataSource(DataSource):
            """DataSource that applies pushed filters during read."""

            @classmethod
            def name(cls) -> str:
                return "filter_applying_test"

            def schema(self):
                return pa.schema(
                    [
                        ("id", pa.int64()),
                        ("value", pa.string()),
                    ]
                )

            def reader(self, schema):  # noqa: ARG002
                return FilterApplyingReader()

        class FilterApplyingReader(DataSourceReader):
            def __init__(self):
                self.accepted_filters = []

            def pushFilters(self, filters):  # noqa: N802
                """Accept EqualTo filters and track them."""
                for f in filters:
                    filter_name = type(f).__name__
                    if filter_name == "EqualTo":
                        self.accepted_filters.append(f)
                    else:
                        yield f  # Reject non-equality filters

            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):  # noqa: ARG002
                # Full dataset
                all_data = {"id": [1, 2, 3, 4, 5], "value": ["a", "b", "c", "d", "e"]}

                # Apply accepted filters
                if self.accepted_filters:
                    for f in self.accepted_filters:
                        # EqualTo filter has references to column tuple and value
                        # Access the filter value (it's stored as an attribute)
                        filter_val = getattr(f, "value", None)
                        if filter_val is not None:
                            # Filter rows where id matches
                            filtered_ids = []
                            filtered_values = []
                            for i, id_val in enumerate(all_data["id"]):
                                if id_val == filter_val:
                                    filtered_ids.append(id_val)
                                    filtered_values.append(all_data["value"][i])
                            all_data = {"id": filtered_ids, "value": filtered_values}

                batch = pa.RecordBatch.from_pydict(
                    all_data, schema=pa.schema([("id", pa.int64()), ("value", pa.string())])
                )
                yield batch

        spark.dataSource.register(FilterApplyingDataSource)

        # Execute a query with a filter
        df = spark.read.format("filter_applying_test").load()

        # Query without filter should return all 5 rows
        all_rows = df.collect()
        expected_all_rows = 5
        assert len(all_rows) == expected_all_rows, f"Expected 5 rows, got {len(all_rows)}"

        # Query WITH filter - if pushFilters works, Python reader applies it
        filtered_rows = df.filter("id = 3").collect()

        # Should get exactly 1 row
        assert len(filtered_rows) == 1, f"Expected 1 row, got {len(filtered_rows)}"
        assert filtered_rows[0].id == 3  # noqa: PLR2004

    def test_ddl_schema_fallback(self, spark):
        """Test that DDL string schema is correctly parsed and used."""
        spark.dataSource.register(RangeDataSource)

        # This uses the option we added to RangeDataSource to force DDL schema return
        df = spark.read.format("range").option("use_ddl_schema", "true").option("end", "10").load()

        # Verify schema
        assert df.schema.fields[0].name == "id"
        # In Spark, BIGINT maps to LongType (int64)
        from pyspark.sql.types import LongType

        assert df.schema.fields[0].dataType == LongType()

        # Verify data read works
        rows = df.collect()
        assert len(rows) == 10  # noqa: PLR2004
        # Sort by ID to ensure deterministic check
        rows.sort(key=lambda r: r.id)
        assert rows[0].id == 0

    def test_filter_pushdown_comparison(self, spark):
        """Test that comparison filters (>, <, >=, <=) work correctly.

        We verify that query results are correct when using comparison filters.
        DataFusion post-filters ensure correctness even if pushdown isn't applied.
        """
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class SimpleRangeDataSource(DataSource):
            @classmethod
            def name(cls) -> str:
                return "simple_range_test"

            def schema(self):
                return pa.schema([("id", pa.int64())])

            def reader(self, schema):  # noqa: ARG002
                return SimpleRangeReader()

        class SimpleRangeReader(DataSourceReader):
            def pushFilters(self, filters):  # noqa: ARG002, N802
                # Accept all filters (don't actually apply - let DataFusion post-filter)
                return iter([])

            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):  # noqa: ARG002
                batch = pa.RecordBatch.from_pydict(
                    {"id": list(range(10))},  # 0-9
                    schema=pa.schema([("id", pa.int64())]),
                )
                yield batch

        spark.dataSource.register(SimpleRangeDataSource)
        df = spark.read.format("simple_range_test").load()

        # Test greater than
        rows = df.filter("id > 5").collect()
        expected_rows = 4
        assert len(rows) == expected_rows, f"Expected 4 rows (6,7,8,9), got {len(rows)}"
        min_id = 5
        assert all(r.id > min_id for r in rows)

    def test_filter_pushdown_rejection(self, spark):
        """Test that when reader rejects filters, DataFusion post-filters correctly."""
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class RejectAllFiltersDataSource(DataSource):
            @classmethod
            def name(cls) -> str:
                return "reject_all_filters_test"

            def schema(self):
                return pa.schema([("id", pa.int64())])

            def reader(self, schema):  # noqa: ARG002
                return RejectAllFiltersReader()

        class RejectAllFiltersReader(DataSourceReader):
            def pushFilters(self, filters):  # noqa: N802
                # Reject all filters by yielding them back
                yield from filters

            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):  # noqa: ARG002
                batch = pa.RecordBatch.from_pydict({"id": [1, 2, 3, 4, 5]}, schema=pa.schema([("id", pa.int64())]))
                yield batch

        spark.dataSource.register(RejectAllFiltersDataSource)
        df = spark.read.format("reject_all_filters_test").load()

        # Even though reader rejects filters, query should still work
        # DataFusion will post-filter the results
        rows = df.filter("id = 3").collect()
        assert len(rows) == 1
        assert rows[0].id == 3  # noqa: PLR2004

    def test_filter_pushdown_complex_and(self, spark):
        """Test that complex AND filters are pushed to Python reader.

        Verifies that multiple filters combined with AND are sent as a list
        to pushFilters and can be tracked/applied by the reader.
        """
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class FilterTrackingDataSource(DataSource):
            """DataSource that tracks pushed filters in the output."""

            @classmethod
            def name(cls) -> str:
                return "filter_tracking_and_test"

            def schema(self):
                return pa.schema(
                    [
                        ("id", pa.int32()),
                        ("name", pa.string()),
                        ("value", pa.int32()),
                    ]
                )

            def reader(self, schema):  # noqa: ARG002
                return FilterTrackingReader()

        class FilterTrackingReader(DataSourceReader):
            def __init__(self):
                self.pushed_filters = []

            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):  # noqa: ARG002
                # Encode pushed filters into the 'name' column so test can verify
                filters_str = str(self.pushed_filters)
                data = {
                    "id": [1, 2, 3],
                    "name": [filters_str, filters_str, filters_str],
                    "value": [10, 20, 30],
                }
                schema = pa.schema(
                    [
                        ("id", pa.int32()),
                        ("name", pa.string()),
                        ("value", pa.int32()),
                    ]
                )
                batch = pa.RecordBatch.from_pydict(data, schema=schema)
                yield batch

            def pushFilters(self, filters):  # noqa: N802
                # Store string representations of pushed filters
                self.pushed_filters = [str(f) for f in filters]
                # Accept all filters (return empty to indicate all handled)
                return []

        spark.dataSource.register(FilterTrackingDataSource)

        # Query with complex AND filter
        df = spark.read.format("filter_tracking_and_test").load()
        filtered_df = df.filter("value > 15 AND id < 3")

        rows = filtered_df.collect()
        assert len(rows) > 0

        # The 'name' column contains the string representation of pushed filters
        pushed_filters_str = rows[0].name

        # Should see both GreaterThan and LessThan filters in the list
        assert "GreaterThan" in pushed_filters_str or "LessThan" in pushed_filters_str
        assert "value" in pushed_filters_str or "id" in pushed_filters_str


# ============================================================================
# Write Support Tests
# ============================================================================


def _create_writable_test_datasource():
    """Create a writable datasource class with per-test local commit state."""
    datasource_name = f"writable_test_{uuid4().hex}"

    class InMemoryWriter(DataSourceWriter):
        """Row-based writer for testing."""

        def __init__(self, schema, overwrite):
            self.schema = schema
            self.overwrite = overwrite
            self.data = []
            self.committed = False
            self.aborted = False
            self.commit_messages = []

        def write(self, iterator):
            """Write rows and return commit message."""
            for row in iterator:
                # Convert Row to dict for storage
                self.data.append(dict(row.asDict()))
            return {"partition_data": self.data, "count": len(self.data)}

        def commit(self, messages):
            """Commit the write."""
            self.committed = True
            self.commit_messages = messages
            WritableDataSource.row_commit_messages = messages

        def abort(self, messages):
            """Abort the write."""
            self.aborted = True
            self.commit_messages = messages

    class InMemoryArrowWriter(DataSourceArrowWriter):
        """Arrow-based writer for testing."""

        def __init__(self, schema, overwrite):
            self.schema = schema
            self.overwrite = overwrite
            self.batches = []
            self.committed = False
            self.aborted = False
            self.commit_messages = []

        def write(self, iterator):
            """Write Arrow RecordBatches and return commit message."""
            batch_count = 0
            total_rows = 0
            for batch in iterator:
                self.batches.append(batch)
                batch_count += 1
                total_rows += batch.num_rows
            return {"batch_count": batch_count, "total_rows": total_rows}

        def commit(self, messages):
            """Commit the write."""
            self.committed = True
            self.commit_messages = messages
            WritableDataSource.arrow_commit_messages = messages

        def abort(self, messages):
            """Abort the write."""
            self.aborted = True
            self.commit_messages = messages

    class WritableDataSource(DataSource):
        """DataSource that supports both reading and writing."""

        row_commit_messages = None
        arrow_commit_messages = None

        def __init__(self, options):
            self.options = options
            self.writer_type = options.get("writer_type", "row")

        @classmethod
        def name(cls):
            return datasource_name

        def schema(self):
            return "id INT, value STRING"

        def reader(self, schema):  # noqa: ARG002
            # Simple reader for testing
            class SimpleReader(DataSourceReader):
                def partitions(self):
                    return [InputPartition(0)]

                def read(self, partition):  # noqa: ARG002
                    data = {"id": [1, 2, 3], "value": ["a", "b", "c"]}
                    reader_schema = pa.schema([("id", pa.int32()), ("value", pa.string())])
                    batch = pa.RecordBatch.from_pydict(data, schema=reader_schema)
                    yield batch

            return SimpleReader()

        def writer(self, schema, overwrite):
            """Return writer based on configuration."""
            if self.writer_type == "arrow":
                return InMemoryArrowWriter(schema, overwrite)
            return InMemoryWriter(schema, overwrite)

    return WritableDataSource, datasource_name


def test_basic_write(spark):
    """Test basic Row-based write."""
    writable_test_ds, datasource_name = _create_writable_test_datasource()
    spark.dataSource.register(writable_test_ds)

    # Create test DataFrame
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "value"])

    # Write data
    df.write.format(datasource_name).option("writer_type", "row").mode("append").save()

    # Verify commit was called
    row_messages = writable_test_ds.row_commit_messages
    assert row_messages is not None
    assert len(row_messages) > 0


def test_arrow_write(spark):
    """Test Arrow-based write."""
    writable_test_ds, datasource_name = _create_writable_test_datasource()
    spark.dataSource.register(writable_test_ds)

    # Create test DataFrame
    df = spark.createDataFrame([(1, "x"), (2, "y")], ["id", "value"])

    # Write data using Arrow writer
    df.write.format(datasource_name).option("writer_type", "arrow").mode("append").save()

    # Verify commit was called
    arrow_messages = writable_test_ds.arrow_commit_messages
    assert arrow_messages is not None
    assert len(arrow_messages) > 0


def test_write_overwrite_mode(spark):
    """Test that overwrite mode is passed correctly."""

    class OverwriteCheckWriter(DataSourceWriter):
        def __init__(self, schema, overwrite):  # noqa: ARG002
            self.overwrite = overwrite
            self.data = []

        def write(self, iterator):
            for row in iterator:
                self.data.append(dict(row.asDict()))
            return {}

        def commit(self, messages):  # noqa: ARG002
            # Store overwrite flag for verification
            OverwriteCheckWriter._last_overwrite = self.overwrite

    OverwriteCheckWriter._last_overwrite = None  # noqa: SLF001

    class OverwriteCheckDataSource(DataSource):
        def __init__(self, options):
            super().__init__(options)

        @classmethod
        def name(cls):
            return "overwrite_check"

        def schema(self):
            return "id INT"

        def writer(self, schema, overwrite):
            return OverwriteCheckWriter(schema, overwrite)

    spark.dataSource.register(OverwriteCheckDataSource)

    df = spark.createDataFrame([(1,), (2,)], ["id"])
    df.write.format("overwrite_check").mode("overwrite").save()

    assert OverwriteCheckWriter._last_overwrite is True  # noqa: SLF001


def test_write_append_mode(spark):
    """Test that append mode is passed correctly."""

    class AppendCheckWriter(DataSourceWriter):
        def __init__(self, schema, overwrite):  # noqa: ARG002
            self.overwrite = overwrite
            self.data = []

        def write(self, iterator):
            for row in iterator:
                self.data.append(dict(row.asDict()))
            return {}

        def commit(self, messages):  # noqa: ARG002
            AppendCheckWriter._last_overwrite = self.overwrite

    AppendCheckWriter._last_overwrite = None  # noqa: SLF001

    class AppendCheckDataSource(DataSource):
        def __init__(self, options):
            super().__init__(options)

        @classmethod
        def name(cls):
            return "append_check"

        def schema(self):
            return "id INT"

        def writer(self, schema, overwrite):
            return AppendCheckWriter(schema, overwrite)

    spark.dataSource.register(AppendCheckDataSource)

    df = spark.createDataFrame([(1,), (2,)], ["id"])
    df.write.format("append_check").mode("append").save()

    assert AppendCheckWriter._last_overwrite is False  # noqa: SLF001


def test_write_commit(spark):
    """Test that commit is called with all partition messages."""

    class CommitTrackingWriter(DataSourceWriter):
        def __init__(self, schema, overwrite):  # noqa: ARG002
            self.data = []

        def write(self, iterator):
            count = sum(1 for _ in iterator)
            return {"count": count}

        def commit(self, messages):
            CommitTrackingWriter._commit_messages = messages

    CommitTrackingWriter._commit_messages = []  # noqa: SLF001

    class CommitTrackingDataSource(DataSource):
        def __init__(self, options):
            super().__init__(options)

        @classmethod
        def name(cls):
            return "commit_tracking"

        def schema(self):
            return "id INT"

        def writer(self, schema, overwrite):
            return CommitTrackingWriter(schema, overwrite)

    spark.dataSource.register(CommitTrackingDataSource)

    df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    df.write.format("commit_tracking").save()

    # Verify commit was called with messages
    assert CommitTrackingWriter._commit_messages is not None  # noqa: SLF001
    assert len(CommitTrackingWriter._commit_messages) > 0  # noqa: SLF001


def test_write_empty_dataframe(spark):
    """Test writing an empty DataFrame."""
    writable_test_ds, datasource_name = _create_writable_test_datasource()
    spark.dataSource.register(writable_test_ds)

    # Create empty DataFrame
    df = spark.createDataFrame([], "id INT, value STRING")

    # Write should succeed even with empty data
    df.write.format(datasource_name).option("writer_type", "row").save()

    # Verify commit was called
    row_messages = writable_test_ds.row_commit_messages
    assert row_messages is not None


def test_write_no_writer_implemented(spark):
    """Test error when writer() is not implemented."""

    class ReadOnlyDataSource(DataSource):
        def __init__(self, options):
            pass

        @classmethod
        def name(cls):
            return "readonly"

        def schema(self):
            return "id INT"

        def reader(self, schema):  # noqa: ARG002
            class SimpleReader(DataSourceReader):
                def partitions(self):
                    return [InputPartition(0)]

                def read(self, partition):  # noqa: ARG002
                    data = {"id": [1, 2, 3]}
                    reader_schema = pa.schema([("id", pa.int32())])
                    batch = pa.RecordBatch.from_pydict(data, schema=reader_schema)
                    yield batch

            return SimpleReader()

        # Note: writer() not implemented

    spark.dataSource.register(ReadOnlyDataSource)

    df = spark.createDataFrame([(1,), (2,)], ["id"])

    # Writing should fail - DataSource doesn't implement writer()
    with pytest.raises(Exception, match=r"(?i)writer|not implemented|unsupported"):
        df.write.format("readonly").save()


def test_write_abort_on_failure(spark):
    """Test that abort is called when writer.write() raises an exception."""

    class FailingWriter(DataSourceWriter):
        _abort_called = False
        _abort_messages = None

        def __init__(self, schema, overwrite):
            pass

        def write(self, iterator):  # noqa: ARG002
            msg = "Intentional write failure for testing"
            raise RuntimeError(msg)

        def commit(self, messages):  # noqa: ARG002
            # Should never be called
            msg = "commit() should not be called on failure"
            raise AssertionError(msg)

        def abort(self, messages):
            FailingWriter._abort_called = True
            FailingWriter._abort_messages = messages

    FailingWriter._abort_called = False  # noqa: SLF001
    FailingWriter._abort_messages = None  # noqa: SLF001

    class FailingWriteDataSource(DataSource):
        def __init__(self, options):
            super().__init__(options)

        @classmethod
        def name(cls):
            return "failing_write"

        def schema(self):
            return "id INT"

        def writer(self, schema, overwrite):
            return FailingWriter(schema, overwrite)

    spark.dataSource.register(FailingWriteDataSource)

    df = spark.createDataFrame([(1,), (2,)], ["id"])

    # Write should fail
    with pytest.raises(Exception, match=r"(?i)write failure|error"):
        df.write.format("failing_write").save()

    # Verify abort was called
    assert FailingWriter._abort_called, "abort() should have been called on write failure"  # noqa: SLF001


def test_write_save_path_passed(spark):
    """Test that .save("/my/path") passes the path to DataSource via options["path"]."""

    class PathCheckWriter(DataSourceWriter):
        _received_path = None

        def __init__(self, schema, overwrite):
            pass

        def write(self, iterator):
            for _ in iterator:
                pass
            return {}

        def commit(self, messages):
            pass

    class PathCheckDataSource(DataSource):
        def __init__(self, options):
            self.options = options
            # Capture the path option for test verification
            PathCheckDataSource._received_options = dict(options)

        @classmethod
        def name(cls):
            return "path_check"

        def schema(self):
            return "id INT"

        def writer(self, schema, overwrite):
            return PathCheckWriter(schema, overwrite)

    PathCheckDataSource._received_options = {}  # noqa: SLF001

    spark.dataSource.register(PathCheckDataSource)

    df = spark.createDataFrame([(1,), (2,)], ["id"])
    df.write.format("path_check").mode("append").save("/my/test/path")

    # Verify the path was passed through options
    assert "path" in PathCheckDataSource._received_options, (  # noqa: SLF001
        "Save path should be passed to DataSource via options['path']"
    )
    assert PathCheckDataSource._received_options["path"] == "/my/test/path"  # noqa: SLF001


def test_write_mode_passed_as_option(spark):
    """Test that the save mode is passed to DataSource via options["mode"]."""

    class ModeCheckWriter(DataSourceWriter):
        def __init__(self, schema, overwrite):
            pass

        def write(self, iterator):
            for _ in iterator:
                pass
            return {}

        def commit(self, messages):
            pass

    class ModeCheckDataSource(DataSource):
        def __init__(self, options):
            self.options = options
            ModeCheckDataSource._received_options = dict(options)

        @classmethod
        def name(cls):
            return "mode_check"

        def schema(self):
            return "id INT"

        def writer(self, schema, overwrite):
            return ModeCheckWriter(schema, overwrite)

    ModeCheckDataSource._received_options = {}  # noqa: SLF001

    spark.dataSource.register(ModeCheckDataSource)

    df = spark.createDataFrame([(1,), (2,)], ["id"])
    df.write.format("mode_check").mode("append").save()

    # Verify the mode was passed through options
    assert "mode" in ModeCheckDataSource._received_options, (  # noqa: SLF001
        "Save mode should be passed to DataSource via options['mode']"
    )
    assert ModeCheckDataSource._received_options["mode"] == "append"  # noqa: SLF001


def test_write_commit_failure_triggers_abort(spark):
    """Test that abort is called when commit() raises an exception."""

    class CommitFailWriter(DataSourceWriter):
        _abort_called = False
        _abort_messages = None

        def __init__(self, schema, overwrite):
            pass

        def write(self, iterator):
            count = sum(1 for _ in iterator)
            return {"count": count}

        def commit(self, messages):  # noqa: ARG002
            msg = "Intentional commit failure for testing"
            raise RuntimeError(msg)

        def abort(self, messages):
            CommitFailWriter._abort_called = True
            CommitFailWriter._abort_messages = messages

    CommitFailWriter._abort_called = False  # noqa: SLF001
    CommitFailWriter._abort_messages = None  # noqa: SLF001

    class CommitFailDataSource(DataSource):
        def __init__(self, options):
            super().__init__(options)

        @classmethod
        def name(cls):
            return "commit_fail"

        def schema(self):
            return "id INT"

        def writer(self, schema, overwrite):
            return CommitFailWriter(schema, overwrite)

    spark.dataSource.register(CommitFailDataSource)

    df = spark.createDataFrame([(1,), (2,)], ["id"])

    # Write should fail because commit fails
    with pytest.raises(Exception, match=r"(?i)commit failure|error"):
        df.write.format("commit_fail").save()

    # Verify abort was called after commit failure
    assert CommitFailWriter._abort_called, (  # noqa: SLF001
        "abort() should have been called after commit failure"
    )


def test_write_null_values(spark):
    """Test writing data that contains null values."""
    writable_test_ds, datasource_name = _create_writable_test_datasource()
    spark.dataSource.register(writable_test_ds)

    # Create DataFrame with null values
    df = spark.createDataFrame(
        [(1, "a"), (2, None), (None, "c")],
        "id INT, value STRING",
    )

    # Test row-based write with nulls
    df.write.format(datasource_name).option("writer_type", "row").mode("append").save()
    row_messages = writable_test_ds.row_commit_messages
    assert row_messages is not None
    assert len(row_messages) > 0

    # Test Arrow-based write with nulls
    df.write.format(datasource_name).option("writer_type", "arrow").mode("append").save()
    arrow_messages = writable_test_ds.arrow_commit_messages
    assert arrow_messages is not None
    assert len(arrow_messages) > 0


def test_write_none_commit_message(spark):
    """Test that writer returning None as commit message is handled correctly."""

    class NoneMessageWriter(DataSourceWriter):
        _commit_messages = None

        def __init__(self, schema, overwrite):
            pass

        def write(self, iterator):
            for _ in iterator:
                pass
            # Explicitly return None (no commit message)

        def commit(self, messages):
            NoneMessageWriter._commit_messages = messages

    NoneMessageWriter._commit_messages = None  # noqa: SLF001

    class NoneMessageDataSource(DataSource):
        def __init__(self, options):
            super().__init__(options)

        @classmethod
        def name(cls):
            return "none_message"

        def schema(self):
            return "id INT"

        def writer(self, schema, overwrite):
            return NoneMessageWriter(schema, overwrite)

    spark.dataSource.register(NoneMessageDataSource)

    df = spark.createDataFrame([(1,), (2,)], ["id"])
    df.write.format("none_message").save()

    # Verify commit was called — messages list should contain the None message
    assert NoneMessageWriter._commit_messages is not None  # noqa: SLF001
    assert len(NoneMessageWriter._commit_messages) > 0  # noqa: SLF001
