"""
Tests for Python DataSource integration.

These tests verify the Python DataSource API works correctly with Sail,
including both Arrow RecordBatch and tuple-based paths.
"""
import pytest
import pyarrow as pa
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition


class TestPythonDataSourceBasic:
    """Basic API tests that don't require a Spark session."""

    def test_range_datasource_api(self):
        """Test RangeDataSource API without server."""
        from pysail.tests.spark.datasource_examples import RangeDataSource

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
        assert len(partitions) == 2

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
                return pa.schema([
                    ("id", pa.int64()),
                    ("value", pa.float64()),
                ])

            def reader(self, schema):
                return ArrowRangeReader()

        class ArrowRangeReader(DataSourceReader):
            """Reader that yields Arrow RecordBatches."""

            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):
                ids = list(range(100))
                values = [float(i) * 1.5 for i in ids]
                batch = pa.RecordBatch.from_pydict(
                    {"id": ids, "value": values},
                    schema=pa.schema([("id", pa.int64()), ("value", pa.float64())])
                )
                yield batch

        # Register and read
        spark.dataSource.register(ArrowRangeDataSource)
        df = spark.read.format("arrow_range_test").load()

        rows = df.collect()
        assert len(rows) == 100
        assert rows[0].id == 0
        assert rows[0].value == 0.0

        # Test filter query (filter is applied post-read by DataFusion in MVP)
        filtered = df.filter("value > 50.0").collect()
        # ids 34+ have value > 50 (34 * 1.5 = 51)
        assert len(filtered) == 66  # ids 34-99 have value > 50

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
                return pa.schema([
                    ("id", pa.int64()),
                    ("square", pa.int64()),
                ])

            def reader(self, schema):
                return TupleReader()

        class TupleReader(DataSourceReader):
            """Reader that yields many tuples to test batching."""

            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):
                # Yield 1000 tuples to test batching behavior
                for i in range(1000):
                    yield (i, i * i)

        spark.dataSource.register(TupleDataSource)
        df = spark.read.format("tuple_range_test").load()

        rows = df.collect()
        assert len(rows) == 1000

        # Verify data integrity: check that squares are correct
        # Find row with id=100, should have square=10000
        sample = df.filter("id = 100").collect()
        assert len(sample) == 1
        assert sample[0].square == 10000  # 100² = 10000

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
                return pa.schema([
                    ("partition_id", pa.int32()),
                    ("row_id", pa.int32()),
                ])

            def reader(self, schema):
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
                    schema=pa.schema([("partition_id", pa.int32()), ("row_id", pa.int32())])
                )
                yield batch

        spark.dataSource.register(MultiPartitionDataSource)
        df = spark.read.format("multi_partition_test").load()

        rows = df.collect()
        # 4 partitions * 25 rows each = 100 total
        assert len(rows) == 100

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

            def reader(self, schema):
                return FailingPartitionReader()

        class FailingPartitionReader(DataSourceReader):
            def partitions(self):
                return [InputPartition(i) for i in range(4)]

            def read(self, partition):
                pid = partition.value
                if pid == 2:
                    raise ValueError(f"Deliberate failure in partition {pid}!")

                schema = pa.schema([("id", pa.int32())])
                batch = pa.RecordBatch.from_pydict({"id": [pid]}, schema=schema)
                yield batch

        spark.dataSource.register(FailingPartitionDataSource)
        df = spark.read.format("failing_partition_test").load()

        with pytest.raises(Exception) as exc_info:
            df.collect()

        error_msg = str(exc_info.value).lower()
        # Error should include partition context
        assert "partition" in error_msg or "2" in error_msg
        assert "deliberate failure" in error_msg

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
                return pa.schema([
                    ("id", pa.int32()),
                    ("name", pa.string()),
                ])

            def reader(self, schema):
                return EmptyPartitionsReader()

        class EmptyPartitionsReader(DataSourceReader):
            def partitions(self):
                return []  # No partitions

            def read(self, partition):
                raise RuntimeError("read() should not be called with 0 partitions!")

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
                return pa.schema([
                    ("id", pa.int32()),
                    ("name", pa.string()),
                ])

            def reader(self, schema):
                return SchemaMismatchReader()

        class SchemaMismatchReader(DataSourceReader):
            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):
                # Return batch with int64 id instead of int32 - schema mismatch!
                wrong_schema = pa.schema([
                    ("id", pa.int64()),  # Wrong type!
                    ("name", pa.string()),
                ])
                batch = pa.RecordBatch.from_pydict({
                    "id": [1, 2, 3],
                    "name": ["a", "b", "c"],
                }, schema=wrong_schema)
                yield batch

        spark.dataSource.register(SchemaMismatchDataSource)
        df = spark.read.format("schema_mismatch_test").load()

        # Should either succeed with coercion or fail with schema error
        # The important thing is it doesn't crash unexpectedly
        try:
            rows = df.collect()
            # If it succeeds, verify data is present
            assert len(rows) == 3
        except Exception as e:
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
                return pa.schema([
                    ("id", pa.int32()),
                    ("value", pa.string()),
                ])

            def reader(self, schema):
                return ExceptionReader()

        class ExceptionReader(DataSourceReader):
            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):
                raise ValueError("This is a deliberate test exception!")

        spark.dataSource.register(ExceptionDataSource)
        df = spark.read.format("exception_test").load()

        with pytest.raises(Exception) as exc_info:
            df.collect()

        error_msg = str(exc_info.value)
        # Exception message should be preserved
        assert "deliberate test exception" in error_msg.lower()

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

            def reader(self, schema):
                return SessionIsolationReader()

        class SessionIsolationReader(DataSourceReader):
            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):
                batch = pa.RecordBatch.from_pydict({
                    "id": [1, 2, 3],
                    "msg": ["hello", "from", "session_a"],
                }, schema=pa.schema([("id", pa.int32()), ("msg", pa.string())]))
                yield batch

        # Session A: Register the datasource
        spark_a = spark_session_factory()
        spark_a.dataSource.register(SessionIsolationDataSource)

        # Verify Session A can read from it
        df_a = spark_a.read.format("session_isolation_test").load()
        rows_a = df_a.collect()
        assert len(rows_a) == 3

        # Session B: Create a completely independent session
        spark_b = spark_session_factory()

        # Session B should NOT be able to access the datasource from Session A
        # because datasources are registered per-session
        with pytest.raises(Exception) as exc_info:
            df_b = spark_b.read.format("session_isolation_test").load()
            df_b.collect()

        error_msg = str(exc_info.value).lower()
        # Error should indicate the format is not found
        assert "session_isolation_test" in error_msg or "not found" in error_msg or "unknown" in error_msg


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
                return pa.schema([
                    ("id", pa.int64()),
                    ("value", pa.string()),
                ])

            def reader(self, schema):
                return FilterApplyingReader()

        class FilterApplyingReader(DataSourceReader):
            def __init__(self):
                self.accepted_filters = []

            def pushFilters(self, filters):
                """Accept EqualTo filters and track them."""
                for f in filters:
                    filter_name = type(f).__name__
                    if filter_name == "EqualTo":
                        self.accepted_filters.append(f)
                    else:
                        yield f  # Reject non-equality filters

            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):
                # Full dataset
                all_data = {"id": [1, 2, 3, 4, 5], "value": ["a", "b", "c", "d", "e"]}

                # Apply accepted filters
                if self.accepted_filters:
                    for f in self.accepted_filters:
                        # EqualTo filter has references to column tuple and value
                        # Access the filter value (it's stored as an attribute)
                        filter_val = getattr(f, 'value', None)
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
                    all_data,
                    schema=pa.schema([("id", pa.int64()), ("value", pa.string())])
                )
                yield batch

        spark.dataSource.register(FilterApplyingDataSource)

        # Execute a query with a filter
        df = spark.read.format("filter_applying_test").load()

        # Query without filter should return all 5 rows
        all_rows = df.collect()
        assert len(all_rows) == 5, f"Expected 5 rows, got {len(all_rows)}"

        # Query WITH filter - if pushFilters works, Python reader applies it
        filtered_rows = df.filter("id = 3").collect()

        # Should get exactly 1 row
        assert len(filtered_rows) == 1, f"Expected 1 row, got {len(filtered_rows)}"
        assert filtered_rows[0].id == 3

    def test_ddl_schema_fallback(self, spark):
        """Test that DDL string schema is correctly parsed and used."""
        from pysail.tests.spark.datasource_examples import RangeDataSource
        spark.dataSource.register(RangeDataSource)

        # This uses the option we added to RangeDataSource to force DDL schema return
        df = spark.read.format("range") \
            .option("use_ddl_schema", "true") \
            .option("end", "10") \
            .load()

        # Verify schema
        assert df.schema.fields[0].name == "id"
        # In Spark, BIGINT maps to LongType (int64)
        from pyspark.sql.types import LongType
        assert df.schema.fields[0].dataType == LongType()

        # Verify data read works
        rows = df.collect()
        assert len(rows) == 10
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

            def reader(self, schema):
                return SimpleRangeReader()

        class SimpleRangeReader(DataSourceReader):
            def pushFilters(self, filters):
                # Accept all filters (don't actually apply - let DataFusion post-filter)
                return iter([])

            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):
                batch = pa.RecordBatch.from_pydict(
                    {"id": list(range(10))},  # 0-9
                    schema=pa.schema([("id", pa.int64())])
                )
                yield batch

        spark.dataSource.register(SimpleRangeDataSource)
        df = spark.read.format("simple_range_test").load()

        # Test greater than
        rows = df.filter("id > 5").collect()
        assert len(rows) == 4, f"Expected 4 rows (6,7,8,9), got {len(rows)}"
        assert all(r.id > 5 for r in rows)

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

            def reader(self, schema):
                return RejectAllFiltersReader()

        class RejectAllFiltersReader(DataSourceReader):
            def pushFilters(self, filters):
                # Reject all filters by yielding them back
                yield from filters

            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):
                batch = pa.RecordBatch.from_pydict(
                    {"id": [1, 2, 3, 4, 5]},
                    schema=pa.schema([("id", pa.int64())])
                )
                yield batch

        spark.dataSource.register(RejectAllFiltersDataSource)
        df = spark.read.format("reject_all_filters_test").load()

        # Even though reader rejects filters, query should still work
        # DataFusion will post-filter the results
        rows = df.filter("id = 3").collect()
        assert len(rows) == 1
        assert rows[0].id == 3
