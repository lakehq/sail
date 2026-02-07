import pytest
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
import pyarrow as pa


class FilterTrackingDataSource(DataSource):
    """DataSource that tracks pushed filters."""

    @classmethod
    def name(cls):
        return "filter_tracking_test"

    def schema(self):
        return pa.schema(
            [
                ("id", pa.int32()),
                ("name", pa.string()),
                ("value", pa.int32()),
            ]
        )

    def reader(self, schema):
        return FilterTrackingReader()


class FilterTrackingReader(DataSourceReader):
    def __init__(self):
        self.pushed_filters = []

    def partitions(self):
        return [InputPartition(0)]

    def read(self, partition):
        # Return data that matches the filters to ensure correct results
        # But primarily we want to check self.pushed_filters
        # Since read() is called likely in a different process/thread, we can't easily
        # inspect self.pushed_filters from the test driver if it's not communicated back.
        # Sail executes Python in-process (for now), but in a separate thread.
        # However, the Reader instance is created in the worker thread.

        # To verify filters, we can encode the received filters into the data
        # or raise an exception with the filters if we want to debug.
        # Let's return the filters as a string in the 'name' column.

        filters_str = str(self.pushed_filters)

        # Generate some rows
        data = {"id": [1, 2, 3], "name": [filters_str, filters_str, filters_str], "value": [10, 20, 30]}
        schema = pa.schema(
            [
                ("id", pa.int32()),
                ("name", pa.string()),
                ("value", pa.int32()),
            ]
        )
        batch = pa.RecordBatch.from_pydict(data, schema=schema)
        yield batch

    def pushFilters(self, filters):
        # Store pushed filters
        # filters is a list of Filter objects
        self.pushed_filters = [str(f) for f in filters]
        # Return unhandled filters (empty means all handled)
        return []


@pytest.mark.filterwarnings("ignore")
def test_filter_pushdown(spark):
    spark.dataSource.register(FilterTrackingDataSource)

    # query with a filter
    df = spark.read.format("filter_tracking_test").load()
    filtered_df = df.filter("value > 15")

    # Collect result
    rows = filtered_df.collect()

    # Check if filters were pushed
    # The 'name' column should contain the string representation of pushed filters
    assert len(rows) > 0
    pushed_filters_str = rows[0].name
    print(f"Pushed filters: {pushed_filters_str}")

    assert "GreaterThan" in pushed_filters_str
    assert "value" in pushed_filters_str
    assert "15" in pushed_filters_str


def test_filter_pushdown_complex(spark):
    spark.dataSource.register(FilterTrackingDataSource)

    # complex query
    df = spark.read.format("filter_tracking_test").load()
    filtered_df = df.filter("value > 15 AND id < 3")

    rows = filtered_df.collect()
    pushed_filters_str = rows[0].name
    print(f"Pushed filters complex: {pushed_filters_str}")

    # Should see both filters (And is not pushed as a single object usually, but list of filters?)
    # PySpark pushFilters receives a list of filters.
    # Sail implementation converts Exprs to list of Python Filters.

    assert "GreaterThan" in pushed_filters_str or "LessThan" in pushed_filters_str
