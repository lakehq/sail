import re
from collections.abc import Iterator
from uuid import uuid4

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark

try:
    from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
    from pyspark.sql.types import IntegerType, StructField, StructType
except ImportError:
    pytest.skip("Python DataSource API is not available in this Spark version", allow_module_level=True)


@pytest.mark.skipif(is_jvm_spark(), reason="Sail physical plans")
def test_python_data_source_table_is_read_for_each_reference(spark, tmp_path):
    """Python data source reads may have side effects, so repeated references do not share a source."""
    table_name = f"sql_reads_{uuid4().hex[:8]}"
    counter = str(tmp_path / "reads.txt")

    class CountingDataSource(DataSource):
        @classmethod
        def name(cls) -> str:
            return table_name

        def schema(self) -> StructType:
            return StructType([StructField("x", IntegerType())])

        def reader(self, schema: StructType) -> DataSourceReader:  # noqa: ARG002
            return CountingReader()

    class CountingReader(DataSourceReader):
        def read(self, partition: InputPartition) -> Iterator[tuple]:  # noqa: ARG002
            with open(counter, "a") as f:
                f.write("read\n")
            yield (7,)

    spark.dataSource.register(CountingDataSource)
    spark.sql(f"CREATE TABLE {table_name} USING {table_name}")
    try:
        df = spark.sql(f"SELECT (SELECT x FROM {table_name}) AS a, (SELECT x FROM {table_name}) AS b")  # noqa: S608
        plan = df._explain_string()  # noqa: SLF001
        assert re.findall(r"ScalarSubqueryExec: subqueries=(\d+)", plan) == ["2"]
        assert [tuple(row) for row in df.collect()] == [(7, 7)]
        with open(counter) as f:
            assert f.read().splitlines() == ["read", "read"]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
