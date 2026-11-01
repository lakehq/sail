import re
from collections.abc import Iterator
from uuid import uuid4

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.testing.spark.utils.sql import escape_sql_string_literal

try:
    from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
    from pyspark.sql.types import IntegerType, StructField, StructType
except ImportError:
    pytest.skip("Python DataSource API is not available in this Spark version", allow_module_level=True)

SCALAR_SUBQUERY_SLOTS = re.compile(r"ScalarSubqueryExec: subqueries=(\d+)")


def scalar_subquery_slots(df):
    return [int(n) for n in SCALAR_SUBQUERY_SLOTS.findall(df._explain_string())]  # noqa: SLF001


@pytest.fixture
def counting_table(spark, tmp_path):
    """Returns (table, counter path) for a Python data source table that logs each read."""
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
    yield table_name, counter
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def reads(counter):
    try:
        with open(counter) as f:
            return len(f.read().splitlines())
    except FileNotFoundError:
        return 0


@pytest.mark.skipif(is_jvm_spark(), reason="Sail physical plans")
@pytest.mark.parametrize(
    "query",
    [
        pytest.param("SELECT (SELECT x FROM {t}) AS a, (SELECT x FROM {t}) AS b", id="table"),
        pytest.param("WITH c AS (SELECT x FROM {t}) SELECT (SELECT x FROM c) AS a, (SELECT x FROM c) AS b", id="cte"),
        pytest.param(
            "WITH c AS (SELECT x FROM {t}) SELECT (SELECT sum(x) FROM c) AS a, (SELECT sum(x) FROM c) AS b",
            id="cte-aggregate",
        ),
        pytest.param("SELECT (SELECT x FROM {v}) AS a, (SELECT x FROM {v}) AS b", id="view"),
        pytest.param(
            "WITH c AS (SELECT x FROM {v}) SELECT (SELECT x FROM c) AS a, (SELECT x FROM c) AS b", id="view-cte"
        ),
    ],
)
def test_python_data_source_table_is_read_for_each_reference(spark, counting_table, query):
    """Python data source reads may have side effects, so repeated references do not share a source.

    This holds for references through a CTE or a temporary view too, which Spark also reads once
    per reference.
    """
    table_name, counter = counting_table
    view_name = f"{table_name}_view"
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {view_name} AS SELECT x FROM {table_name}")  # noqa: S608
    try:
        df = spark.sql(query.format(t=table_name, v=view_name))
        assert scalar_subquery_slots(df) == [2]
        assert [tuple(row) for row in df.collect()] == [(7, 7)]
        assert reads(counter) == 2  # noqa: PLR2004
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {view_name}")


@pytest.mark.skipif(is_jvm_spark(), reason="Sail physical plans")
@pytest.mark.parametrize("read", ["table", "format"])
def test_python_data_source_dataframe_view_is_read_for_each_reference(spark, counting_table, read):
    table_name, counter = counting_table
    view_name = f"{table_name}_df_view"
    df = spark.table(table_name) if read == "table" else spark.read.format(table_name).load()
    df.createOrReplaceTempView(view_name)
    try:
        df = spark.sql(f"SELECT (SELECT x FROM {view_name}) AS a, (SELECT x FROM {view_name}) AS b")  # noqa: S608
        assert scalar_subquery_slots(df) == [2]
        assert [tuple(row) for row in df.collect()] == [(7, 7)]
        assert reads(counter) == 2  # noqa: PLR2004
    finally:
        spark.catalog.dropTempView(view_name)


@pytest.mark.skipif(is_jvm_spark(), reason="Sail physical plans")
@pytest.mark.parametrize(
    ("query", "slots", "expected_reads"),
    [
        # Reading a Python data source anywhere in the body keeps the subqueries separate.
        pytest.param(
            "WITH c AS (SELECT x + w AS x FROM {t} CROSS JOIN {p}) "
            "SELECT (SELECT x FROM c) AS a, (SELECT x FROM c) AS b",
            [2],
            2,
            id="mixed-cte",
        ),
        # Bodies that read only shareable sources are still shared.
        pytest.param(
            "WITH c AS (SELECT w AS x FROM {p}) SELECT (SELECT x + 7 FROM c) AS a, (SELECT x + 7 FROM c) AS b",
            [1],
            0,
            id="parquet-cte",
        ),
    ],
)
def test_python_data_source_is_checked_in_every_scan(spark, counting_table, tmp_path, query, slots, expected_reads):
    table_name, counter = counting_table
    parquet_name = f"{table_name}_parquet"
    location = escape_sql_string_literal(str(tmp_path / parquet_name))
    spark.sql(f"CREATE TABLE {parquet_name} (w INT) USING parquet LOCATION '{location}'")
    spark.sql(f"INSERT INTO {parquet_name} VALUES (0)")  # noqa: S608
    try:
        df = spark.sql(query.format(t=table_name, p=parquet_name))
        assert scalar_subquery_slots(df) == slots
        assert [tuple(row) for row in df.collect()] == [(7, 7)]
        assert reads(counter) == expected_reads
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {parquet_name}")
