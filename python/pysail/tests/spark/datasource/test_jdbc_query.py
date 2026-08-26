from __future__ import annotations

import pyarrow as pa
import pytest

from pysail.testing.spark.utils.common import pyspark_version

if pyspark_version() < (4, 1):
    pytest.skip("Python data source requires Spark 4.1+", allow_module_level=True)


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("SELECT 1;", "SELECT 1"),
        (" SELECT 1; \n", "SELECT 1"),
        ("SELECT 1;;;", "SELECT 1"),
        ("SELECT 1", "SELECT 1"),
        ("SELECT ';' AS x;", "SELECT ';' AS x"),
    ],
)
def test_query_option_matches_spark_normalization(query, expected):
    from pysail.spark.datasource.jdbc import JdbcDataSource

    datasource = JdbcDataSource(options={"url": "jdbc:postgresql://localhost/test", "query": query})

    assert datasource._resolve_options()["query"] == expected  # noqa: SLF001


def test_schema_and_execution_use_the_normalized_query(monkeypatch):
    from pyspark.sql.datasource import GreaterThan

    from pysail.spark.datasource import jdbc

    schema_queries = []

    def read_sql(_conn_str, query, *, return_type):
        assert return_type == "arrow"
        schema_queries.append(query)
        return pa.table({"n": pa.array([], type=pa.int64())})

    monkeypatch.setattr(jdbc.cx, "read_sql", read_sql)
    datasource = jdbc.JdbcDataSource(options={"url": "jdbc:postgresql://localhost/test", "query": " SELECT 1 AS n; \n"})

    schema = datasource.schema()
    reader = datasource.reader(schema)
    assert list(reader.pushFilters([GreaterThan(("n",), 0)])) == []
    partition = reader.partitions()[0]

    assert schema_queries == ["SELECT * FROM (SELECT 1 AS n) AS _cx_schema_q LIMIT 0"]
    assert ";) AS _cx_schema_q" not in schema_queries[0]
    assert partition.query == 'SELECT * FROM (SELECT 1 AS n) AS _cx_subq WHERE "n" > 0'
