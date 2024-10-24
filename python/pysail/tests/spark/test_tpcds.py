from pathlib import Path

import duckdb
import pytest
from pandas.testing import assert_frame_equal


@pytest.fixture(scope="module")
def duck():
    conn = duckdb.connect()
    conn.sql("CALL dsdgen(sf = 0.01)")
    return conn


@pytest.fixture(scope="module", autouse=True)
def data(sail, spark, duck):
    tables = list(duck.sql("SHOW TABLES").df()["name"])
    for table in tables:
        df = duck.sql(f"SELECT * FROM {table}").df()  # noqa: S608
        sail.createDataFrame(df).createOrReplaceTempView(table)
        spark.createDataFrame(df).createOrReplaceTempView(table)
    yield
    for table in tables:
        sail.catalog.dropTempView(table)
        spark.catalog.dropTempView(table)


@pytest.mark.parametrize("query", [f"q{x + 1}" for x in range(99)])
@pytest.mark.skip(reason="TPC-DS queries are not yet fully supported")
def test_tpcds_query_execution(sail, query):
    for sql in read_sql(query):
        sail.sql(sql).toPandas()


@pytest.mark.parametrize("query", [f"q{x + 1}" for x in range(99)])
@pytest.mark.skip(reason="TPC-DS queries do not have full parity with Spark yet")
def test_tpcds_query_spark_parity(sail, spark, query):
    for sql in read_sql(query):
        actual = sail.sql(sql)
        expected = spark.sql(sql)
        assert_frame_equal(actual.toPandas(), expected.toPandas())


def read_sql(query):
    path = Path(__file__).parent.parent.parent / "data" / "tpcds" / "queries" / f"{query}.sql"
    with open(path) as f:
        text = f.read()
    for sql in text.split(";"):
        sql = sql.strip()  # noqa: PLW2901
        if sql:
            yield sql
