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
def data(sail, spark, duck):  # noqa: ARG001
    tables = list(duck.sql("SHOW TABLES").df()["name"])
    for table in tables:
        df = duck.sql(f"SELECT * FROM {table}").arrow().to_pandas()  # noqa: S608
        sail.createDataFrame(df).createOrReplaceTempView(table)
        # spark.createDataFrame(df).createOrReplaceTempView(table)
    yield
    for table in tables:
        sail.catalog.dropTempView(table)
        # spark.catalog.dropTempView(table)


@pytest.mark.parametrize("query", [f"q{x + 1}" for x in range(99)])
def test_derived_tpcds_query_execution(sail, query):
    # Skip unsupported queries to ensure continued support for the supported ones.
    skip = {
        "q12",
        "q16",
        "q20",
        "q23",
        "q27",
        "q36",
        "q47",
        "q51",
        "q53",
        "q57",
        "q63",
        "q70",
        "q72",
        "q86",
        "q89",
        "q91",
        "q92",
        "q94",
        "q95",
        "q98",
    }
    if query in skip:
        pytest.skip(f"Derived TPC-DS queries are not yet fully supported Skipping unsupported query: {query}")

    for sql in read_sql(query):
        try:
            sail.sql(sql).toPandas()
        except Exception as e:
            err = f"Error executing query {query} with error: {e}\n SQL: {sql}"
            raise Exception(err) from e  # noqa: TRY002


@pytest.mark.parametrize("query", [f"q{x + 1}" for x in range(99)])
@pytest.mark.skip(reason="Derived TPC-DS queries do not have full parity with Spark yet")
def test_derived_tpcds_query_spark_parity(sail, spark, query):
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
