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
def data(sail, duck):
    tables = list(duck.sql("SHOW TABLES").df()["name"])
    for table in tables:
        df = duck.sql(f"SELECT * FROM {table}").df()  # noqa: S608
        df_sail = sail.createDataFrame(df)
        df_sail.createOrReplaceTempView(table)
        # df_spark = spark.createDataFrame(df) # doesn't work
        # df_spark.createOrReplaceTempView(table)
    yield
    for table in tables:
        sail.catalog.dropTempView(table)
        # spark.catalog.dropTempView(table)


@pytest.mark.parametrize("query", [f"q{x + 1}" for x in range(99)])
@pytest.mark.skip(reason="TPC-DS queries are not yet fully supported")
def test_tpcds_query_success(sail, query):
    path = Path(__file__).parent.parent.parent / "data" / "tpcds" / "queries" / f"{query}.sql"
    with open(path) as f:
        text = f.read()
    for sql in text.split(";"):
        sql = sql.strip()  # noqa: PLW2901
        if not sql:
            continue
        actual = sail.sql(sql)
        actual.toPandas()


@pytest.mark.parametrize("query", [f"q{x + 1}" for x in range(99)])
@pytest.mark.skip(reason="Don't have full parity with Spark yet")
def test_tpcds_query_spark_parity(sail, spark, query):
    path = Path(__file__).parent.parent.parent / "data" / "tpcds" / "queries" / f"{query}.sql"
    with open(path) as f:
        text = f.read()
    for sql in text.split(";"):
        sql = sql.strip()  # noqa: PLW2901
        if not sql:
            continue
        actual = sail.sql(sql)
        expected = spark.sql()
        assert_frame_equal(actual.toPandas(), expected.toPandas())
