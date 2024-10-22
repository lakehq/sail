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
def data(spark, duck):
    tables = list(duck.sql("SHOW TABLES").df()["name"])
    for table in tables:
        df = duck.sql(f"SELECT * FROM {table}").df()  # noqa: S608
        df = spark.createDataFrame(df)
        df.createOrReplaceTempView(table)
    yield
    for table in tables:
        spark.catalog.dropTempView(table)


@pytest.mark.parametrize("query", [f"q{x + 1}" for x in range(99)])
@pytest.mark.skip(reason="TPC-DS queries are not yet fully supported")
def test_tpcds_query(spark, duck, query):
    path = Path(__file__).parent.parent.parent / "data" / "tpcds" / "queries" / f"{query}.sql"
    with open(path) as f:
        text = f.read()
    for sql in text.split(";"):
        sql = sql.strip()  # noqa: PLW2901
        if not sql:
            continue
        # Spark and DuckDB has different syntax for quoted identifiers.
        #   Spark: SELECT 1 AS `v`
        #   DuckDB: SELECT 1 AS "v"
        # In TPC-DS, quoted identifiers are used when column names contain spaces.
        actual = spark.sql(sql)
        expected = duck.sql(sql.replace("`", '"'))
        assert_frame_equal(actual.toPandas(), expected.df())
