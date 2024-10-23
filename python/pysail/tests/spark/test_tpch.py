from pathlib import Path

import duckdb
import pytest
from pandas.testing import assert_frame_equal


@pytest.fixture(scope="module")
def duck():
    conn = duckdb.connect()
    conn.sql("CALL dbgen(sf = 0.001)")
    return conn


@pytest.fixture(scope="module", autouse=True)
def data(sail, spark, duck):
    tables = list(duck.sql("SHOW TABLES").df()["name"])
    for table in tables:
        df = duck.sql(f"SELECT * FROM {table}").df()  # noqa: S608
        df_sail = sail.createDataFrame(df)
        df_sail.createOrReplaceTempView(table)
        df_spark = spark.createDataFrame(df)
        df_spark.createOrReplaceTempView(table)
    yield
    for table in tables:
        sail.catalog.dropTempView(table)
        spark.catalog.dropTempView(table)


@pytest.mark.parametrize("query", [f"q{x + 1}" for x in range(22)])
def test_tpch_query_success(sail, query):
    path = Path(__file__).parent.parent.parent / "data" / "tpch" / "queries" / f"{query}.sql"
    with open(path) as f:
        text = f.read()
    for sql in text.split(";"):
        sql = sql.strip()  # noqa: PLW2901
        if not sql:
            continue
        actual = sail.sql(sql.replace("create view", "create temp view"))
        if "create view" in sql or "drop view" in sql:
            continue
        actual.toPandas()


@pytest.mark.parametrize("query", [f"q{x + 1}" for x in range(22)])
@pytest.mark.skip(reason="Don't have full parity with Spark yet")
def test_tpch_query_spark_parity(sail, spark, query):
    path = Path(__file__).parent.parent.parent / "data" / "tpch" / "queries" / f"{query}.sql"
    with open(path) as f:
        text = f.read()
    for sql in text.split(";"):
        sql = sql.strip()  # noqa: PLW2901
        if not sql:
            continue
        actual = sail.sql(sql.replace("create view", "create temp view"))
        expected = spark.sql(sql)
        if "create view" in sql or "drop view" in sql:
            continue
        assert_frame_equal(actual.toPandas(), expected.toPandas())
