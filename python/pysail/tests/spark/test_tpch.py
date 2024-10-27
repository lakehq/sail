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
        df = duck.sql(f"SELECT * FROM {table}").arrow().to_pandas()  # noqa: S608
        sail.createDataFrame(df).createOrReplaceTempView(table)
        spark.createDataFrame(df).createOrReplaceTempView(table)
    yield
    for table in tables:
        sail.catalog.dropTempView(table)
        spark.catalog.dropTempView(table)


@pytest.mark.parametrize("query", [f"q{x + 1}" for x in range(22)])
def test_derived_tpch_query_execution(sail, query):
    for sql in read_sql(query):
        sail.sql(sql).toPandas()


@pytest.mark.parametrize("query", [f"q{x + 1}" for x in range(22)])
@pytest.mark.skip(reason="Derived TPC-H queries do not have full parity with Spark yet")
def test_derived_tpch_query_spark_parity(sail, spark, query):
    for sql in read_sql(query):
        actual = sail.sql(sql)
        expected = spark.sql(sql)
        if is_ddl(sql):
            continue
        assert_frame_equal(actual.toPandas(), expected.toPandas())


def read_sql(query):
    path = Path(__file__).parent.parent.parent / "data" / "tpch" / "queries" / f"{query}.sql"
    with open(path) as f:
        text = f.read()
    for sql in text.split(";"):
        sql = sql.strip()  # noqa: PLW2901
        sql = sql.replace("create view", "create temp view")  # noqa: PLW2901
        if sql:
            yield sql


def is_ddl(sql):
    return any(x in sql for x in ("create view", "create temp view", "drop view"))
