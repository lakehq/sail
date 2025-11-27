from pathlib import Path

import duckdb
import pytest

from pysail.tests.spark.utils import is_jvm_spark


@pytest.fixture(scope="module")
def duck():
    conn = duckdb.connect()
    conn.sql("CALL dbgen(sf = 0.001)")
    return conn


@pytest.fixture(scope="module", autouse=True)
def data(spark, duck):
    tables = list(duck.sql("SHOW TABLES").df()["name"])
    for table in tables:
        df = duck.sql(f"SELECT * FROM {table}").fetch_arrow_table().to_pandas()  # noqa: S608
        spark.createDataFrame(df).createOrReplaceTempView(table)
    yield
    for table in tables:
        spark.catalog.dropTempView(table)


@pytest.mark.parametrize("query", [f"q{x + 1}" for x in range(22)])
@pytest.mark.skipif(is_jvm_spark(), reason="slow tests in JVM Spark")
def test_derived_tpch_query_execution(spark, query):
    # TODO: add tests for result parity
    for sql in read_sql(query):
        spark.sql(sql).toPandas()


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
