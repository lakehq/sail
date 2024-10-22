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
def data(spark, duck):
    tables = list(duck.sql("SHOW TABLES").df()["name"])
    for table in tables:
        df = duck.sql(f"SELECT * FROM {table}").df()  # noqa: S608
        df = spark.createDataFrame(df)
        df.createOrReplaceTempView(table)
    yield
    for table in tables:
        spark.catalog.dropTempView(table)


@pytest.mark.parametrize("query", [x + 1 for x in range(22)])
@pytest.mark.skip(reason="TPC-H queries are not yet fully supported")
def test_tpch_query(spark, duck, query):
    path = Path(__file__).parent.parent.parent / "data" / "tpch" / "queries" / f"q{query}.sql"
    with open(path) as f:
        text = f.read()
    for sql in text.split(";"):
        sql = sql.strip()  # noqa: PLW2901
        if not sql:
            continue
        actual = spark.sql(sql.replace("create view", "create temp view"))
        expected = duck.sql(sql)
        if "create view" in sql or "drop view" in sql:
            continue
        assert_frame_equal(actual.toPandas(), expected.df())
