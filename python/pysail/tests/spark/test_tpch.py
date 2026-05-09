from pathlib import Path

import duckdb
import pytest

from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.testing.spark.utils.sql import format_show_string, parse_show_string


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


@pytest.mark.parametrize("query", [f"q{x + 1}" for x in range(22)], ids=[f"{x + 1:02}" for x in range(22)])
@pytest.mark.skipif(is_jvm_spark(), reason="slow tests in JVM Spark")
@pytest.mark.yamlsnapshot(group="result")
def test_derived_tpch_query_result(spark, query, snapshot):
    for sql in read_sql(query):
        result = spark.sql(sql)._show_string(n=0x7FFFFFFF, truncate=False)  # noqa: SLF001
        table = format_show_string(parse_show_string(result))
        assert table == snapshot


@pytest.mark.parametrize("query", [f"q{x + 1}" for x in range(22)], ids=[f"{x + 1:02}" for x in range(22)])
@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_derived_tpch_query_plan(spark, query, snapshot):
    for sql in read_sql(query):
        # `spark.sql` will have the side effect for DDL statements.
        # This is needed so that the temporary view is created for subsequent queries.
        plan = normalize_plan_text(spark.sql(sql)._explain_string())  # noqa: SLF001
        assert plan == snapshot


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
