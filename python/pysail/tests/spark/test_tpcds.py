from pathlib import Path

import duckdb
import pytest
from syrupy.assertion import SnapshotAssertion

from pysail.tests.spark.conftest import PlanSnapshotExtension

from .utils import is_jvm_spark  # noqa: TID252


@pytest.fixture(scope="module")
def duck():
    conn = duckdb.connect()
    conn.sql("CALL dsdgen(sf = 0.01)")
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


@pytest.mark.parametrize("query", [f"q{x + 1}" for x in range(99)])
@pytest.mark.skipif(is_jvm_spark(), reason="slow tests in JVM Spark")
def test_derived_tpcds_query_explain_matches_snapshot(spark, query, snapshot: SnapshotAssertion):
    explained = False
    for sql in read_sql(query):
        plan_rows = spark.sql(f"EXPLAIN {sql}").collect()
        assert len(plan_rows) == 1, f"expected single row, got {len(plan_rows)}"
        plan = plan_rows[0][0]
        assert snapshot(extension_class=PlanSnapshotExtension) == plan
        explained = True

    assert explained, f"expected at least one statement in {query}.sql"


@pytest.mark.parametrize("query", [f"q{x + 1}" for x in range(99)])
@pytest.mark.skipif(is_jvm_spark(), reason="slow tests in JVM Spark")
def test_derived_tpcds_query_execution(spark, query):
    # TODO: add tests for result parity
    for sql in read_sql(query):
        spark.sql(sql).toPandas()


def read_sql(query):
    path = Path(__file__).parent.parent.parent / "data" / "tpcds" / "queries" / f"{query}.sql"
    with open(path) as f:
        text = f.read()
    for sql in text.split(";"):
        sql = sql.strip()  # noqa: PLW2901
        if sql:
            yield sql
