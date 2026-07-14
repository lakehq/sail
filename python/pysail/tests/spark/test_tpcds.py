from pathlib import Path

import duckdb
import pytest

from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.testing.spark.utils.sql import (
    format_show_string,
    normalize_floating_point_string,
    parse_show_string,
)


@pytest.fixture(scope="module")
def duck():
    conn = duckdb.connect()
    conn.sql("CALL dsdgen(sf = 0.01)")
    return conn


@pytest.fixture(scope="module", autouse=True)
def data(spark, duck):
    threshold_key = "spark.sql.session.localRelationCacheThreshold"
    previous_threshold = spark.conf.get(threshold_key)
    spark.conf.set(threshold_key, "2147483647")
    tables = list(duck.sql("SHOW TABLES").df()["name"])
    try:
        for table in tables:
            df = duck.sql(f"SELECT * FROM {table}").fetch_arrow_table().to_pandas()  # noqa: S608
            spark.createDataFrame(df).createOrReplaceTempView(table)
        yield
    finally:
        for table in tables:
            spark.catalog.dropTempView(table)
        spark.conf.set(threshold_key, previous_threshold)


@pytest.mark.parametrize("query", [f"q{x + 1}" for x in range(99)], ids=[f"{x + 1:02}" for x in range(99)])
@pytest.mark.skipif(is_jvm_spark(), reason="slow tests in JVM Spark")
@pytest.mark.yamlsnapshot(group="result")
def test_derived_tpcds_query_result(spark, query, snapshot):
    for sql in read_sql(query):
        result = spark.sql(sql)._show_string(n=0x7FFFFFFF, truncate=False)  # noqa: SLF001
        table = format_show_string(
            parse_show_string(result),
            normalizer=lambda x: normalize_floating_point_string(x, d=6, n=4),
        )
        assert table == snapshot


@pytest.mark.parametrize("query", [f"q{x + 1}" for x in range(99)], ids=[f"{x + 1:02}" for x in range(99)])
@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_derived_tpcds_query_plan(spark, query, snapshot):
    for sql in read_sql(query):
        plan = normalize_plan_text(spark.sql(sql)._explain_string())  # noqa: SLF001
        assert plan == snapshot


def read_sql(query):
    path = Path(__file__).parent.parent.parent / "data" / "tpcds" / "queries" / f"{query}.sql"
    with open(path) as f:
        text = f.read()
    for sql in text.split(";"):
        sql = sql.strip()  # noqa: PLW2901
        if sql:
            yield sql
