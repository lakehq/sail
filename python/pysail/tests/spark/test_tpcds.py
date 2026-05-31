import dataclasses
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
    tables = list(duck.sql("SHOW TABLES").df()["name"])
    for table in tables:
        df = duck.sql(f"SELECT * FROM {table}").fetch_arrow_table().to_pandas()  # noqa: S608
        spark.createDataFrame(df).createOrReplaceTempView(table)
    yield
    for table in tables:
        spark.catalog.dropTempView(table)


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
        plan = _edit_tpcds_plan(query, plan)
        assert plan == snapshot


def read_sql(query):
    path = Path(__file__).parent.parent.parent / "data" / "tpcds" / "queries" / f"{query}.sql"
    with open(path) as f:
        text = f.read()
    for sql in text.split(";"):
        sql = sql.strip()  # noqa: PLW2901
        if sql:
            yield sql


@dataclasses.dataclass
class PlanEditRule:
    query: str
    source: str
    target: str


TPC_DS_PLAN_EDIT_RULES: list[PlanEditRule] = []


def _edit_tpcds_plan(query: str, plan: str) -> str:
    for rule in TPC_DS_PLAN_EDIT_RULES:
        if rule.query == query:
            plan = plan.replace(rule.source, rule.target)
            # The source string may or may not be present in the plan before replacement.
            # But if the target string is missing in the plan after replacement,
            # the plan edit rule is known to be outdated and should be updated or removed.
            assert rule.target in plan, f"edit rule not applied: {rule}"
    return plan
