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


TPC_DS_PLAN_EDIT_RULES = [
    # DataFusion `LogicalPlanBuilder::sort()` adds missing columns in a non-deterministic order
    # (possibly due to `HashSet` returned by `Expr::column_refs()`),
    # so we have to normalize the plan to ensure deterministic snapshots.
    PlanEditRule(
        query="q12",
        source="""\
    SortPreservingMergeExec: [#46@7 ASC, #44@8 ASC, #35@9 ASC, #38@10 ASC, sum(web_sales.#23)@12 * Some(100),10,0 / CASE WHEN sum(sum(web_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@11 = Some(0),27,2 THEN CAST(raise_error(Division by zero) AS Decimal128(27, 2)) ELSE sum(sum(web_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@11 END ASC], fetch=100
      SortExec: TopK(fetch=100), expr=[#87@2 ASC, #88@3 ASC, #85@0 ASC, #86@1 ASC, #90@5 * Some(100),10,0 / CASE WHEN sum(sum(web_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@11 = Some(0),27,2 THEN CAST(raise_error(Division by zero) AS Decimal128(27, 2)) ELSE sum(sum(web_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@11 END ASC], preserve_partitioning=[true]
        ProjectionExec: expr=[#35@0 as #85, #38@1 as #86, #46@2 as #87, #44@3 as #88, #39@4 as #89, sum(web_sales.#23)@5 as #90, sum(web_sales.#23)@5 * Some(100),10,0 / CASE WHEN sum(sum(web_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@6 = Some(0),27,2 THEN CAST(raise_error(Division by zero) AS Decimal128(27, 2)) ELSE sum(sum(web_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@6 END as #91, #46@2 as #46, #44@3 as #44, #35@0 as #35, #38@1 as #38, sum(sum(web_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@6 as sum(sum(web_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, sum(web_sales.#23)@5 as sum(web_sales.#23)]
""",
        target="""\
    SortPreservingMergeExec: [#46@7 ASC, #44@8 ASC, #35@9 ASC, #38@10 ASC, sum(web_sales.#23)@11 * Some(100),10,0 / CASE WHEN sum(sum(web_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@12 = Some(0),27,2 THEN CAST(raise_error(Division by zero) AS Decimal128(27, 2)) ELSE sum(sum(web_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@12 END ASC], fetch=100
      SortExec: TopK(fetch=100), expr=[#87@2 ASC, #88@3 ASC, #85@0 ASC, #86@1 ASC, #90@5 * Some(100),10,0 / CASE WHEN sum(sum(web_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@12 = Some(0),27,2 THEN CAST(raise_error(Division by zero) AS Decimal128(27, 2)) ELSE sum(sum(web_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@12 END ASC], preserve_partitioning=[true]
        ProjectionExec: expr=[#35@0 as #85, #38@1 as #86, #46@2 as #87, #44@3 as #88, #39@4 as #89, sum(web_sales.#23)@5 as #90, sum(web_sales.#23)@5 * Some(100),10,0 / CASE WHEN sum(sum(web_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@6 = Some(0),27,2 THEN CAST(raise_error(Division by zero) AS Decimal128(27, 2)) ELSE sum(sum(web_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@6 END as #91, #46@2 as #46, #44@3 as #44, #35@0 as #35, #38@1 as #38, sum(web_sales.#23)@5 as sum(web_sales.#23), sum(sum(web_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@6 as sum(sum(web_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]
""",
    ),
    PlanEditRule(
        query="q20",
        source="""\
    SortPreservingMergeExec: [#46@7 ASC, #44@8 ASC, #35@9 ASC, #38@10 ASC, sum(catalog_sales.#23)@12 * Some(100),10,0 / CASE WHEN sum(sum(catalog_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@11 = Some(0),27,2 THEN CAST(raise_error(Division by zero) AS Decimal128(27, 2)) ELSE sum(sum(catalog_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@11 END ASC], fetch=100
      SortExec: TopK(fetch=100), expr=[#87@2 ASC, #88@3 ASC, #85@0 ASC, #86@1 ASC, #90@5 * Some(100),10,0 / CASE WHEN sum(sum(catalog_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@11 = Some(0),27,2 THEN CAST(raise_error(Division by zero) AS Decimal128(27, 2)) ELSE sum(sum(catalog_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@11 END ASC], preserve_partitioning=[true]
        ProjectionExec: expr=[#35@0 as #85, #38@1 as #86, #46@2 as #87, #44@3 as #88, #39@4 as #89, sum(catalog_sales.#23)@5 as #90, sum(catalog_sales.#23)@5 * Some(100),10,0 / CASE WHEN sum(sum(catalog_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@6 = Some(0),27,2 THEN CAST(raise_error(Division by zero) AS Decimal128(27, 2)) ELSE sum(sum(catalog_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@6 END as #91, #46@2 as #46, #44@3 as #44, #35@0 as #35, #38@1 as #38, sum(sum(catalog_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@6 as sum(sum(catalog_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, sum(catalog_sales.#23)@5 as sum(catalog_sales.#23)]
""",
        target="""\
    SortPreservingMergeExec: [#46@7 ASC, #44@8 ASC, #35@9 ASC, #38@10 ASC, sum(catalog_sales.#23)@11 * Some(100),10,0 / CASE WHEN sum(sum(catalog_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@12 = Some(0),27,2 THEN CAST(raise_error(Division by zero) AS Decimal128(27, 2)) ELSE sum(sum(catalog_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@12 END ASC], fetch=100
      SortExec: TopK(fetch=100), expr=[#87@2 ASC, #88@3 ASC, #85@0 ASC, #86@1 ASC, #90@5 * Some(100),10,0 / CASE WHEN sum(sum(catalog_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@12 = Some(0),27,2 THEN CAST(raise_error(Division by zero) AS Decimal128(27, 2)) ELSE sum(sum(catalog_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@12 END ASC], preserve_partitioning=[true]
        ProjectionExec: expr=[#35@0 as #85, #38@1 as #86, #46@2 as #87, #44@3 as #88, #39@4 as #89, sum(catalog_sales.#23)@5 as #90, sum(catalog_sales.#23)@5 * Some(100),10,0 / CASE WHEN sum(sum(catalog_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@6 = Some(0),27,2 THEN CAST(raise_error(Division by zero) AS Decimal128(27, 2)) ELSE sum(sum(catalog_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@6 END as #91, #46@2 as #46, #44@3 as #44, #35@0 as #35, #38@1 as #38, sum(catalog_sales.#23)@5 as sum(catalog_sales.#23), sum(sum(catalog_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@6 as sum(sum(catalog_sales.#23)) PARTITION BY [item.#44] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]
""",
    ),
    PlanEditRule(
        query="q98",
        source="""\
    SortPreservingMergeExec: [#35@7 ASC, #33@8 ASC, #24@9 ASC, #27@10 ASC, sum(store_sales.#15)@12 * Some(100),10,0 / CASE WHEN sum(sum(store_sales.#15)) PARTITION BY [item.#33] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@11 = Some(0),27,2 THEN CAST(raise_error(Division by zero) AS Decimal128(27, 2)) ELSE sum(sum(store_sales.#15)) PARTITION BY [item.#33] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@11 END ASC]
      SortExec: expr=[#76@2 ASC, #77@3 ASC, #74@0 ASC, #75@1 ASC, #79@5 * Some(100),10,0 / CASE WHEN sum(sum(store_sales.#15)) PARTITION BY [item.#33] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@11 = Some(0),27,2 THEN CAST(raise_error(Division by zero) AS Decimal128(27, 2)) ELSE sum(sum(store_sales.#15)) PARTITION BY [item.#33] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@11 END ASC], preserve_partitioning=[true]
        ProjectionExec: expr=[#24@0 as #74, #27@1 as #75, #35@2 as #76, #33@3 as #77, #28@4 as #78, sum(store_sales.#15)@5 as #79, sum(store_sales.#15)@5 * Some(100),10,0 / CASE WHEN sum(sum(store_sales.#15)) PARTITION BY [item.#33] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@6 = Some(0),27,2 THEN CAST(raise_error(Division by zero) AS Decimal128(27, 2)) ELSE sum(sum(store_sales.#15)) PARTITION BY [item.#33] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@6 END as #80, #35@2 as #35, #33@3 as #33, #24@0 as #24, #27@1 as #27, sum(sum(store_sales.#15)) PARTITION BY [item.#33] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@6 as sum(sum(store_sales.#15)) PARTITION BY [item.#33] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, sum(store_sales.#15)@5 as sum(store_sales.#15)]
""",
        target="""\
    SortPreservingMergeExec: [#35@7 ASC, #33@8 ASC, #24@9 ASC, #27@10 ASC, sum(store_sales.#15)@11 * Some(100),10,0 / CASE WHEN sum(sum(store_sales.#15)) PARTITION BY [item.#33] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@12 = Some(0),27,2 THEN CAST(raise_error(Division by zero) AS Decimal128(27, 2)) ELSE sum(sum(store_sales.#15)) PARTITION BY [item.#33] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@12 END ASC]
      SortExec: expr=[#76@2 ASC, #77@3 ASC, #74@0 ASC, #75@1 ASC, #79@5 * Some(100),10,0 / CASE WHEN sum(sum(store_sales.#15)) PARTITION BY [item.#33] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@12 = Some(0),27,2 THEN CAST(raise_error(Division by zero) AS Decimal128(27, 2)) ELSE sum(sum(store_sales.#15)) PARTITION BY [item.#33] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@12 END ASC], preserve_partitioning=[true]
        ProjectionExec: expr=[#24@0 as #74, #27@1 as #75, #35@2 as #76, #33@3 as #77, #28@4 as #78, sum(store_sales.#15)@5 as #79, sum(store_sales.#15)@5 * Some(100),10,0 / CASE WHEN sum(sum(store_sales.#15)) PARTITION BY [item.#33] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@6 = Some(0),27,2 THEN CAST(raise_error(Division by zero) AS Decimal128(27, 2)) ELSE sum(sum(store_sales.#15)) PARTITION BY [item.#33] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@6 END as #80, #35@2 as #35, #33@3 as #33, #24@0 as #24, #27@1 as #27, sum(store_sales.#15)@5 as sum(store_sales.#15), sum(sum(store_sales.#15)) PARTITION BY [item.#33] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING@6 as sum(sum(store_sales.#15)) PARTITION BY [item.#33] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]
""",
    ),
]


def _edit_tpcds_plan(query: str, plan: str) -> str:
    for rule in TPC_DS_PLAN_EDIT_RULES:
        if rule.query == query:
            plan = plan.replace(rule.source, rule.target)
            # The source string may or may not be present in the plan before replacement.
            # But if the target string is missing in the plan after replacement,
            # the plan edit rule is known to be outdated and should be updated or removed.
            assert rule.target in plan, f"edit rule not applied: {rule}"
    return plan
