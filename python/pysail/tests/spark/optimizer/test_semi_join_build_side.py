from __future__ import annotations

from dataclasses import dataclass

import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail optimizer plans")


@dataclass(frozen=True)
class OptimizerSettings:
    reorder: bool
    swap: bool


@pytest.fixture(
    scope="module",
    params=[
        OptimizerSettings(reorder=False, swap=True),
        OptimizerSettings(reorder=True, swap=True),
        OptimizerSettings(reorder=True, swap=False),
    ],
    ids=["reorder-off-swap-on", "reorder-on-swap-on", "reorder-on-swap-off"],
)
def optimizer_settings(request) -> OptimizerSettings:
    return request.param


@pytest.fixture(scope="module")
def remote(optimizer_settings: OptimizerSettings):
    with spark_connect_server(
        envs={
            "SAIL_OPTIMIZER__ENABLE_JOIN_REORDER": str(optimizer_settings.reorder).lower(),
            "SAIL_OPTIMIZER__ENABLE_JOIN_SWAP": str(optimizer_settings.swap).lower(),
        }
    ) as server:
        yield server.remote


@pytest.fixture(scope="module", autouse=True)
def facts(spark, tmp_path_factory):
    path = str(tmp_path_factory.mktemp("semi_join_build_side") / "facts")
    # Exceed JoinSelection's collection threshold. String keys have no NDV
    # statistics, so GROUP BY inherits the 150,000-row input estimate.
    spark.range(150_000).selectExpr(
        "CASE WHEN id % 3 = 2 THEN NULL ELSE CAST(id % 3 AS STRING) END AS k",
        "id AS ticket",
    ).write.parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("semi_join_facts")
    yield
    spark.catalog.dropTempView("semi_join_facts")


@pytest.mark.parametrize("null_safe", [False, True])
@pytest.mark.parametrize("residual", [False, True])
def test_grouped_filter_builds_on_keys(spark, snapshot, null_safe, residual):
    equality = "<=>" if null_safe else "="
    predicate = "AND f.ticket < g.n" if residual else ""
    query = f"""
        SELECT f.k, COUNT(*) AS n
        FROM semi_join_facts f LEFT SEMI JOIN (
            SELECT k, n FROM (
                SELECT k, COUNT(*) AS n,
                       RANK() OVER (PARTITION BY k ORDER BY COUNT(*) DESC) AS r
                FROM semi_join_facts GROUP BY k
            ) ranked WHERE r <= 5
        ) g ON f.k {equality} g.k {predicate}
        GROUP BY f.k ORDER BY f.k
    """  # noqa: S608
    count = 16_667 if residual else 50_000
    expected = [("0", count), ("1", count)]
    if null_safe:
        expected.insert(0, (None, 16_666 if residual else 50_000))
    assert [tuple(row) for row in spark.sql(query).collect()] == expected
    plan = normalize_plan_text(spark.sql(query)._explain_string())  # noqa: SLF001
    assert plan == snapshot


@pytest.mark.parametrize(
    "filter_query",
    [
        # A grouping column not covered by the join keys leaves duplicate keys.
        "SELECT k, ticket FROM semi_join_facts GROUP BY k, ticket",
        # GROUPING SETS can emit multiple rows for the same join key.
        "SELECT k FROM semi_join_facts GROUP BY GROUPING SETS ((k), ())",
        # Computed projections are not direct grouping-column lineage.
        "SELECT reverse(k) AS k FROM semi_join_facts GROUP BY k",
        # Both sides have known row estimates, and the current build is smaller.
        "SELECT k FROM (SELECT k FROM semi_join_facts UNION ALL SELECT k FROM semi_join_facts) GROUP BY k",
    ],
)
def test_unsupported_or_larger_filter_keeps_build_side(spark, snapshot, filter_query):
    query = f"""
        SELECT COUNT(*) AS n FROM semi_join_facts f
        LEFT SEMI JOIN ({filter_query}) g ON f.k = g.k
    """  # noqa: S608
    # Check the fallback boundaries without executing a deliberately expensive
    # many-to-many semi join in the extra-grouping-column case.
    plan = normalize_plan_text(spark.sql(query)._explain_string())  # noqa: SLF001
    assert plan == snapshot
