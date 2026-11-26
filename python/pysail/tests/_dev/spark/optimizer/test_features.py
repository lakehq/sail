# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import re

import pytest
from pytest_bdd import scenarios

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.common import shares_deterministic_ctes

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Tests Sail optimizer behavior")

scenarios("features")


@pytest.mark.parametrize(
    ("limit", "expected_n", "expected_total"),
    [(60_000, 150, 8_700), (65_536, 164, 9_512), (70_000, 175, 10_150)],
)
def test_early_filters_collect_estimated_keys(spark, tmp_path, limit, expected_n, expected_total):
    facts = str(tmp_path / "facts")
    dimension = str(tmp_path / "dimension")
    spark.range(400_000).selectExpr("(id % 1000) * 400 AS k1", "id % 7 AS k2").write.parquet(facts)
    spark.range(100_000).selectExpr("id AS k1", "id % 7 AS k2", "id AS v").write.parquet(dimension)
    query = f"""
        SELECT COUNT(*) AS n, SUM(g.n) AS total
        FROM parquet.`{dimension}` d
        JOIN (
            SELECT k1, k2, COUNT(*) AS n
            FROM parquet.`{facts}`
            GROUP BY k1, k2
        ) g ON d.k1 = g.k1 AND d.k2 = g.k2
        WHERE d.v < {limit}
    """  # noqa: S608 - paths and limit are controlled by this test.
    assert tuple(spark.sql(query).first()) == (expected_n, expected_total)

    plan = spark.sql(f"EXPLAIN {query}").first()[0]
    reductions = [line for line in plan.splitlines() if "HashJoinExec" in line and "__early_join_key_0" in line]
    assert len(reductions) == 1, plan
    assert "mode=CollectLeft" in reductions[0], plan


def test_early_filters_deduplicate_dimension_keys(spark, tmp_path):
    duplicate_count = 1_000
    facts = str(tmp_path / "facts")
    dimension = str(tmp_path / "dimension")
    spark.range(10_000).selectExpr("id % 10 AS k", "CAST(1 AS BIGINT) AS amount").write.parquet(facts)
    spark.range(duplicate_count + 1).selectExpr(
        "CAST(1 AS BIGINT) AS k",
        f"CASE WHEN id < {duplicate_count} THEN 'red' ELSE 'blue' END AS color",
    ).write.parquet(dimension)
    query = f"""
        SELECT d.k, g.total
        FROM parquet.`{dimension}` d
        JOIN (
            SELECT k, SUM(amount) AS total
            FROM parquet.`{facts}`
            GROUP BY k
        ) g ON d.k = g.k
        WHERE d.color = 'red'
    """  # noqa: S608 - paths are controlled by this test.
    rows = spark.sql(query).collect()
    assert len(rows) == duplicate_count
    assert all(tuple(row) == (1, 1_000) for row in rows)

    plan = spark.sql(f"EXPLAIN ANALYZE {query}").first()[0]
    reductions = [line for line in plan.splitlines() if "join_type=RightSemi" in line and "__early_join_key_0" in line]
    assert len(reductions) == 1, plan
    # Deduplicate copied keys while retaining the original dimension multiplicity.
    assert re.search(r"\bbuild_input_rows=1(?:,|\])", reductions[0]), reductions[0]


def test_early_filters_restrict_a_shared_cte_to_the_keys_of_its_references(spark, tmp_path):
    facts = str(tmp_path / "facts")
    keys = str(tmp_path / "keys")
    spark.range(100_000).selectExpr("id % 1000 AS k", "id AS v").write.parquet(facts)
    spark.range(1000).selectExpr("id AS k", "id AS v").write.parquet(keys)
    query = f"""
        WITH c AS (
            SELECT k, v % 7 AS g, SUM(v) AS s
            FROM parquet.`{facts}`
            GROUP BY k, v % 7
        )
        SELECT x.k, x.g, x.s AS s1, y.s AS s2
        FROM (SELECT c.k, c.g, c.s FROM c JOIN parquet.`{keys}` d ON c.k = d.k WHERE d.v < 10) x
        JOIN (SELECT c.k, c.g, c.s FROM c JOIN parquet.`{keys}` d ON c.k = d.k WHERE d.v BETWEEN 5 AND 14) y
          ON x.k = y.k - 5 AND x.g = y.g
        ORDER BY 1, 2
    """  # noqa: S608 - paths are controlled by this test.
    totals = {}
    for v in range(100_000):
        totals[(v % 1000, v % 7)] = totals.get((v % 1000, v % 7), 0) + v
    expected = [(k, g, totals[(k, g)], totals[(k + 5, g)]) for k in range(10) for g in range(7)]
    assert [tuple(row) for row in spark.sql(query).collect()] == expected

    if not shares_deterministic_ctes():
        return
    plan = spark.sql(f"EXPLAIN {query}").first()[0]
    lines = plan.splitlines()
    references = [i for i, line in enumerate(lines) if "SharedCteScanExec" in line]
    reductions = [i for i, line in enumerate(lines) if "join_type=RightSemi" in line and "__early_join_key_0" in line]
    # The CTE is evaluated once, and its plan, which follows the plan of the query, reads
    # only the rows of the keys of both references.
    assert len(references) == 2, plan
    assert len(reductions) == 1, plan
    assert reductions[0] > references[-1], plan
    assert sum("UnionExec" in line for line in lines[reductions[0] :]) == 1, plan


@pytest.mark.parametrize("mode", ["local", "local-cluster"])
def test_early_filters_keep_keys_of_scalar_subqueries_out_of_a_shared_cte(tmp_path, mode):
    """The plan of a shared CTE cannot read the result of a scalar subquery of the query,
    so the keys of a reference that depend on such a result are not copied into it."""
    envs = {"SAIL_MODE": mode, "SAIL_OPTIMIZER__ENABLE_JOIN_REORDER": "true"}
    with spark_connect_server(envs=envs) as server, spark_session_factory(server.remote) as sessions:
        spark = sessions.create()
        facts = str(tmp_path / "facts")
        keys = str(tmp_path / "keys")
        spark.range(100_000).selectExpr("id % 1000 AS k", "id AS v").write.parquet(facts)
        spark.range(1000).selectExpr("id AS k", "id AS v").write.parquet(keys)
        totals = {}
        for v in range(100_000):
            totals[(v % 1000, v % 7)] = totals.get((v % 1000, v % 7), 0) + v
        expected = [(k, g, totals[(k, g)], totals[(k + 5, g)]) for k in range(10) for g in range(7)]
        for bound in [
            f"SELECT max(v) - 984 FROM parquet.`{keys}`",  # noqa: S608
            # The scalar subquery reads the CTE itself.
            f"SELECT count(DISTINCT c.k) + 5 FROM c JOIN parquet.`{keys}` d ON c.k = d.k WHERE d.v < 10",  # noqa: S608
        ]:
            query = f"""
                WITH c AS (
                    SELECT k, v % 7 AS g, SUM(v) AS s
                    FROM parquet.`{facts}`
                    GROUP BY k, v % 7
                )
                SELECT x.k, x.g, x.s AS s1, y.s AS s2
                FROM (SELECT c.k, c.g, c.s FROM c JOIN parquet.`{keys}` d ON c.k = d.k WHERE d.v < 10) x
                JOIN (
                    SELECT c.k, c.g, c.s FROM c JOIN parquet.`{keys}` d ON c.k = d.k
                    WHERE d.v >= 5 AND d.v < ({bound})
                ) y ON x.k = y.k - 5 AND x.g = y.g
                ORDER BY 1, 2
            """  # noqa: S608 - paths are controlled by this test.
            assert [tuple(row) for row in spark.sql(query).collect()] == expected, bound

            if not shares_deterministic_ctes(envs):
                continue
            plan = spark.sql(f"EXPLAIN {query}").first()[0]
            # The CTE is evaluated once, and it is not reduced to keys that it cannot compute.
            assert "SharedCteScanExec" in plan, plan
            assert "__early_join_key" not in plan, plan
