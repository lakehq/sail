from __future__ import annotations

import re

import pytest
from pytest_bdd import scenarios

from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail only")

COLLECTED_KEY_LIMIT = 60_000


scenarios("features")


@pytest.mark.parametrize(
    ("limit", "expected_n", "expected_total"),
    [(COLLECTED_KEY_LIMIT, 150, 8_700), (65_536, 164, 9_512), (70_000, 175, 10_150)],
)
def test_early_filters_respect_key_byte_threshold(spark, tmp_path, limit, expected_n, expected_total):
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
    if limit == COLLECTED_KEY_LIMIT:
        assert len(reductions) == 1, plan
        assert "mode=CollectLeft" in reductions[0], plan
    else:
        assert not reductions, plan


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
