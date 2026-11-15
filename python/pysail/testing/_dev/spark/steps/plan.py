from __future__ import annotations

from typing import TYPE_CHECKING

from pytest_bdd import then

from pysail.testing.spark.utils.plan import normalize_plan_text

if TYPE_CHECKING:
    from syrupy.assertion import SnapshotAssertion


def _collect_plan(query: str, spark) -> str:
    df = spark.sql(query)
    rows = df.collect()
    assert len(rows) == 1, f"expected single row, got {len(rows)}"
    assert len(rows[0]) == 1, f"expected single column, got {len(rows[0])}"
    plan = rows[0][0]
    assert isinstance(plan, str), "expected string plan output"
    assert plan, "expected non-empty plan output"
    return plan


@then("query plan matches snapshot")
def query_plan_matches_snapshot(query, spark, snapshot: SnapshotAssertion):
    """Executes the SQL query and only asserts against the stored snapshot."""
    plan = _collect_plan(query, spark)
    assert snapshot == normalize_plan_text(plan)
