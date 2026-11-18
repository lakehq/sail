"""`OFFSET`, which skips the first rows of its input in the order of the input."""

import pytest


@pytest.mark.parametrize(
    "projection",
    ["explode(array(id))", "CAST(id AS STRING)"],
    ids=["generator", "computed"],
)
def test_offset_above_projection_of_sorted_subquery(spark, projection):
    # The sorted subquery returns several batches in one partition, and the projection
    # does not report the order of its output.
    df = spark.sql(
        f"SELECT {projection} AS x "  # noqa: S608
        "FROM (SELECT id FROM range(0, 100000, 1, 8) ORDER BY id DESC LIMIT 20000) OFFSET 10"
    )
    assert sorted(int(r.x) for r in df.collect()) == list(range(80000, 99990))
