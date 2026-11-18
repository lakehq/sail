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


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "SELECT k FROM ((SELECT id AS k FROM range(0, 100000, 1, 1) LIMIT 2 OFFSET 3) "
            "UNION ALL (SELECT id + 1 FROM range(0, 10, 1, 1) LIMIT 4)) ORDER BY k DESC",
            [4, 4, 3, 3, 2, 1],
        ),
        ("SELECT k FROM (SELECT id AS k FROM range(0, 100000, 1, 1) LIMIT 2 OFFSET 3) ORDER BY k DESC", [4, 3]),
        (
            "SELECT k FROM (SELECT id AS k FROM range(0, 100000, 1, 1) OFFSET 99997) ORDER BY k DESC",
            [99999, 99998, 99997],
        ),
    ],
    ids=["union", "limit", "no-limit"],
)
def test_sort_above_offset_of_unsorted_input(spark, query, expected):
    # The input of the offset has one partition and reports no order, and the sort above
    # the offset must not change the rows that the offset skips.
    assert [r.k for r in spark.sql(query).collect()] == expected
