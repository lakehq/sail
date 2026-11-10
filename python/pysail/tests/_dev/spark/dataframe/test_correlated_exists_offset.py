"""A correlated EXISTS whose body has an OFFSET tests whether the body returns more rows than
the offset for each outer row, whatever the correlation.

Expected values were verified on JVM Spark 4.2.0.
"""

import pytest

OUTER = "FROM range(0, 12) AS o"
INNER = "FROM range(0, 10) AS t"


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param(
            f"SELECT id {OUTER} WHERE EXISTS (SELECT 1 {INNER} WHERE t.id >= o.id LIMIT 1 OFFSET 3)",
            list(range(7)),
            id="exists-non-equality",
        ),
        pytest.param(
            f"SELECT id {OUTER} WHERE NOT EXISTS (SELECT 1 {INNER} WHERE t.id >= o.id LIMIT 1 OFFSET 3)",
            list(range(7, 12)),
            id="not-exists-non-equality",
        ),
        pytest.param(
            f"SELECT id {OUTER} WHERE EXISTS (SELECT 1 {INNER} WHERE t.id >= o.id OFFSET 3)",
            list(range(7)),
            id="exists-offset-without-limit",
        ),
        pytest.param(
            f"SELECT id {OUTER} WHERE EXISTS "
            f"(SELECT 1 {INNER} WHERE t.id >= o.id ORDER BY t.id DESC LIMIT 2 OFFSET 3)",
            list(range(7)),
            id="exists-sorted",
        ),
        pytest.param(
            f"SELECT id {OUTER} WHERE EXISTS (SELECT 1 {INNER} WHERE t.id % 3 = o.id % 3 LIMIT 1 OFFSET 3)",
            [0, 3, 6, 9],
            id="exists-computed-equality",
        ),
        pytest.param(
            f"SELECT id {OUTER} WHERE NOT EXISTS "
            f"(SELECT 1 {INNER} WHERE t.id % 3 = o.id % 3 AND t.id > o.id LIMIT 1 OFFSET 1)",
            list(range(4, 12)),
            id="not-exists-mixed-correlation",
        ),
        pytest.param(
            f"SELECT id {OUTER} WHERE id < 3 OR NOT EXISTS "
            f"(SELECT t.id {INNER} WHERE t.id >= o.id LIMIT 5 OFFSET 2)",
            [0, 1, 2, 8, 9, 10, 11],
            id="not-exists-in-disjunction",
        ),
        pytest.param(
            f"SELECT id {OUTER} WHERE EXISTS "  # noqa: S608
            f"(SELECT k FROM (SELECT t.id AS k {INNER} WHERE t.id >= o.id) AS s LIMIT 1 OFFSET 4)",
            list(range(6)),
            id="exists-derived-table",
        ),
        pytest.param(
            f"SELECT id {OUTER} WHERE EXISTS (SELECT max(t.id) {INNER} WHERE t.id >= o.id LIMIT 1 OFFSET 1)",
            [],
            id="exists-scalar-aggregate",
        ),
        pytest.param(
            f"SELECT id {OUTER} WHERE EXISTS (SELECT 1 {INNER} WHERE t.id >= o.id LIMIT 0 OFFSET 3)",
            [],
            id="exists-limit-0",
        ),
        pytest.param(
            "SELECT id FROM range(0, 6) AS o WHERE EXISTS "  # noqa: S608
            f"(SELECT 1 {INNER} WHERE t.id >= o.id LIMIT 1 OFFSET 3) AND NOT EXISTS "
            f"(SELECT 1 {INNER} WHERE t.id >= o.id LIMIT 1 OFFSET 7)",
            [3, 4, 5],
            id="exists-and-not-exists",
        ),
        pytest.param(
            f"SELECT id {OUTER} WHERE EXISTS (SELECT 1 {INNER} WHERE t.id >= o.id AND EXISTS "  # noqa: S608
            "(SELECT 1 FROM range(0, 10) AS u WHERE u.id > t.id LIMIT 1 OFFSET 2) LIMIT 1 OFFSET 1)",
            list(range(6)),
            id="nested",
        ),
    ],
)
def test_correlated_exists_with_offset(spark, query, expected):
    actual = sorted(row[0] for row in spark.sql(query).collect())
    assert actual == expected


def test_correlated_exists_with_offset_over_join(spark):
    query = """
        SELECT o.id, count(*) AS c FROM range(0, 8) AS o JOIN range(0, 3) AS p ON o.id % 3 = p.id
        WHERE EXISTS (SELECT 1 FROM range(0, 10) AS t WHERE t.id >= o.id + p.id LIMIT 1 OFFSET 2)
        GROUP BY o.id ORDER BY o.id
    """
    assert [tuple(row) for row in spark.sql(query).collect()] == [(i, 1) for i in range(7)]
