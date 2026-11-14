"""A correlated scalar aggregate with an inequality over a join returns one value per outer row.

Expected values were verified on JVM Spark 4.2.0.
"""

import pytest

W = "(SELECT id AS k, id AS v FROM range(0, 10, 1, 2))"
VAL = (
    "(SELECT CAST(c1 AS BIGINT) AS k, CAST(c2 AS BIGINT) AS v "
    "FROM VALUES (0, 0), (1, 10), (2, 20), (3, 30) AS t(c1, c2))"
)
JOIN = f"(SELECT p.k, q.v FROM {W} p JOIN {W} q ON p.k = q.k)"  # noqa: S608


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            f"SELECT a.k, (SELECT count(*) FROM {JOIN} x WHERE x.v > a.v) FROM {W} a",  # noqa: S608
            [(k, 9 - k) for k in range(10)],
        ),
        (
            f"SELECT a.k, (SELECT count(*) FROM (SELECT p.k, q.v FROM {W} p LEFT JOIN {W} q ON p.k = q.k) x "  # noqa: S608
            f"WHERE x.k >= a.v) FROM {W} a",
            [(k, 10 - k) for k in range(10)],
        ),
        (
            f"SELECT a.k, (SELECT count(*) FROM (SELECT p.k, q.v FROM {VAL} p JOIN {VAL} q ON p.k = q.k) x "  # noqa: S608
            f"WHERE x.v >= a.v) FROM {W} a",
            [(0, 4)] + [(k, 3) for k in range(1, 10)],
        ),
        (
            f"SELECT a.k, (SELECT count(*) FROM {W} p CROSS JOIN {W} q WHERE q.v >= a.v AND p.k < 2) FROM {W} a",  # noqa: S608
            [(k, 20 - 2 * k) for k in range(10)],
        ),
        (
            f"SELECT a.k, (SELECT sum(x.v) FROM {JOIN} x WHERE x.v > a.v) FROM {W} a",  # noqa: S608
            [(0, 45), (1, 44), (2, 42), (3, 39), (4, 35), (5, 30), (6, 24), (7, 17), (8, 9), (9, None)],
        ),
        (
            f"SELECT a.k, (SELECT count(*) FROM {JOIN} x WHERE x.k = a.k AND x.v >= a.v) FROM {W} a",  # noqa: S608
            [(k, 1) for k in range(10)],
        ),
        (
            f"SELECT a.k FROM {W} a WHERE a.v < (SELECT max(q.v) FROM {W} p JOIN {W} q ON p.k = q.k WHERE q.v >= a.v)",  # noqa: S608
            [(k,) for k in range(9)],
        ),
    ],
    ids=["inner-join", "left-join", "values", "cross-join", "sum-null", "equality-and-inequality", "filter"],
)
def test_correlated_scalar_aggregate_with_an_inequality_over_a_join(spark, query, expected):
    assert sorted(tuple(row) for row in spark.sql(query).collect()) == expected
