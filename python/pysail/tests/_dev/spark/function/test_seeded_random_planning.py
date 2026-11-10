"""Tests for random functions with a seed next to expressions that are planned separately.

The expected values were verified on JVM Spark 4.2.0.
"""

import pytest


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        # A seeded call in a conditional branch advances its generator only for the rows that
        # evaluate the branch, also when another branch has a scalar subquery.
        pytest.param(
            "SELECT id, CASE WHEN id % 2 = 0 THEN rand(42) ELSE (SELECT 0.0D) END AS r "
            "FROM range(0, 6, 1, 1) ORDER BY id",
            [(0, 0.619189370225301), (1, 0.0), (2, 0.5096018842446481), (3, 0.0), (4, 0.8325259388871524), (5, 0.0)],
            id="conditional-with-scalar-subquery",
        ),
        pytest.param(
            "SELECT id, CASE WHEN id % 2 = 0 THEN rand(42) ELSE (SELECT 0.0D) END AS r "
            "FROM range(0, 8, 1, 2) ORDER BY id",
            [
                (0, 0.619189370225301),
                (1, 0.0),
                (2, 0.5096018842446481),
                (3, 0.0),
                (4, 0.8017532427858894),
                (5, 0.0),
                (6, 0.6565552949992319),
                (7, 0.0),
            ],
            id="conditional-with-scalar-subquery-partitions",
        ),
        pytest.param(
            "SELECT id, CASE WHEN EXISTS (SELECT 1 FROM range(10) x WHERE x.id = t.id AND x.id % 3 <> 1) "
            "THEN rand(42) END AS r FROM range(0, 6, 1, 1) t ORDER BY id",
            [
                (0, 0.619189370225301),
                (1, None),
                (2, 0.5096018842446481),
                (3, 0.8325259388871524),
                (4, None),
                (5, 0.26322809041172357),
            ],
            id="conditional-with-correlated-exists",
        ),
        # Equal seeded expressions next to a scalar subquery still have equal values.
        pytest.param(
            "SELECT id, rand(42) + (SELECT 0.0D) AS a, rand(42) + (SELECT 0.0D) AS b FROM range(0, 6, 1, 2) ORDER BY id",
            [
                (0, 0.619189370225301, 0.619189370225301),
                (1, 0.5096018842446481, 0.5096018842446481),
                (2, 0.8325259388871524, 0.8325259388871524),
                (3, 0.8017532427858894, 0.8017532427858894),
                (4, 0.6565552949992319, 0.6565552949992319),
                (5, 0.2515595782593636, 0.2515595782593636),
            ],
            id="equal-expressions-with-scalar-subquery",
        ),
        # The other expressions of the projection are evaluated for each row of the generator.
        pytest.param(
            "SELECT id, explode(array(1, 2)) AS g, rand(42) AS r FROM range(0, 3, 1, 1) ORDER BY id, g",
            [
                (0, 1, 0.619189370225301),
                (0, 2, 0.5096018842446481),
                (1, 1, 0.8325259388871524),
                (1, 2, 0.26322809041172357),
                (2, 1, 0.6702867696264135),
                (2, 2, 0.5173283545794627),
            ],
            id="generator",
        ),
        pytest.param(
            "SELECT id, explode(array(1, 2)) AS g, rand(42) AS r FROM range(0, 4, 1, 2) ORDER BY id, g",
            [
                (0, 1, 0.619189370225301),
                (0, 2, 0.5096018842446481),
                (1, 1, 0.8325259388871524),
                (1, 2, 0.26322809041172357),
                (2, 1, 0.8017532427858894),
                (2, 2, 0.6565552949992319),
                (3, 1, 0.2515595782593636),
                (3, 2, 0.2073428376111074),
            ],
            id="generator-partitions",
        ),
        # Each partition of the generator input keeps one generator across all of its batches.
        pytest.param(
            "SELECT count(*) AS n, round(sum(r), 4) AS s "
            "FROM (SELECT id, explode(array(1, 2)) AS g, rand(42) AS r FROM range(0, 10000, 1, 1))",
            [(20000, 9941.969)],
            id="generator-batches",
        ),
        pytest.param(
            "SELECT count(*) AS n, round(sum(r), 4) AS s "
            "FROM (SELECT id, explode(array(1, 2)) AS g, rand(42) AS r FROM range(0, 10000, 1, 2))",
            [(20000, 9917.2884)],
            id="generator-batches-partitions",
        ),
        # Seeded calls on the columns of a correlated subquery.
        pytest.param(
            "SELECT id FROM range(0, 12, 1, 1) t "
            "WHERE EXISTS (SELECT 1 FROM range(0, 12, 1, 1) x WHERE x.id = t.id AND rand(1) < 0.5) ORDER BY id",
            [(2,), (3,), (6,), (7,), (8,), (11,)],
            id="correlated-exists",
        ),
        pytest.param(
            "SELECT id FROM range(0, 12, 1, 1) t "
            "WHERE EXISTS (SELECT 1 FROM range(0, 12, 1, 1) x WHERE x.id = t.id AND x.id + rand(1) > 0.5) ORDER BY id",
            [(i,) for i in range(12)],
            id="correlated-exists-inner-column",
        ),
        pytest.param(
            "WITH t AS (SELECT id, id % 3 AS k, CAST(id AS DOUBLE) AS v FROM range(0, 6, 1, 1)) "
            "SELECT id, (SELECT max(r) FROM (SELECT v + rand(1) AS r FROM t t2 WHERE t2.k = t.k)) AS m "
            "FROM t ORDER BY id",
            [
                (0, 3.076841639054609),
                (1, 4.853921111175545),
                (2, 5.716770421797234),
                (3, 3.076841639054609),
                (4, 4.853921111175545),
                (5, 5.716770421797234),
            ],
            id="correlated-scalar-subquery",
        ),
        pytest.param(
            "SELECT id, rand(1) AS r, (SELECT count(*) FROM range(0, 12, 1, 1) x WHERE x.id % 3 = t.id) AS c "
            "FROM range(0, 6, 1, 2) t ORDER BY id",
            [
                (0, 0.6363787615254752, 4),
                (1, 0.5993846534021868, 4),
                (2, 0.134842710012538, 4),
                (3, 0.5311207224659675, 0),
                (4, 0.2861372051669987, 0),
                (5, 0.49443063728956627, 0),
            ],
            id="correlated-scalar-subquery-computed-key",
        ),
        # An empty scalar subquery is NULL next to a seeded call.
        pytest.param(
            "SELECT id, rand(42) AS r, s FROM (SELECT id, (SELECT id FROM range(1) WHERE id > 5) AS s "
            "FROM range(0, 3, 1, 1)) ORDER BY id",
            [(0, 0.619189370225301, None), (1, 0.5096018842446481, None), (2, 0.8325259388871524, None)],
            id="empty-scalar-subquery",
        ),
        pytest.param(
            "SELECT id, rand(42) + s AS r, rand(42) AS q FROM (SELECT id, (SELECT id FROM range(1) WHERE id > 5) AS s "
            "FROM range(0, 3, 1, 1)) ORDER BY id",
            [(0, None, 0.619189370225301), (1, None, 0.5096018842446481), (2, None, 0.8325259388871524)],
            id="empty-scalar-subquery-arithmetic",
        ),
        # An unused seeded column does not drop the input sort that other operators depend on.
        pytest.param(
            "SELECT collect_list(id) AS l FROM (SELECT id, rand(42) AS r "
            "FROM (SELECT * FROM range(0, 6, 1, 1) ORDER BY id DESC))",
            [([5, 4, 3, 2, 1, 0],)],
            id="unused-seeded-column-order-relevant-aggregate",
        ),
        pytest.param(
            "SELECT id, r FROM (SELECT id, a, rand(42) AS r "
            "FROM (SELECT id, rand(1) AS a FROM (SELECT * FROM range(0, 6, 1, 1) ORDER BY id DESC)))",
            [
                (5, 0.619189370225301),
                (4, 0.5096018842446481),
                (3, 0.8325259388871524),
                (2, 0.26322809041172357),
                (1, 0.6702867696264135),
                (0, 0.5173283545794627),
            ],
            id="unused-seeded-column-below-used-one",
        ),
    ],
)
def test_seeded_random_next_to_separately_planned_expressions(spark, query, expected):
    assert [tuple(row) for row in spark.sql(query).collect()] == expected


@pytest.mark.parametrize(
    "query",
    [
        "SELECT id, rand(42) + rand() AS a, rand(42) + rand() AS b FROM range(0, 6, 1, 1)",
        "SELECT id, rand(42) + rand() + (SELECT 0.0D) AS a, rand(42) + rand() + (SELECT 0.0D) AS b "
        "FROM range(0, 6, 1, 1)",
        "SELECT id, IF(id % 2 = 0, rand(42) + rand(), (SELECT 0.0D)) AS a, "
        "IF(id % 2 = 0, rand(42) + rand(), (SELECT 0.0D)) AS b FROM range(0, 6, 1, 1)",
    ],
)
def test_equal_expressions_with_unseeded_calls_are_independent(spark, query):
    # Each call without a seed has its own generator, so equal expressions have different values.
    rows = spark.sql(query).collect()
    assert all(row.a != row.b for row in rows if row.id % 2 == 0)


def test_equal_unaliased_expressions_with_unseeded_calls_are_independent(spark):
    rows = spark.sql("SELECT id, rand(42) + rand(), rand(42) + rand() FROM range(0, 6, 1, 1)").collect()
    assert all(row[1] != row[2] for row in rows)


@pytest.mark.spark(skip_if_jvm=True, reason="Inspects the Sail plan")
def test_unused_seeded_random_column_does_not_keep_the_input_sort(spark):
    query = "SELECT sum({}) AS s FROM (SELECT id, rand(1) AS r FROM (SELECT * FROM range(0, 100, 1, 4) ORDER BY id DESC)) WHERE id > 5"
    df = spark.sql(query.format("id"))
    assert df.collect()[0].s == 4935  # noqa: PLR2004
    assert "SortExec" not in df._explain_string()  # noqa: SLF001
    # The random values depend on the order of the input when the random column is used.
    assert "SortExec" in spark.sql(query.format("r"))._explain_string()  # noqa: SLF001
