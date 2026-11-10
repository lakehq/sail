"""Tests for partition-dependent functions next to generators, map generators,
and non-deterministic expressions and subqueries in aggregate queries.

The expected values were verified on JVM Spark 4.2.0.
"""

import pytest


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param(
            "SELECT id, explode(array(1, 2)) e, monotonically_increasing_id() m FROM range(0, 3, 1, 1)",
            [(0, 1, 0), (0, 2, 1), (1, 1, 2), (1, 2, 3), (2, 1, 4), (2, 2, 5)],
            id="monotonically-increasing-id",
        ),
        pytest.param(
            "SELECT id, explode(array(1, 2)) e, spark_partition_id() p, monotonically_increasing_id() + 1 m "
            "FROM range(0, 3, 1, 1)",
            [(0, 1, 0, 1), (0, 2, 0, 2), (1, 1, 0, 3), (1, 2, 0, 4), (2, 1, 0, 5), (2, 2, 0, 6)],
            id="spark-partition-id",
        ),
        pytest.param(
            "SELECT id, explode(array(monotonically_increasing_id(), 10)) e FROM range(0, 3, 1, 1)",
            [(0, 0), (0, 10), (1, 1), (1, 10), (2, 2), (2, 10)],
            id="generator-argument",
        ),
    ],
)
def test_partition_functions_next_to_generator(spark, query, expected):
    assert [tuple(row) for row in spark.sql(query).collect()] == expected


def test_seeded_random_in_aggregate_result(spark):
    rows = spark.sql(
        "SELECT k, CASE WHEN k = 0 THEN rand(42) ELSE sum(id) END v "
        "FROM (SELECT id, id % 2 k FROM range(0, 10, 1, 1)) GROUP BY k ORDER BY k"
    ).collect()
    assert [row.k for row in rows] == [0, 1]
    assert 0.0 <= rows[0].v < 1.0
    assert rows[1].v == 25.0  # noqa: PLR2004
    assert spark.sql("SELECT sum(id) + rand(7) v FROM range(0, 30, 1, 1)").collect()[0].v == 435.28188121484885  # noqa: PLR2004
    assert [
        tuple(row)
        for row in spark.sql(
            "SELECT id % 2 k, max(id) m, rand() IS NOT NULL r FROM range(0, 10) GROUP BY id % 2 ORDER BY k"
        ).collect()
    ] == [(0, 8, True), (1, 9, True)]


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param(
            "SELECT sum(CASE WHEN EXISTS (SELECT 1) THEN 1 ELSE 0 END) s FROM range(3)",
            [(3,)],
            id="exists",
        ),
        pytest.param(
            "SELECT id % 2 k, sum(CASE WHEN EXISTS (SELECT 1 FROM range(5) t WHERE t.id = o.id) THEN 1 ELSE 0 END) s "
            "FROM range(0, 8) o GROUP BY id % 2 ORDER BY k",
            [(0, 3), (1, 2)],
            id="correlated-exists",
        ),
        pytest.param(
            "SELECT count(*) FILTER (WHERE id IN (SELECT id * 2 FROM range(5))) c, "
            "sum(CASE WHEN id IN (SELECT id * 3 FROM range(5)) THEN id END) s FROM range(0, 10)",
            [(5, 18)],
            id="in",
        ),
        pytest.param(
            "SELECT id % 2 k, sum(id) FROM range(0, 8) o GROUP BY id % 2 "
            "HAVING sum(CASE WHEN EXISTS (SELECT 1 FROM range(3) t WHERE t.id = o.id) THEN 1 ELSE 0 END) > 1 "
            "ORDER BY k",
            [(0, 12)],
            id="having",
        ),
    ],
)
def test_predicate_subquery_in_aggregate_function(spark, query, expected):
    assert [tuple(row) for row in spark.sql(query).collect()] == expected


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param(
            "SELECT explode(map(1, 'x', 2, 'y')) FROM range(2)",
            [(1, "x"), (2, "y"), (1, "x"), (2, "y")],
            id="explode",
        ),
        pytest.param(
            "SELECT posexplode(map(1, 'x', 2, 'y')) FROM range(2)",
            [(0, 1, "x"), (1, 2, "y"), (0, 1, "x"), (1, 2, "y")],
            id="posexplode",
        ),
        pytest.param(
            "SELECT explode_outer(map(1, 'x', 2, 'y')) FROM range(2)",
            [(1, "x"), (2, "y"), (1, "x"), (2, "y")],
            id="explode-outer",
        ),
        pytest.param(
            "SELECT posexplode_outer(map(1, 'x', 2, 'y')) FROM range(2)",
            [(0, 1, "x"), (1, 2, "y"), (0, 1, "x"), (1, 2, "y")],
            id="posexplode-outer",
        ),
        pytest.param(
            "SELECT id, explode(m) FROM (SELECT id, map(id, CAST(id AS STRING)) m FROM range(3))",
            [(0, 0, "0"), (1, 1, "1"), (2, 2, "2")],
            id="column-values",
        ),
        pytest.param(
            "SELECT id, explode(m) FROM (SELECT id, map(id, IF(id = 0, NULL, 'x')) m FROM range(3))",
            [(0, 0, None), (1, 1, "x"), (2, 2, "x")],
            id="nullable-values",
        ),
        pytest.param(
            "SELECT id, explode_outer(m) FROM (SELECT id, IF(id = 1, NULL, map(id, 'x')) m FROM range(3))",
            [(0, 0, "x"), (1, None, None), (2, 2, "x")],
            id="explode-outer-null-map",
        ),
        pytest.param(
            "SELECT id, posexplode_outer(m) FROM (SELECT id, IF(id = 1, NULL, map(id, 'x')) m FROM range(3))",
            [(0, 0, 0, "x"), (1, None, None, None), (2, 0, 2, "x")],
            id="posexplode-outer-null-map",
        ),
        pytest.param(
            "SELECT id, explode_outer(m) FROM "
            "(SELECT id, IF(id = 1, map_filter(map(id, 'x'), (k, v) -> false), map(id, 'x')) m FROM range(3))",
            [(0, 0, "x"), (1, None, None), (2, 2, "x")],
            id="explode-outer-empty-map",
        ),
        pytest.param(
            "SELECT explode(map_from_arrays(array(1, 2), array('x', 'y'))) FROM range(1)",
            [(1, "x"), (2, "y")],
            id="map-from-arrays",
        ),
        pytest.param(
            "SELECT explode(map(1, map(2, 'z'))) FROM range(1)",
            [(1, {2: "z"})],
            id="nested-map-value",
        ),
    ],
)
def test_explode_map(spark, query, expected):
    assert [tuple(row) for row in spark.sql(query).collect()] == expected
