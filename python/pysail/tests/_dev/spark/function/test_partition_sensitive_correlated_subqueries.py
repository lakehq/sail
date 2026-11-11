"""Tests for partition-sensitive functions in correlated subqueries.

Spark removes the correlated equalities from a subquery when it decorrelates it, so the
functions are evaluated for the rows before them. The expected values were verified on JVM
Spark 4.2.0. They depend on the partition of each row, so every input has an explicit number
of partitions.
"""

import pytest


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param(
            "SELECT (SELECT max(x.v) FROM"
            " (SELECT id, id + rand(1) AS v FROM range(0, 4, 1, 1)) x WHERE x.id = t.id) AS r"
            " FROM range(0, 4, 1, 1) t ORDER BY id",
            [0.6363787615254752, 1.5993846534021867, 2.134842710012538, 3.076841639054609],
            id="rand-in-derived-table",
        ),
        pytest.param(
            "SELECT (SELECT round(sum(r), 6) FROM"
            " (SELECT rand(1) r FROM (SELECT id % 3 AS k FROM range(0, 20000, 1, 2)) t2"
            " WHERE t2.k = t.k)) m FROM (SELECT id AS k FROM range(0, 3, 1, 1)) t ORDER BY k",
            [3315.805032, 3309.99223, 3313.107413],
            id="rand-above-correlated-filter",
        ),
        pytest.param(
            "SELECT (SELECT count(*) FROM"
            " (SELECT id % 3 AS k FROM range(0, 20000, 1, 2)) t2"
            " WHERE t2.k = t.k AND rand(1) > 0.3) c"
            " FROM (SELECT id AS k FROM range(0, 3, 1, 1)) t ORDER BY k",
            [4659, 4611, 4671],
            id="rand-in-correlated-filter",
        ),
        pytest.param(
            "SELECT (SELECT count(*) FROM"
            " (SELECT id % 3 AS k FROM range(0, 20000, 1, 2)) t2"
            " WHERE rand(1) > 0.3 AND t2.k = t.k) c"
            " FROM (SELECT id AS k FROM range(0, 3, 1, 1)) t ORDER BY k",
            [4659, 4611, 4671],
            id="rand-before-correlated-equality",
        ),
        pytest.param(
            "SELECT o.id FROM range(0, 12, 1, 1) o WHERE o.id IN"
            " (SELECT monotonically_increasing_id() FROM range(0, 12, 1, 3) t WHERE t.id = o.id)"
            " ORDER BY o.id",
            [0, 1, 2, 3],
            id="monotonic-id-in-value",
        ),
        pytest.param(
            "SELECT o.id FROM range(0, 12, 1, 1) o WHERE EXISTS (SELECT 1 FROM"
            " (SELECT t2.id + monotonically_increasing_id() AS r FROM range(0, 12, 1, 2) t2"
            " WHERE t2.id % 3 = o.id % 3) WHERE r % 2 = 0) ORDER BY o.id",
            list(range(12)),
            id="monotonic-id-exists-non-equality",
        ),
        pytest.param(
            "SELECT EXISTS (SELECT 1 FROM range(0, 12, 1, 1) x WHERE x.id = t.id AND rand(1) < 0.5)"
            " FROM range(0, 12, 1, 1) t ORDER BY id",
            [False, False, True, True, False, False, True, True, True, False, False, True],
            id="rand-in-projected-exists",
        ),
        pytest.param(
            "SELECT (SELECT count(*) FROM (SELECT t.id, rand(42) AS r FROM range(0, 10, 1, 2) t) x"
            " WHERE x.id <= o.id % 10 AND x.r < 0.5) FROM range(0, 10, 1, 1) o ORDER BY id",
            [0, 0, 0, 1, 1, 1, 1, 2, 3, 3],
            id="rand-below-non-equality",
        ),
        pytest.param(
            "SELECT (SELECT max(r) FROM (SELECT rand(1) r FROM (SELECT x.id,"
            " row_number() OVER (ORDER BY x.id) rn FROM range(0, 4, 1, 1) x) t2"
            " WHERE t2.rn = t.id + 1)) FROM range(0, 4, 1, 1) t ORDER BY id",
            [0.6363787615254752, 0.5993846534021868, 0.134842710012538, 0.07684163905460906],
            id="rand-above-window",
        ),
        pytest.param(
            "SELECT (SELECT max(r) FROM (SELECT rand(1) r FROM range(0, 4, 1, 1) a"
            " JOIN range(0, 4, 1, 1) b ON a.id = b.id WHERE a.id = t.id))"
            " FROM range(0, 4, 1, 1) t ORDER BY id",
            [0.6363787615254752, 0.5993846534021868, 0.134842710012538, 0.07684163905460906],
            id="rand-above-join",
        ),
        pytest.param(
            "SELECT (SELECT floor(max(w) + rand(1)) FROM VALUES (0, 10), (1, 11), (1, 12),"
            " (NULL, 13), (5, 15) AS s(k2, w) WHERE s.k2 = t.k) FROM (SELECT id, id % 3 AS k"
            " FROM range(0, 6, 1, 1)) t ORDER BY id",
            [10, 12, None] * 2,
            id="rand-above-aggregate",
        ),
        pytest.param(
            "SELECT (SELECT spark_partition_id()"
            " FROM range(0, 24, 1, 3) x WHERE x.id = t.id) AS v FROM range(0, 24, 1, 2) t"
            " ORDER BY id",
            [0] * 8 + [1] * 8 + [2] * 8,
            id="partition-id",
        ),
        pytest.param(
            "SELECT (SELECT max(r + p) FROM"
            " (SELECT monotonically_increasing_id() r, spark_partition_id() p"
            " FROM range(0, 12, 1, 3) t2 WHERE t2.id = t.id)) m FROM range(0, 12, 1, 1) t"
            " ORDER BY id",
            [
                0,
                1,
                2,
                3,
                8589934593,
                8589934594,
                8589934595,
                8589934596,
                17179869186,
                17179869187,
                17179869188,
                17179869189,
            ],
            id="monotonic-id-and-partition-id",
        ),
        pytest.param(
            "SELECT id FROM range(0, 12, 1, 1) t WHERE EXISTS (SELECT 1 FROM range(0, 12, 1, 3) x"
            " WHERE x.id = t.id AND spark_partition_id() = 1) ORDER BY id",
            [4, 5, 6, 7],
            id="partition-id-in-exists",
        ),
        pytest.param(
            "SELECT id FROM range(0, 12, 1, 1) t WHERE NOT EXISTS (SELECT 1 FROM range(0, 12, 1, 3) x"
            " WHERE x.id = t.id AND monotonically_increasing_id() % 2 = 0) ORDER BY id",
            [1, 3, 5, 7, 9, 11],
            id="monotonic-id-in-not-exists",
        ),
        pytest.param(
            "SELECT EXISTS (SELECT 1 FROM range(0, 12, 1, 3) x"
            " WHERE x.id = t.id AND monotonically_increasing_id() % 3 = 0) FROM range(0, 12, 1, 1) t ORDER BY id",
            [True, False, False, True, False, True, False, False, False, False, True, False],
            id="monotonic-id-in-projected-exists",
        ),
        pytest.param(
            "SELECT id FROM range(0, 12, 1, 1) t WHERE EXISTS (SELECT 1 FROM range(0, 12, 1, 3) x"
            " WHERE x.id = t.id AND EXISTS (SELECT 1 FROM range(0, 12, 1, 2) y"
            " WHERE y.id = x.id AND spark_partition_id() = 1)) ORDER BY id",
            [6, 7, 8, 9, 10, 11],
            id="partition-id-in-nested-exists",
        ),
        pytest.param(
            "SELECT id FROM range(0, 12, 1, 1) t WHERE EXISTS (SELECT 1 FROM range(0, 12, 1, 3) x"
            " WHERE x.id = t.id AND EXISTS (SELECT 1 FROM range(0, 12, 1, 2) y"
            " WHERE y.id = x.id AND monotonically_increasing_id() % 2 = 0)) ORDER BY id",
            [0, 2, 4, 6, 8, 10],
            id="monotonic-id-in-nested-exists",
        ),
        pytest.param(
            "SELECT id FROM range(0, 12, 1, 1) t WHERE EXISTS (SELECT 1 FROM range(0, 12, 1, 3) x"
            " WHERE x.id = t.id AND (SELECT max(y.p) FROM"
            " (SELECT id, spark_partition_id() AS p FROM range(0, 12, 1, 2)) y WHERE y.id = x.id) = 1)"
            " ORDER BY id",
            [6, 7, 8, 9, 10, 11],
            id="partition-id-in-scalar-subquery-in-exists",
        ),
    ],
)
def test_partition_sensitive_functions_in_correlated_subqueries(spark, query, expected):
    assert [row[0] for row in spark.sql(query).collect()] == expected


def test_monotonic_id_in_correlated_subquery_over_view(spark):
    spark.sql("CREATE OR REPLACE TEMP VIEW t AS SELECT id, id % 3 AS k FROM range(0, 12, 1, 1)")
    try:
        rows = spark.sql(
            "SELECT (SELECT max(r) FROM"
            " (SELECT monotonically_increasing_id() r FROM t t2 WHERE t2.k = t.k)) m"
            " FROM t ORDER BY id"
        ).collect()
    finally:
        spark.sql("DROP VIEW t")
    assert [row[0] for row in rows] == [9, 10, 11] * 4


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param(
            "SELECT count(*), sum(id) FROM range(0, 40000, 1, 2) t"
            " WHERE EXISTS (SELECT 1 FROM range(0, 40000, 1, 3) x WHERE x.id = t.id AND rand(1) < 0.5)",
            (20095, 403111379),
            id="exists",
        ),
        pytest.param(
            "SELECT count(*), sum(id) FROM range(0, 40000, 1, 2) t"
            " WHERE id IN (SELECT x.id FROM range(0, 40000, 1, 3) x WHERE x.id = t.id AND randn(3) > 0)",
            (19895, 398286218),
            id="in",
        ),
    ],
)
def test_seeded_random_in_correlated_predicate_across_batches(spark, query, expected):
    # Each partition of the subquery input has more than one batch.
    assert tuple(spark.sql(query).collect()[0]) == expected
