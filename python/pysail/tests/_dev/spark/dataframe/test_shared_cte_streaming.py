"""CTEs that are referenced more than once, at sizes where the references cannot all be
served from a small buffer, and with references that stop early or never read.

Spark evaluates a non-deterministic CTE once for all of its references, and inlines a
deterministic CTE into each reference (InlineCTE). Sail may evaluate any CTE once for
several references, but the results must be those of an engine that evaluates a
deterministic CTE per reference: every reference reads all rows of one evaluation, a
volatile CTE is evaluated once, and a query fails only if it reads a row that fails. In
local mode, a reference that is never read does not evaluate the CTE. In cluster mode,
the stage of a shared CTE starts when it is scheduled, as every other stage does.

Spark materializes a non-deterministic CTE in full in a shuffle before any reference
reads it, so it fails on a row that no reference reads (for example under a LIMIT, or
when every reference is the probe side of an empty join). Sail fails only when a
reference reads the row, as it does when the CTE is evaluated for each reference.
"""

import os

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark

LOCAL_CLUSTER = os.environ.get("SAIL_MODE") == "local-cluster"
DIVISION_BY_ZERO = "(?i)division by zero"

# Wide rows, so that one evaluation of a CTE is about 50 MB of Arrow data.
ROWS = 400_000
PAD = 120
SOURCE = f"range(0, {ROWS}, 1, 4)"
WIDE = f"repeat('x', {PAD})"


@pytest.fixture
def counted(spark, tmp_path):
    """Registers `counted(x)`, a deterministic Python UDF that returns `x` and logs each call,
    and returns a function that reads the number of calls."""
    from pyspark.sql.functions import udf

    counter = tmp_path / "calls.txt"

    def log(x):
        with open(counter, "a") as f:
            f.write("x")
        return x

    spark.udf.register("counted", udf(log, "bigint"))
    return lambda: counter.stat().st_size if counter.exists() else 0


@pytest.fixture
def ansi(spark):
    spark.conf.set("spark.sql.ansi.enabled", "true")
    yield
    spark.conf.unset("spark.sql.ansi.enabled")


def rows(spark, query):
    return [tuple(row) for row in spark.sql(query).collect()]


def volatile(function="rand()"):
    return f"c AS (SELECT id, {function} AS r, {WIDE} AS pad FROM {SOURCE})"  # noqa: S608


# Volatile CTEs: every reference reads the same evaluation, however the references interleave.

VOLATILE_SHAPES = {
    # A hash join reads one side completely before it reads the other side.
    "self-join": (
        "SELECT count(*), count_if(a.r = b.r), sum(length(a.pad) + length(b.pad)) FROM c a JOIN c b ON a.id = b.id",
        (ROWS, ROWS, 2 * PAD * ROWS),
    ),
    "union-all": (
        "SELECT count(*), sum(length(pad)) FROM (SELECT id, r, count(*) AS k, max(pad) AS pad "
        "FROM (SELECT id, r, pad FROM c UNION ALL SELECT id, r, pad FROM c) GROUP BY id, r) WHERE k = 2",
        (ROWS, PAD * ROWS),
    ),
    "three-references": (
        "SELECT count(*) FROM c a JOIN c b ON a.id = b.id AND a.r = b.r JOIN c d ON a.id = d.id AND a.r = d.r",
        (ROWS,),
    ),
    "scalar-subquery": (
        "SELECT count(*), count_if(a.r = b.r), max(a.r) = (SELECT max(r) FROM c) FROM c a JOIN c b ON a.id = b.id",
        (ROWS, ROWS, True),
    ),
    "exists": (
        "SELECT count(*) FROM c x WHERE EXISTS (SELECT 1 FROM c y WHERE y.id = x.id AND y.r = x.r)",
        (ROWS,),
    ),
    "different-filters": (
        "SELECT count(*) FROM (SELECT * FROM c WHERE id % 2 = 0) x JOIN (SELECT * FROM c WHERE id % 3 = 0) y "
        "ON x.id = y.id AND x.r = y.r",
        ((ROWS + 5) // 6,),
    ),
    # The other reference stops after 10 rows, and this one still reads every row.
    "limit-and-full": (
        "SELECT count(*), count_if(x.r = y.r), sum(length(y.pad)) FROM (SELECT * FROM c LIMIT 10) x "
        "JOIN c y ON x.id = y.id",
        (10, 10, 10 * PAD),
    ),
    "sorted-limit-and-full": (
        "SELECT count(*), count_if(x.r = y.r) FROM (SELECT * FROM c ORDER BY id LIMIT 10) x JOIN c y ON x.id = y.id",
        (10, 10),
    ),
    # Two LIMITs without ORDER BY may read different rows, but a row read by both has one value.
    "two-limits": (
        "SELECT count_if(x.r <> y.r) FROM (SELECT * FROM c LIMIT 50) x JOIN (SELECT * FROM c LIMIT 50) y ON x.id = y.id",
        (0,),
    ),
    "nested": (
        ", d AS (SELECT a.id, a.r FROM c a JOIN c b ON a.id = b.id AND a.r = b.r) "
        "SELECT count(*) FROM d x JOIN d y ON x.id = y.id AND x.r = y.r",
        (ROWS,),
    ),
}


@pytest.mark.parametrize("shape", list(VOLATILE_SHAPES))
@pytest.mark.parametrize("function", ["rand()", "uuid()"])
def test_references_to_large_volatile_cte_read_one_evaluation(spark, function, shape):
    suffix, expected = VOLATILE_SHAPES[shape]
    query = f"WITH {volatile(function)}" + ("" if suffix.startswith(",") else " ") + suffix
    assert rows(spark, query) == [expected]


def test_partition_wise_join_of_volatile_cte(spark):
    """The aggregate partitions both references by the join key, so the join can read the
    references partition by partition."""
    query = (
        "WITH c AS (SELECT id % 100000 AS k, sum(r) AS s FROM (SELECT id, rand() AS r FROM range(0, 2000000, 1, 8)) "
        "GROUP BY 1) SELECT count(*), count_if(a.s = b.s) FROM c a JOIN c b ON a.k = b.k"
    )
    assert rows(spark, query) == [(100000, 100000)]


def test_deterministic_cte_over_volatile_cte(spark):
    """`d` is deterministic but reads the volatile `c`: both references of `d` and the direct
    references of `c` read one evaluation of `c`."""
    query = (
        f"WITH {volatile()}, d AS (SELECT id % 1000 AS g, sum(r) AS s, max(pad) AS pad FROM c GROUP BY 1) "  # noqa: S608
        "SELECT count(*), count_if(abs(a.s - b.s) < 1e-9), sum(length(a.pad) + length(b.pad)), "
        "abs((SELECT sum(r) FROM c) - (SELECT sum(s) FROM d)) < 1e-6 FROM d a JOIN d b ON a.g = b.g"
    )
    assert rows(spark, query) == [(1000, 1000, 2 * PAD * 1000, True)]


# Deterministic CTEs: the results of an engine that evaluates the CTE for each reference.


def test_deterministic_cte_read_by_both_sides_of_a_join(spark):
    query = (
        f"WITH c AS (SELECT id % 200000 AS k, count(*) AS n, max({WIDE}) AS pad FROM {SOURCE} GROUP BY 1) "  # noqa: S608
        "SELECT count(*), sum(a.n * b.n), sum(length(a.pad) + length(b.pad)) FROM c a JOIN c b ON a.k = b.k"
    )
    assert rows(spark, query) == [(200000, 800000, 2 * PAD * 200000)]


def test_deterministic_cte_read_by_union_branches_and_a_scalar_subquery(spark):
    query = (
        f"WITH c AS (SELECT id % 200000 AS k, count(*) AS n, max({WIDE}) AS pad FROM {SOURCE} GROUP BY 1) "  # noqa: S608
        "SELECT count(*), sum(n), sum(length(pad)), (SELECT max(k) FROM c) FROM (SELECT * FROM c UNION ALL SELECT * FROM c)"
    )
    assert rows(spark, query) == [(400000, 2 * ROWS, 2 * PAD * 200000, 199999)]


def test_partition_wise_join_of_deterministic_cte(spark):
    query = (
        "WITH c AS (SELECT id % 100000 AS k, sum(id) AS s FROM range(0, 2000000, 1, 8) GROUP BY 1) "
        "SELECT count(*), sum(a.s), sum(b.s) FROM c a JOIN c b ON a.k = b.k"
    )
    assert rows(spark, query) == [(100000, 1999999000000, 1999999000000)]


def test_nested_chain_of_deterministic_ctes(spark):
    ctes = ["c0 AS (SELECT id % 5000 AS k, id AS v FROM range(0, 20000, 1, 4))"]
    ctes.extend(
        f"c{i} AS (SELECT a.k, a.v FROM c{i - 1} a JOIN (SELECT DISTINCT k FROM c{i - 1}) b ON a.k = b.k)"  # noqa: S608
        for i in range(1, 6)
    )
    query = f"WITH {', '.join(ctes)} SELECT count(*), sum(v) FROM c5"  # noqa: S608
    assert rows(spark, query) == [(20000, 199990000)]


def test_each_reference_reads_all_rows_of_one_evaluation(spark):
    """A LIMIT without ORDER BY can return different rows in each evaluation, and a
    repartitioned CTE can assign rows to different partitions in each evaluation. Each
    reference must still read all rows of one evaluation, with no row missing or repeated."""
    body = f"SELECT k, max({WIDE}) AS pad FROM (SELECT id % 300000 AS k FROM {SOURCE}) GROUP BY k"  # noqa: S608
    for cte, n in [
        (f"c AS ({body} LIMIT 250000)", 250000),
        (f"c AS (SELECT /*+ REPARTITION(7) */ * FROM ({body}))", 300000),  # noqa: S608
    ]:
        query = (
            f"WITH {cte} SELECT count(a.k), count(DISTINCT a.k), count(b.k), count(DISTINCT b.k), "  # noqa: S608
            "sum(length(a.pad)) FROM c a FULL JOIN c b ON a.k = b.k"
        )
        assert rows(spark, query) == [(n, n, n, n, PAD * n)], cte


def test_early_stopping_consumers_over_deterministic_cte(spark):
    body = "c AS (SELECT id % 100000 AS k, count(*) AS n FROM range(0, 2000000, 1, 8) GROUP BY 1)"
    query = f"SELECT count(*) FROM (WITH {body} SELECT a.k FROM c a JOIN c b ON a.k = b.k LIMIT 1)"  # noqa: S608
    assert rows(spark, query) == [(1,)]
    query = (
        f"WITH {body} SELECT count(*), sum(n) FROM "  # noqa: S608
        "(SELECT * FROM (SELECT * FROM c LIMIT 7) UNION ALL SELECT * FROM (SELECT * FROM c LIMIT 5))"
    )
    assert rows(spark, query) == [(12, 240)]


def test_limit_reference_joined_with_full_reference_of_deterministic_cte(spark):
    """The LIMIT reference returns 10 rows in total, not 10 rows per partition or per attempt."""
    query = (
        f"WITH c AS (SELECT id % {ROWS} AS id, max({WIDE}) AS pad FROM {SOURCE} GROUP BY 1) "  # noqa: S608
        "SELECT count(*), sum(length(y.pad)) FROM (SELECT * FROM c LIMIT 10) x JOIN c y ON x.id = y.id"
    )
    assert rows(spark, query) == [(10, 10 * PAD)]


# LIMIT over a volatile CTE.


def test_limits_over_volatile_cte_read_one_evaluation(spark, counted):
    query = (
        f"WITH c AS (SELECT id, counted(id) AS v, rand() AS r FROM {SOURCE}) "  # noqa: S608
        "SELECT count_if(x.r <> y.r), count(*) <= 10, count_if(x.v = y.v) = count(*) "
        "FROM (SELECT * FROM c LIMIT 10) x JOIN (SELECT * FROM c LIMIT 10) y ON x.id = y.id"
    )
    assert rows(spark, query) == [(0, True, True)]
    if not is_jvm_spark() and not LOCAL_CLUSTER:
        # Spark evaluates the whole CTE before the references read it. Sail local mode stops
        # evaluating it once every reference has stopped reading. In cluster mode, the stage
        # of the CTE runs ahead of its references, as every other stage does.
        assert counted() < ROWS // 4


def test_limit_and_exists_over_volatile_cte(spark, counted):  # noqa: ARG001
    query = (
        f"WITH c AS (SELECT id, counted(id) AS v, rand() AS r FROM {SOURCE}) "  # noqa: S608
        "SELECT count(*), count(x.v) FROM (SELECT * FROM c LIMIT 10) x "
        "WHERE EXISTS (SELECT 1 FROM c y WHERE y.id = x.id AND y.r = x.r)"
    )
    assert rows(spark, query) == [(10, 10)]


# A reference that is never read does not evaluate the CTE.

EMPTY = "(SELECT id AS k FROM range(100) WHERE id * 7 % 1000 = 999)"

UNREAD = {
    "join-body": "c AS (SELECT a.id AS k, counted(a.id) AS v FROM range(0, 1000, 1, 2) a "
    "JOIN range(0, 10) b ON a.id % 10 = b.id)",
    "union-body": "c AS (SELECT k, counted(k) AS v FROM (SELECT id AS k FROM range(0, 1000, 1, 2) "
    "UNION ALL SELECT id FROM range(0, 1)))",
    "aggregate-body": "c AS (SELECT id % 100 AS k, sum(counted(id)) AS v FROM range(0, 1000, 1, 2) GROUP BY 1)",
    "volatile-body": "c AS (SELECT a.id AS k, counted(a.id) + rand() AS v FROM range(0, 1000, 1, 2) a "
    "JOIN range(0, 10) b ON a.id % 10 = b.id)",
}


@pytest.mark.parametrize("cte", list(UNREAD))
def test_cte_is_not_evaluated_when_the_other_join_side_is_empty(spark, counted, cte):
    query = (
        f"WITH {UNREAD[cte]} SELECT count(*), sum(x.v + y.v + e.k) FROM {EMPTY} e "  # noqa: S608
        "JOIN c x ON e.k = x.k JOIN c y ON x.k = y.k"
    )
    assert rows(spark, query) == [(0, None)]
    if not is_jvm_spark() and not LOCAL_CLUSTER:
        # Spark may evaluate a CTE in a stage of its own before it finds the other side empty.
        # In cluster mode, the stage of the CTE starts when it is scheduled, as every other
        # stage does, before the other side is found empty.
        assert counted() == 0


# Errors: a query fails if and only if a reference reads a row that fails.

FAILS_AT_3 = "100 DIV (id - 3)"
VOLATILE_FALLIBLE = f"c AS (SELECT id AS k, id AS v, rand() AS u, {FAILS_AT_3} AS f FROM range(0, 2000, 1, 3))"  # noqa: S608
DETERMINISTIC_FALLIBLE = (
    "c AS (SELECT k, max(v) AS v, 100 DIV (k - 3) AS f FROM (SELECT id AS k, id AS v FROM range(0, 2000, 1, 3)) "
    "GROUP BY k)"
)
EXPECTED_SUM_F = sum(100 // (k - 3) if (k - 3) > 0 else -(100 // (3 - k)) for k in range(2000) if k != 3)  # noqa: PLR2004


@pytest.mark.parametrize("cte", [VOLATILE_FALLIBLE, DETERMINISTIC_FALLIBLE], ids=["volatile", "deterministic"])
def test_failing_row_filtered_out_by_the_reference_that_reads_the_column(spark, ansi, cte):  # noqa: ARG001
    """Only `a` filters out the failing row, and `b` does not read the failing column."""
    query = f"WITH {cte} SELECT count(*), sum(a.f) FROM (SELECT * FROM c WHERE k <> 3) a JOIN c b ON a.k = b.k"  # noqa: S608
    assert rows(spark, query) == [(1999, EXPECTED_SUM_F)]


@pytest.mark.parametrize("cte", [VOLATILE_FALLIBLE, DETERMINISTIC_FALLIBLE], ids=["volatile", "deterministic"])
def test_failing_row_filtered_out_by_the_union_branch_that_reads_the_column(spark, ansi, cte):  # noqa: ARG001
    """Only the first branch reads the failing column, and it filters out the failing row.
    Spark materializes a volatile CTE with the failing column for every row, because the
    second branch has no filter to push into it, so it fails; Sail computes the column only
    for the rows that the first branch keeps, as it does when the CTE is evaluated for each
    reference."""
    query = f"WITH {cte} SELECT count(*), sum(f) FROM (SELECT k, f FROM c WHERE k <> 3 UNION ALL SELECT k, 0 FROM c)"  # noqa: S608
    if is_jvm_spark() and cte == VOLATILE_FALLIBLE:
        with pytest.raises(Exception, match="DIVIDE_BY_ZERO"):
            spark.sql(query).collect()
    else:
        assert rows(spark, query) == [(3999, EXPECTED_SUM_F)]


@pytest.mark.parametrize("cte", [VOLATILE_FALLIBLE, DETERMINISTIC_FALLIBLE], ids=["volatile", "deterministic"])
def test_failing_column_that_no_reference_reads(spark, ansi, cte):  # noqa: ARG001
    query = f"WITH {cte} SELECT count(*), sum(a.v) FROM c a JOIN c b ON a.k = b.k"  # noqa: S608
    assert rows(spark, query) == [(2000, 1999000)]


@pytest.mark.parametrize("volatile", [False, True])
def test_failing_row_filtered_out_by_every_reference(spark, ansi, volatile):  # noqa: ARG001
    if volatile:
        cte = "c AS (SELECT id AS k, rand() AS r, 10 DIV (id - 5) AS q FROM range(0, 1000, 1, 4))"
        on, expected = "a.k = b.k AND a.r = b.r", [(999, sum(2 * int(10 / (k - 5)) for k in range(1000) if k != 5))]  # noqa: PLR2004
    else:
        cte = "c AS (SELECT k, 10 DIV (k - 5) AS q FROM (SELECT id % 10 AS k FROM range(0, 1000, 1, 4)) GROUP BY k)"
        on, expected = "a.k = b.k", [(9, -4)]
    query = f"WITH {cte} SELECT count(*), sum(a.q + b.q) FROM c a JOIN c b ON {on} WHERE a.k <> 5 AND b.k <> 5"  # noqa: S608
    assert rows(spark, query) == expected


def test_limits_over_volatile_cte_do_not_read_a_failing_row(spark, ansi):  # noqa: ARG001
    """The last row of the CTE fails, and the references read 5 rows each. The two references
    may read different rows, but a row that both read has one value."""
    body = "WITH c AS (SELECT id, rand() AS r, 10 DIV (id - 199999) AS q FROM range(0, 200000, 1, 1)) "
    union = (
        f"{body} SELECT count(*), count(q), max(id) < 199999 FROM "  # noqa: S608
        "(SELECT * FROM (SELECT * FROM c LIMIT 5) UNION ALL SELECT * FROM (SELECT * FROM c LIMIT 5))"
    )
    join = (
        f"{body} SELECT count(*) <= 5, count_if(x.r = y.r AND x.q = y.q) = count(*) FROM (SELECT * FROM c LIMIT 5) x "  # noqa: S608
        "JOIN (SELECT * FROM c LIMIT 5) y ON x.id = y.id"
    )
    if is_jvm_spark():
        for query in [union, join]:
            with pytest.raises(Exception, match="DIVIDE_BY_ZERO"):
                spark.sql(query).collect()
    else:
        assert rows(spark, union) == [(10, 10, True)]
        assert rows(spark, join) == [(True, True)]


def test_sorted_limits_over_deterministic_cte_do_not_compute_a_failing_column(spark, ansi):  # noqa: ARG001
    """The failing column is computed for the last key only, and each reference keeps the
    first 5 keys. Spark computes the column in the aggregate before the sort, so it fails;
    Sail computes it after the limit, as it does when the CTE is evaluated for each reference."""
    query = (
        "WITH c AS (SELECT k, 10 DIV (k - 199999) AS q FROM (SELECT id AS k FROM range(0, 200000, 1, 1)) GROUP BY k) "
        "SELECT count(*), count(x.q) FROM (SELECT * FROM c ORDER BY k LIMIT 5) x "
        "JOIN (SELECT * FROM c ORDER BY k LIMIT 5) y ON x.k = y.k"
    )
    if is_jvm_spark():
        with pytest.raises(Exception, match="DIVIDE_BY_ZERO"):
            spark.sql(query).collect()
    else:
        assert rows(spark, query) == [(5, 5)]


def test_failing_volatile_cte_whose_references_are_not_read(spark, ansi):  # noqa: ARG001
    """Every reference is under a filter on a scalar subquery that is false at runtime."""
    query = (
        f"WITH {VOLATILE_FALLIBLE} SELECT count(*), sum(a.f) FROM c a JOIN c b ON a.k = b.k "  # noqa: S608
        "WHERE (SELECT count(*) FROM range(0, 5, 1, 1) WHERE rand() < -1) > 0"
    )
    if is_jvm_spark():
        with pytest.raises(Exception, match="DIVIDE_BY_ZERO"):
            spark.sql(query).collect()
    else:
        assert rows(spark, query) == [(0, None)]


def test_failing_deterministic_cte_whose_references_are_not_read(spark, ansi):  # noqa: ARG001
    """Every reference is the probe side of a join whose build side is empty at runtime."""
    query = (
        "WITH c AS (SELECT k, sum(id) AS s, 10 DIV (k - 5) AS q FROM (SELECT id % 10 AS k, id FROM range(0, 1000, 1, 4)) "  # noqa: S608
        f"GROUP BY k) SELECT count(*) FROM {EMPTY} e JOIN c a ON e.k = a.k JOIN c b ON a.k = b.k AND a.q = b.q"
    )
    assert rows(spark, query) == [(0,)]


@pytest.mark.parametrize(
    "query",
    [
        f"WITH {VOLATILE_FALLIBLE} SELECT count(*), sum(a.f + b.f) FROM c a JOIN c b ON a.k = b.k",  # noqa: S608
        f"WITH {DETERMINISTIC_FALLIBLE} SELECT count(*), sum(a.f) FROM c a JOIN c b ON a.k = b.k",  # noqa: S608
        f"WITH {DETERMINISTIC_FALLIBLE} SELECT count(*), sum(CASE WHEN a.k <> 3 THEN a.f END) FROM c a JOIN c b ON a.k = b.k",  # noqa: S608
        f"WITH {DETERMINISTIC_FALLIBLE} SELECT (SELECT sum(CASE WHEN k <> 3 THEN f END) FROM c), (SELECT count(*) FROM c)",  # noqa: S608
        f"WITH {VOLATILE_FALLIBLE} SELECT (SELECT sum(CASE WHEN k <> 3 THEN f END) FROM c), (SELECT count(*) FROM c)",  # noqa: S608
    ],
    ids=["volatile", "deterministic", "guarded-join", "guarded-aggregate", "guarded-volatile"],
)
def test_failing_row_read_by_a_reference(spark, ansi, query):  # noqa: ARG001
    """The failing row reaches a reference. The CASE guards do not help: the CTE computes the
    failing column before the reference evaluates the guard."""
    with pytest.raises(Exception, match=DIVISION_BY_ZERO):
        spark.sql(query).collect()
