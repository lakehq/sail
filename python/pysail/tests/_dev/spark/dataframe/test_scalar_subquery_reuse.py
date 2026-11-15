"""Repeated aggregate scalar subqueries.

Spark plans identical uncorrelated scalar subqueries once and reuses the result
(`ReuseExchangeAndSubquery` compares canonicalized plans, which ignore expression IDs).
These tests check the results on every engine, and on Sail they also check how many
scalar subqueries are planned.
"""

import datetime
import decimal
import os
import re
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.testing.spark.utils.sql import escape_sql_string_literal

# The largest value of `x` in the `t` table of the `tables` fixture.
MAX_X = 20
SCALAR_SUBQUERY_SLOTS = re.compile(r"ScalarSubqueryExec: subqueries=(\d+)")


def scalar_subquery_slots(df):
    return [int(n) for n in SCALAR_SUBQUERY_SLOTS.findall(df._explain_string())]  # noqa: SLF001


def check(df, expected, slots):
    assert sorted(tuple(row) for row in df.collect()) == sorted(expected)
    if not is_jvm_spark():
        assert sorted(scalar_subquery_slots(df)) == sorted(slots)


@pytest.fixture(scope="module")
def tables(spark, tmp_path_factory):
    """Returns the names of a parquet table, a second parquet table with the same schema,
    a temporary view and a permanent view over the first table."""
    suffix = uuid.uuid4().hex[:8]
    t, u, tv, pv = (
        f"{x}_{suffix}" for x in ("aggregate_reuse_t", "aggregate_reuse_u", "aggregate_reuse_tv", "aggregate_reuse_pv")
    )
    root = tmp_path_factory.mktemp("aggregate_reuse")
    for name, rows in [
        (t, "(1, 10, 'a'), (1, 20, 'b'), (2, 5, 'c'), (2, NULL, 'd')"),
        (u, "(1, 1, 'x'), (2, 2, 'y')"),
    ]:
        location = escape_sql_string_literal(str(root / name))
        spark.sql(f"CREATE TABLE {name} (g INT, x INT, y STRING) USING parquet LOCATION '{location}'")
        spark.sql(f"INSERT INTO {name} VALUES {rows}")  # noqa: S608
    spark.sql(f"CREATE TEMPORARY VIEW {tv} AS SELECT * FROM {t}")  # noqa: S608
    spark.sql(f"CREATE VIEW {pv} AS SELECT * FROM {t}")  # noqa: S608
    yield {"t": t, "u": u, "tv": tv, "pv": pv}
    spark.sql(f"DROP VIEW IF EXISTS {pv}")
    spark.sql(f"DROP VIEW IF EXISTS {tv}")
    for name in (t, u):
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize(
    ("query", "expected", "slots"),
    [
        pytest.param(
            "SELECT (SELECT max(x) FROM {t}) AS a, (SELECT max(x) FROM {t}) AS b", [(20, 20)], [1], id="table"
        ),
        pytest.param(
            "SELECT (SELECT max(x) FROM {tv}) AS a, (SELECT max(x) FROM {tv}) AS b", [(20, 20)], [1], id="temp-view"
        ),
        pytest.param(
            "SELECT (SELECT max(x) FROM {pv}) AS a, (SELECT max(x) FROM {pv}) AS b", [(20, 20)], [1], id="view"
        ),
        pytest.param(
            "WITH c AS (SELECT x FROM {t}) SELECT (SELECT max(x) FROM c) AS a, (SELECT max(x) FROM c) AS b",
            [(20, 20)],
            [1],
            id="cte",
        ),
        pytest.param(
            "SELECT (SELECT max(x) FROM VALUES (1), (3) AS v(x)) AS a, (SELECT max(x) FROM VALUES (1), (3) AS v(x)) AS b",
            [(3, 3)],
            [1],
            id="inline-table",
        ),
        pytest.param(
            "SELECT (SELECT count(*) FROM {t}) AS a, (SELECT count(*) FROM {t}) AS b, (SELECT count(x) FROM {t}) AS c",
            [(4, 4, 3)],
            [2],
            id="count",
        ),
        pytest.param(
            "SELECT (SELECT max(s) FROM (SELECT g, sum(x) AS s FROM {t} GROUP BY g)) AS a, "
            "(SELECT max(s) FROM (SELECT g, sum(x) AS s FROM {t} GROUP BY g)) AS b",
            [(30, 30)],
            [1],
            id="group-by",
        ),
        pytest.param(
            "SELECT (SELECT max(x) FROM {t} WHERE y <> 'b') AS a, (SELECT max(x) FROM {t} WHERE y <> 'b') AS b",
            [(10, 10)],
            [1],
            id="filter",
        ),
        pytest.param(
            "SELECT g FROM {t} WHERE x = (SELECT max(x) FROM {t}) "
            "UNION ALL SELECT g FROM {t} WHERE x = (SELECT max(x) FROM {t})",
            [(1,), (1,)],
            [1],
            id="union-filter",
        ),
        pytest.param(
            "SELECT (SELECT max(x) FROM {t} WHERE x < (SELECT max(x) FROM {t})) AS a, "
            "(SELECT max(x) FROM {t} WHERE x < (SELECT max(x) FROM {t})) AS b",
            [(10, 10)],
            [1, 1],
            id="nested",
        ),
        pytest.param(
            "WITH avg_x AS (SELECT avg(x) AS a FROM {t}) "
            "SELECT 's' AS c, count(*) AS n FROM {t} WHERE x > (SELECT a FROM avg_x) "
            "UNION ALL SELECT 'w' AS c, count(*) AS n FROM {t} WHERE x > (SELECT a FROM avg_x)",
            [("s", 1), ("w", 1)],
            [1],
            id="cte-body-aggregate",
        ),
        pytest.param(
            "SELECT (SELECT max(s) FROM (SELECT g, sum(x) AS s FROM {t} GROUP BY ROLLUP(g))) AS a, "
            "(SELECT max(s) FROM (SELECT g, sum(x) AS s FROM {t} GROUP BY ROLLUP(g))) AS b",
            [(35, 35)],
            [1],
            id="rollup",
        ),
        pytest.param(
            "SELECT (SELECT max(s) FROM (SELECT g, grouping(g) AS gg, sum(x) AS s FROM {t} "
            "GROUP BY GROUPING SETS ((g), ())) WHERE gg = 0) AS a, "
            "(SELECT max(s) FROM (SELECT g, grouping(g) AS gg, sum(x) AS s FROM {t} "
            "GROUP BY GROUPING SETS ((g), ())) WHERE gg = 0) AS b",
            [(30, 30)],
            [1],
            id="grouping-sets",
        ),
        pytest.param(
            "WITH c AS (SELECT max(o.x) AS m FROM {t} o WHERE EXISTS (SELECT 1 FROM {u} i WHERE i.g = o.g)) "
            "SELECT (SELECT m FROM c) AS a, (SELECT m FROM c) AS b",
            [(20, 20)],
            [1],
            id="correlated-exists-cte",
        ),
        pytest.param(
            "WITH c AS (SELECT x FROM {t}) SELECT (SELECT max(x) FROM c) AS a, (SELECT max(x) FROM {t}) AS b",
            [(20, 20)],
            [1],
            id="cte-and-direct-read",
        ),
        pytest.param(
            # The optimizer turns the correlated EXISTS into a semi join before the subqueries
            # are compared, so the repeated bodies compare equal.
            "SELECT (SELECT max(o.x) FROM {t} o WHERE EXISTS (SELECT 1 FROM {u} i WHERE i.g = o.g)) AS a, "
            "(SELECT max(o.x) FROM {t} o WHERE EXISTS (SELECT 1 FROM {u} i WHERE i.g = o.g)) AS b",
            [(20, 20)],
            [1],
            id="correlated-exists-repeated",
        ),
        pytest.param(
            "SELECT (SELECT max(w.x1 + w.x) FROM (SELECT v.g, v.x, v.x + 1 AS x1, upper(v.y) AS uy FROM {t} AS v) AS w "
            "WHERE w.uy <> 'D') AS a, "
            "(SELECT max(w.x1 + w.x) FROM (SELECT v.g, v.x, v.x + 1 AS x1, upper(v.y) AS uy FROM {t} AS v) AS w "
            "WHERE w.uy <> 'D') AS b",
            [(41, 41)],
            [1],
            id="mixed-projection",
        ),
        pytest.param(
            "WITH z AS (SELECT x AS q, g AS p FROM {t} UNION ALL SELECT x AS q, g AS p FROM {u}) "
            "SELECT (SELECT avg(CAST(q * p AS DECIMAL(18,2))) FROM z) AS a, "
            "(SELECT avg(CAST(q * p AS DECIMAL(18,2))) FROM z) AS b",
            [(decimal.Decimal(9), decimal.Decimal(9))],
            [1],
            id="q14-union-cte",
        ),
    ],
)
def test_repeated_aggregate_scalar_subqueries_share_one_result(spark, tables, query, expected, slots):
    check(spark.sql(query.format(**tables)), expected, slots)


@pytest.mark.parametrize(
    ("query", "expected", "slots"),
    [
        pytest.param(
            "SELECT (SELECT max(x) FROM {t}) AS a, (SELECT min(x) FROM {t}) AS b", [(20, 5)], [2], id="other-aggregate"
        ),
        pytest.param(
            "SELECT (SELECT max(x) FROM {t}) AS a, (SELECT max(x) FROM {u}) AS b", [(20, 2)], [2], id="other-table"
        ),
        pytest.param(
            "SELECT (SELECT max(x) FROM {t} WHERE g = 1) AS a, (SELECT max(x) FROM {t} WHERE g = 2) AS b",
            [(20, 5)],
            [2],
            id="other-literal",
        ),
        pytest.param(
            "SELECT (SELECT max(x) FROM {t} WHERE g = 1) AS a, (SELECT max(x) FROM {t} WHERE g = 1L) AS b",
            [(20, 20)],
            None,
            id="other-literal-type",
        ),
        pytest.param(
            "WITH c AS (SELECT x FROM {t}) SELECT (SELECT max(x) FROM c) AS a, "
            "(WITH c AS (SELECT x + 1 AS x FROM {t}) SELECT (SELECT max(x) FROM c)) AS b",
            [(20, 21)],
            [2, 1],
            id="shadowed-cte",
        ),
        pytest.param(
            "WITH c AS (SELECT x FROM {t}) SELECT d.a, (SELECT max(x) FROM c) AS b "
            "FROM (WITH c AS (SELECT x + 1 AS x FROM {t}) SELECT (SELECT max(x) FROM c) AS a) AS d",
            [(21, 20)],
            [2],
            id="shadowed-cte-derived-table",
        ),
        pytest.param(
            "SELECT (SELECT max(x) FROM {t}) AS a, (SELECT max(x) FROM {tv}) AS b",
            [(20, 20)],
            None,
            id="table-and-view",
        ),
    ],
)
def test_different_aggregate_scalar_subqueries_are_not_shared(spark, tables, query, expected, slots):
    """`slots` is None where sharing is allowed but not required."""
    df = spark.sql(query.format(**tables))
    assert [tuple(row) for row in df.collect()] == expected
    if slots is not None and not is_jvm_spark():
        assert sorted(scalar_subquery_slots(df)) == sorted(slots)


def test_repeated_volatile_aggregate_scalar_subqueries_are_not_shared(spark, tables):
    t = tables["t"]
    df = spark.sql(
        f"SELECT (SELECT max(x) + rand() FROM {t}) AS a, (SELECT max(x) + rand() FROM {t}) AS b, "  # noqa: S608
        f"(SELECT concat(max(y), uuid()) FROM {t}) AS c, (SELECT concat(max(y), uuid()) FROM {t}) AS d"
    )
    a, b, c, d = df.collect()[0]
    assert MAX_X <= a < MAX_X + 1
    assert MAX_X <= b < MAX_X + 1
    assert a != b
    assert c.startswith("d")
    assert d.startswith("d")
    assert c != d
    if not is_jvm_spark():
        assert scalar_subquery_slots(df) == [4]


def test_repeated_correlated_aggregate_subqueries(spark, tables):
    t, u = tables["t"], tables["u"]
    df = spark.sql(
        f"SELECT g, (SELECT max(x) FROM {t} WHERE {t}.g = o.g) AS a, (SELECT max(x) FROM {t} WHERE {t}.g = o.g) AS b "  # noqa: S608
        f"FROM {u} AS o ORDER BY g"
    )
    assert [tuple(row) for row in df.collect()] == [(1, 20, 20), (2, 5, 5)]


@pytest.mark.spark(version=">=4", reason="Uses DataFrame.scalar")
def test_repeated_dataframe_aggregate_scalar_subqueries(spark, tables):
    from pyspark.sql import functions as F  # noqa: N812

    t = spark.table(tables["t"])
    df = spark.range(1).select(
        t.select(F.max("x")).scalar().alias("a"),
        t.select(F.max("x")).scalar().alias("b"),
        t.select(F.min("x")).scalar().alias("c"),
    )
    assert [tuple(row) for row in df.collect()] == [(20, 20, 5)]
    if not is_jvm_spark():
        assert scalar_subquery_slots(df) == [2]


@pytest.mark.spark(version=">=4", reason="Uses DataFrame.scalar")
def test_dataframe_scalar_subquery_column_used_twice(spark, tables):
    from pyspark.sql import functions as F  # noqa: N812

    # The client sends the subquery plan once for each use of the column.
    column = spark.table(tables["t"]).select(F.max("x")).scalar()
    df = spark.range(1).select(column.alias("a"), (column + 1).alias("b"))
    assert [tuple(row) for row in df.collect()] == [(MAX_X, MAX_X + 1)]
    if not is_jvm_spark():
        assert scalar_subquery_slots(df) == [1]


@pytest.mark.parametrize(
    ("query", "expected", "slots"),
    [
        pytest.param(
            "SELECT (SELECT sum(id) FROM range(10)) AS a, (SELECT sum(id) FROM range(10)) AS b",
            [(45, 45)],
            [1],
            id="range",
        ),
        pytest.param(
            "SELECT (SELECT max(x) FROM (SELECT x FROM {t} SORT BY x)) AS a, "
            "(SELECT max(x) FROM (SELECT x FROM {t} SORT BY x)) AS b",
            [(20, 20)],
            [1],
            id="sort-by",
        ),
        pytest.param(
            "SELECT (SELECT max(x) FROM (SELECT /*+ REPARTITION(3) */ x FROM {t})) AS a, "
            "(SELECT max(x) FROM (SELECT /*+ REPARTITION(3) */ x FROM {t})) AS b",
            [(20, 20)],
            [1],
            id="repartition-hint",
        ),
        pytest.param(
            "SELECT (SELECT first(x) FROM (SELECT x FROM {t} ORDER BY x DESC NULLS LAST)) AS a, "
            "(SELECT first(x) FROM (SELECT x FROM {t} ORDER BY x DESC NULLS LAST)) AS b",
            [(20, 20)],
            [1],
            id="ordered-first",
        ),
        pytest.param(
            "SELECT (SELECT collect_list(x) FROM (SELECT x FROM {t} ORDER BY x)) AS a, "
            "(SELECT collect_list(x) FROM (SELECT x FROM {t} ORDER BY x)) AS b",
            [([5, 10, 20], [5, 10, 20])],
            [1],
            id="ordered-collect-list",
        ),
        pytest.param(
            "SELECT (SELECT sum(id) FROM range(10)) AS a, (SELECT sum(id) FROM range(11)) AS b",
            [(45, 55)],
            [2],
            id="other-range",
        ),
        pytest.param(
            "SELECT (SELECT sum(id) FROM range(0, 10, 1, 2)) AS a, (SELECT sum(id) FROM range(0, 10, 1, 3)) AS b",
            [(45, 45)],
            [2],
            id="other-range-partitions",
        ),
        pytest.param(
            "SELECT (SELECT first(x) FROM (SELECT x FROM {t} ORDER BY x ASC NULLS LAST)) AS a, "
            "(SELECT first(x) FROM (SELECT x FROM {t} ORDER BY x DESC NULLS LAST)) AS b",
            [(5, 20)],
            [2],
            id="other-sort-direction",
        ),
        pytest.param(
            "SELECT (SELECT first(x) FROM (SELECT x FROM {t} ORDER BY x ASC NULLS FIRST)) AS a, "
            "(SELECT first(x) FROM (SELECT x FROM {t} ORDER BY x ASC NULLS LAST)) AS b",
            [(None, 5)],
            [2],
            id="other-nulls-ordering",
        ),
        pytest.param(
            "SELECT (SELECT max(m) FROM (SELECT monotonically_increasing_id() AS m FROM range(0, 10, 1, 2))) AS a, "
            "(SELECT max(m) FROM (SELECT monotonically_increasing_id() AS m FROM range(0, 10, 1, 2))) AS b",
            [(8589934596, 8589934596)],
            [1],
            id="monotonically-increasing-id",
        ),
        pytest.param(
            "SELECT (SELECT max(x) FROM (SELECT x FROM {t} ORDER BY x ASC)) AS a, "
            "(SELECT max(x) FROM (SELECT x FROM {t} ORDER BY x DESC)) AS b",
            [(20, 20)],
            [1],
            id="sort-direction-under-max",
        ),
        pytest.param(
            "SELECT (SELECT count(x) FROM (SELECT x FROM {t} SORT BY x)) AS a, (SELECT count(x) FROM {t}) AS b",
            [(3, 3)],
            [1],
            id="sort-by-under-count",
        ),
        pytest.param(
            "SELECT (SELECT avg(x) FROM (SELECT x FROM {t} ORDER BY x ASC)) AS a, "
            "(SELECT avg(x) FROM (SELECT x FROM {t} ORDER BY x DESC)) AS b",
            [(35 / 3, 35 / 3)],
            [1],
            id="sort-direction-under-integer-avg",
        ),
        pytest.param(
            "SELECT (SELECT count(*) FROM (SELECT * FROM range(0, 20, 1, 1) ORDER BY id ASC) AS s "
            "LEFT SEMI JOIN (SELECT * FROM range(0, 20, 1, 1) WHERE id % 2 = 0) AS r ON s.id = r.id) AS a, "
            "(SELECT count(*) FROM (SELECT * FROM range(0, 20, 1, 1) ORDER BY id DESC) AS s "
            "LEFT SEMI JOIN (SELECT * FROM range(0, 20, 1, 1) WHERE id % 2 = 0) AS r ON s.id = r.id) AS b",
            [(10, 10)],
            [1],
            id="sort-under-semi-join-under-count",
        ),
        pytest.param(
            "SELECT (SELECT max(p) FROM (SELECT spark_partition_id() AS p FROM range(0, 10, 1, 2))) AS a, "
            "(SELECT max(p) FROM (SELECT spark_partition_id() AS p FROM range(0, 10, 1, 2))) AS b",
            [(1, 1)],
            [1],
            id="spark-partition-id",
        ),
        pytest.param(
            "SELECT (SELECT max(x) FROM {t} ORDER BY 1) AS a, (SELECT max(x) FROM {t}) AS b",
            [(20, 20)],
            [1],
            id="top-level-sort",
        ),
        pytest.param(
            "SELECT (SELECT max(x) FROM {t} ORDER BY rand(1)) AS a, (SELECT max(x) FROM {t}) AS b",
            [(20, 20)],
            [1],
            id="top-level-sort-by-seeded-random",
        ),
        pytest.param(
            "SELECT (SELECT max(a.x) FROM (SELECT g, x FROM {t} ORDER BY x ASC) a JOIN {u} b ON a.g = b.g) AS a, "
            "(SELECT max(a.x) FROM (SELECT g, x FROM {t} ORDER BY x DESC) a JOIN {u} b ON a.g = b.g) AS b",
            [(20, 20)],
            [1],
            id="sort-under-join",
        ),
        pytest.param(
            "SELECT (SELECT sum(CAST(x AS DOUBLE)) FROM (SELECT x FROM {t} ORDER BY x ASC)) AS a, "
            "(SELECT sum(CAST(x AS DOUBLE)) FROM (SELECT x FROM {t} ORDER BY x DESC)) AS b",
            [(35.0, 35.0)],
            [2],
            id="sort-direction-under-double-sum",
        ),
        pytest.param(
            "SELECT (SELECT max(x) FROM (SELECT x FROM {t} ORDER BY x ASC LIMIT 2)) AS a, "
            "(SELECT max(x) FROM (SELECT x FROM {t} ORDER BY x DESC LIMIT 2)) AS b",
            [(5, 20)],
            [2],
            id="sort-direction-under-limit",
        ),
        pytest.param(
            "SELECT (SELECT max(x) + rand(42) FROM {t}) AS a, (SELECT max(x) + rand(42) FROM {t}) AS b",
            None,
            [1],
            id="seeded-rand",
        ),
        pytest.param(
            "SELECT (SELECT max(r) FROM (SELECT CASE WHEN x > 5 THEN x + rand(42) END AS r FROM {t})) AS a, "
            "(SELECT max(r) FROM (SELECT CASE WHEN x > 5 THEN x + rand(42) END AS r FROM {t})) AS b",
            None,
            [1],
            id="seeded-rand-expression",
        ),
    ],
)
def test_repeated_scalar_subqueries_over_extension_nodes(spark, tables, query, expected, slots):
    df = spark.sql(query.format(**tables))
    if expected is None:
        [(a, b)] = df.collect()
        assert a == b
        if not is_jvm_spark():
            assert scalar_subquery_slots(df) == slots
    else:
        check(df, expected, slots)


def test_sorts_below_joins_and_order_irrelevant_aggregates_are_removed(spark, tables):
    t, u = tables["t"], tables["u"]
    df = spark.sql(
        f"SELECT l.x FROM (SELECT * FROM {t} ORDER BY x) AS l LEFT JOIN {u} AS r ON l.g = r.g LIMIT 10"  # noqa: S608
    )
    assert sorted(row.x for row in df.collect() if row.x is not None) == [5, 10, 20]
    if not is_jvm_spark():
        assert "SortExec" not in df._explain_string()  # noqa: SLF001
    df = spark.sql(f"SELECT g, avg(x) AS a FROM (SELECT * FROM {t} ORDER BY x DESC) GROUP BY g")  # noqa: S608
    assert sorted(tuple(row) for row in df.collect()) == [(1, 15.0), (2, 5.0)]
    if not is_jvm_spark():
        assert "SortExec" not in df._explain_string()  # noqa: SLF001


@pytest.mark.spark(version=">=4", reason="Uses DataFrame.scalar")
def test_repeated_python_udf_scalar_subqueries(spark, tables):
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import DoubleType, LongType

    # Pickled (non-Arrow) UDFs: on JVM Spark 4.2, an Arrow-optimized Python UDF over the nullable INT
    # column `x` never returns (the tasks wait on the Python worker).
    plus_one = F.udf(lambda x: None if x is None else x + 1, LongType(), useArrow=False)
    noise = F.udf(
        lambda x: None if x is None else x + __import__("random").random(), DoubleType(), useArrow=False
    ).asNondeterministic()
    t = spark.table(tables["t"])
    df = spark.range(1).select(
        t.select(F.max(plus_one("x"))).scalar().alias("a"),
        t.select(F.max(plus_one("x"))).scalar().alias("b"),
        t.select(noise("x").alias("n")).select(F.max("n")).scalar().alias("c"),
        t.select(noise("x").alias("n")).select(F.max("n")).scalar().alias("d"),
    )
    [(a, b, c, d)] = df.collect()
    assert (a, b) == (MAX_X + 1, MAX_X + 1)
    # As in Spark, nondeterministic Python UDF bodies are evaluated once.
    assert c == d
    if not is_jvm_spark():
        assert scalar_subquery_slots(df) == [2]

    # Equal nondeterministic calls in one projection are still evaluated separately.
    rows = t.where("x IS NOT NULL").select(noise("x").alias("a"), noise("x").alias("b")).collect()
    assert all(row.a != row.b for row in rows)


@pytest.mark.spark(version=">=4", reason="Uses DataFrame.scalar")
def test_repeated_dataframe_scalar_subqueries_over_extension_nodes(spark, tables):
    from pyspark.sql import functions as F  # noqa: N812

    t = spark.table(tables["t"])
    df = spark.range(1).select(
        spark.range(10).select(F.sum("id")).scalar().alias("a"),
        spark.range(10).select(F.sum("id")).scalar().alias("b"),
        t.repartition(3).select(F.max("x")).scalar().alias("c"),
        t.repartition(3).select(F.max("x")).scalar().alias("d"),
        t.sortWithinPartitions("x").select(F.max("x")).scalar().alias("e"),
        t.sortWithinPartitions("x").select(F.max("x")).scalar().alias("f"),
    )
    assert [tuple(row) for row in df.collect()] == [(45, 45, MAX_X, MAX_X, MAX_X, MAX_X)]
    if not is_jvm_spark():
        assert scalar_subquery_slots(df) == [3]


def repeated(body):
    return f"SELECT ({body}) AS a, ({body}) AS b"


R1 = "range(0, 20, 1, 1)"


@pytest.mark.parametrize(
    ("query", "expected", "slots"),
    [
        # Windows over a sorted input (MonotonicId and RequiredSort).
        pytest.param(
            repeated(
                "SELECT max(f) FROM (SELECT first_value(x) OVER (PARTITION BY g) AS f "
                "FROM (SELECT * FROM {t} ORDER BY x DESC NULLS LAST))"
            ),
            [(20, 20)],
            [1],
            id="window-over-sorted-input",
        ),
        # Join branch beyond inner joins: c3-join-left-outer, c3-join-cross, c5-semi-right-first,
        # c5-anti-right-first, c5-in-right-first.
        pytest.param(
            "SELECT (SELECT count(*) FROM (SELECT * FROM {t} ORDER BY x) AS l LEFT JOIN {t} AS r ON l.x = r.x + 5) AS a, "
            "(SELECT count(*) FROM (SELECT * FROM {t} ORDER BY x DESC) AS l LEFT JOIN {t} AS r ON l.x = r.x + 5) AS b",
            [(4, 4)],
            [1],
            id="sort-under-left-join",
        ),
        pytest.param(
            f"SELECT (SELECT first(s.id) FROM {R1} AS s LEFT SEMI JOIN (SELECT * FROM {R1} WHERE id % 2 = 0 ORDER BY id ASC) "  # noqa: S608
            f"AS r ON s.id = r.id) AS a, (SELECT first(s.id) FROM {R1} AS s LEFT SEMI JOIN (SELECT * FROM {R1} "
            "WHERE id % 2 = 0 ORDER BY id DESC) AS r ON s.id = r.id) AS b",
            [(0, 0)],
            [1],
            id="right-side-sort-of-semi-join",
        ),
        pytest.param(
            f"SELECT (SELECT first(s.id) FROM {R1} AS s LEFT ANTI JOIN (SELECT * FROM {R1} WHERE id % 2 = 0 ORDER BY id ASC) "  # noqa: S608
            f"AS r ON s.id = r.id) AS a, (SELECT first(s.id) FROM {R1} AS s LEFT ANTI JOIN (SELECT * FROM {R1} "
            "WHERE id % 2 = 0 ORDER BY id DESC) AS r ON s.id = r.id) AS b",
            [(1, 1)],
            [1],
            id="right-side-sort-of-anti-join",
        ),
        pytest.param(
            f"SELECT (SELECT first(id) FROM {R1} AS s WHERE s.id IN (SELECT id FROM {R1} WHERE id % 2 = 1 ORDER BY id ASC)) AS a, "  # noqa: S608
            f"(SELECT first(id) FROM {R1} AS s WHERE s.id IN (SELECT id FROM {R1} WHERE id % 2 = 1 ORDER BY id DESC)) AS b",
            [(1, 1)],
            [1],
            id="sorted-in-subquery",
        ),
        # removeTopLevelSort through a projection (ORDER BY a column that is not selected): c4-project-over-sort.
        pytest.param(
            "SELECT (SELECT x + 1 AS z FROM {t} WHERE y = 'b' ORDER BY x DESC) AS a, (SELECT x + 1 AS z FROM {t} WHERE y = 'b') AS b",
            [(21, 21)],
            [1],
            id="top-level-sort-below-projection",
        ),
        # Aggregate classification: grouped, DISTINCT, FILTER, bit/bool, decimal (c2-elim-*, c2-grouped-*).
        pytest.param(
            "SELECT (SELECT named_struct('a', count(DISTINCT x), 'b', bit_or(x), 'c', bool_or(x > 9), "
            "'d', sum(CAST(x AS DECIMAL(10, 2))), 'e', max(x) FILTER (WHERE g = 2)) FROM (SELECT * FROM {t} ORDER BY x ASC)) AS a, "
            "(SELECT named_struct('a', count(DISTINCT x), 'b', bit_or(x), 'c', bool_or(x > 9), "
            "'d', sum(CAST(x AS DECIMAL(10, 2))), 'e', max(x) FILTER (WHERE g = 2)) FROM (SELECT * FROM {t} ORDER BY x DESC)) AS b",
            None,
            [1],
            id="order-irrelevant-aggregates",
        ),
        pytest.param(
            "SELECT (SELECT max(s) FROM (SELECT g, sum(x) AS s FROM (SELECT * FROM {t} ORDER BY x ASC) GROUP BY g)) AS a, "
            "(SELECT max(s) FROM (SELECT g, sum(x) AS s FROM (SELECT * FROM {t} ORDER BY x DESC) GROUP BY g)) AS b",
            [(30, 30)],
            [1],
            id="sort-under-grouped-aggregate",
        ),
        # Accepted better-than-Spark: order-irrelevant aggregates under ROLLUP (c2-rollup-max).
        pytest.param(
            "SELECT (SELECT max(m) FROM (SELECT max(x) AS m FROM (SELECT * FROM {t} ORDER BY x ASC NULLS FIRST) GROUP BY ROLLUP(g))) AS a, "
            "(SELECT max(m) FROM (SELECT max(x) AS m FROM (SELECT * FROM {t} ORDER BY x DESC NULLS LAST) GROUP BY ROLLUP(g))) AS b",
            [(20, 20)],
            [1],
            id="sort-under-rollup",
        ),
        # Sorts that must stay: collect_list / last / stddev (CentralMomentAgg), sort-over-sort (out of scope).
        pytest.param(
            "SELECT (SELECT last(x) FROM (SELECT * FROM {t} ORDER BY x ASC NULLS FIRST)) AS a, "
            "(SELECT last(x) FROM (SELECT * FROM {t} ORDER BY x DESC NULLS LAST)) AS b",
            [(20, None)],
            [2],
            id="sort-under-last",
        ),
        # Seeded random near misses and unseeded functions (c3-seeded-*, c3-unseeded-*).
        pytest.param(
            "SELECT (SELECT max(r) FROM (SELECT rand(42) AS r FROM range(0, 40, 1, 4))) AS a, "
            "(SELECT max(r) FROM (SELECT rand(43) AS r FROM range(0, 40, 1, 4))) AS b",
            None,
            [2],
            id="other-seed",
        ),
        pytest.param(
            "SELECT (SELECT max(r) FROM (SELECT rand(42) AS r FROM range(0, 40, 1, 4))) AS a, "
            "(SELECT max(r) FROM (SELECT randn(42) AS r FROM range(0, 40, 1, 4))) AS b",
            None,
            [2],
            id="rand-and-randn",
        ),
        pytest.param(
            repeated("SELECT sum(r) FROM (SELECT randn(1) AS r FROM range(0, 40, 1, 4))"),
            None,
            [1],
            id="seeded-randn",
        ),
        # input_file_* inside subquery bodies (c5-inputfile-*; needs a multi-file table, see below).
    ],
)
def test_more_repeated_scalar_subqueries_over_extension_nodes(spark, tables, query, expected, slots):
    df = spark.sql(query.format(**tables))
    rows = [tuple(row) for row in df.collect()]
    if expected is None:
        [(a, b)] = rows
        assert (a == b) == (slots == [1])
    else:
        assert rows == expected
    if not is_jvm_spark():
        assert scalar_subquery_slots(df) == slots


def test_unseeded_random_functions_are_not_shared(spark):
    for f in ("randn()", "uuid()", "CAST(shuffle(array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)) AS STRING)"):
        df = spark.sql(repeated(f"SELECT max(r) FROM (SELECT {f} AS r FROM range(0, 40, 1, 4))"))  # noqa: S608
        [(a, b)] = df.collect()
        assert a != b
        if not is_jvm_spark():
            assert scalar_subquery_slots(df) == [2]


@pytest.mark.spark(version=">=4", reason="Uses DataFrame.scalar")
def test_repeated_map_partitions_scalar_subqueries(spark):
    """MapPartitions bodies (mapInPandas / mapInArrow / pandas iterator UDF / UDTF): equal functions share
    (also random ones, a == b, as in Spark); different functions, including closures over different
    values with the same name, do not (c2-mapIn*, probe udf-named-diff / map-closure-diff)."""
    import pyarrow.compute as pc
    from pyspark.sql import functions as F  # noqa: N812

    def plus(n):
        def m(batches):
            for batch in batches:
                yield batch + n

        return m

    def arrow_plus(n):
        def m(batches):
            for batch in batches:
                yield batch.__class__.from_arrays([pc.add(batch.column(0), n)], names=batch.schema.names)

        return m

    one, two = plus(1), plus(2)
    a_one = arrow_plus(1)
    body = lambda fn: spark.range(10).mapInPandas(fn, "id long").select(F.max("id")).scalar()  # noqa: E731
    arrow = lambda fn: spark.range(10).mapInArrow(fn, "id long").select(F.max("id")).scalar()  # noqa: E731
    df = spark.range(1).select(
        body(one).alias("a"),
        body(one).alias("b"),
        body(two).alias("c"),
        arrow(a_one).alias("d"),
        arrow(a_one).alias("e"),
    )
    assert [tuple(row) for row in df.collect()] == [(10, 10, 11, 10, 10)]
    if not is_jvm_spark():
        assert scalar_subquery_slots(df) == [3]


def test_repeated_python_udtf_and_iterator_udf_scalar_subqueries(spark):
    from collections.abc import Iterator

    import pandas as pd
    from pyspark.sql.functions import pandas_udf, udtf

    class Series:
        def eval(self, n: int):
            for i in range(n):
                yield (i,)

    @pandas_udf("long")
    def plus_iter(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
        for batch in batches:
            yield batch + 1

    spark.udtf.register("sse_series", udtf(Series, returnType="i: int"))  # nondeterministic by default
    spark.udf.register("sse_plus_iter", plus_iter)
    df = spark.sql(
        "SELECT (SELECT sum(i) FROM sse_series(5)) AS a, (SELECT sum(i) FROM sse_series(5)) AS b, "
        "(SELECT sum(i) FROM sse_series(6)) AS c, "
        "(SELECT max(sse_plus_iter(id)) FROM range(10)) AS d, (SELECT max(sse_plus_iter(id)) FROM range(10)) AS e"
    )
    assert [tuple(row) for row in df.collect()] == [(10, 10, 15, 10, 10)]
    if not is_jvm_spark():
        assert scalar_subquery_slots(df) == [3]


def test_semi_and_anti_joins_keep_the_order_of_a_sorted_left_input(spark):
    """Result order without an outer ORDER BY (c5-exists-left, c5-in-left, c5-not-in-left, c5-mark-left)."""
    left = "(SELECT * FROM range(0, 50, 1, 4) ORDER BY id DESC) AS s"
    right = "range(0, 50, 1, 3)"
    for predicate, first in [
        (f"EXISTS (SELECT 1 FROM {right} AS r WHERE r.id = s.id AND r.id % 3 = 0)", [48, 45, 42]),  # noqa: S608
        (f"s.id IN (SELECT id FROM {right} WHERE id % 3 = 0)", [48, 45, 42]),  # noqa: S608
        (f"s.id NOT IN (SELECT id FROM {right} WHERE id % 3 = 0)", [49, 47, 46]),  # noqa: S608
        (f"EXISTS (SELECT 1 FROM {right} AS r WHERE r.id = s.id AND r.id % 7 = 0) OR s.id > 45", [49, 48, 47]),  # noqa: S608
    ]:
        rows = [row[0] for row in spark.sql(f"SELECT id FROM {left} WHERE {predicate}").collect()]  # noqa: S608
        assert rows[:3] == first
        assert rows == sorted(rows, reverse=True)


def test_repeated_input_file_function_scalar_subqueries(spark, tmp_path):
    """input_file_name / input_file_block_start inside the bodies (c5-inputfile-*)."""
    from pysail.testing.spark.utils.sql import escape_sql_string_literal

    name = "sse_input_file_mf"
    spark.sql(
        f"CREATE TABLE {name} (k INT, v INT) USING parquet LOCATION '{escape_sql_string_literal(str(tmp_path / name))}'"
    )
    try:
        inserts = ("(1, 10), (2, 20)", "(3, 30)", "(4, 40), (5, 50)")
        for rows in inserts:
            spark.sql(f"INSERT INTO {name} VALUES {rows}")  # noqa: S608
        files = f"SELECT count(DISTINCT f) FROM (SELECT input_file_name() AS f FROM {name})"  # noqa: S608
        start = f"SELECT min(s) FROM (SELECT input_file_block_start() AS s FROM {name})"  # noqa: S608
        other = f"SELECT count(DISTINCT f) FROM (SELECT input_file_name() AS f FROM {name} WHERE k > 2)"  # noqa: S608
        df = spark.sql(f"SELECT ({files}) AS a, ({files}) AS b, ({start}) AS c, ({start}) AS d, ({other}) AS e")
        [(a, b, c, d, e)] = df.collect()
        assert a == b
        assert a >= len(inserts)
        assert c == d == 0
        assert e < a
        if not is_jvm_spark():
            assert scalar_subquery_slots(df) == [3]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.fixture(scope="module")
def self_join_table(spark, tmp_path_factory):
    name = f"aggregate_reuse_sj_{uuid.uuid4().hex[:8]}"
    location = escape_sql_string_literal(str(tmp_path_factory.mktemp("aggregate_reuse_sj") / name))
    spark.sql(f"CREATE TABLE {name} (k INT, v INT) USING parquet LOCATION '{location}'")
    spark.sql(f"INSERT INTO {name} VALUES (1, 10), (2, 20), (3, 5)")  # noqa: S608
    yield name
    spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize(
    ("query", "expected", "slots"),
    [
        pytest.param(
            "SELECT (SELECT max(x.k) FROM {t} x JOIN {t} y ON x.k = y.k + 1) AS a, "
            "(SELECT max(y.k) FROM {t} x JOIN {t} y ON x.k = y.k + 1) AS b",
            (3, 2),
            [2],
            id="other-side",
        ),
        pytest.param(
            "SELECT (SELECT max(x.k) FROM {t} x JOIN {t} y ON x.k = y.k + 1) AS a, "
            "(SELECT max(x.k) FROM {t} y JOIN {t} x ON y.k = x.k + 1) AS b",
            (3, 2),
            [2],
            id="swapped-alias-names",
        ),
        pytest.param(
            "SELECT (SELECT count(y.v) FROM {t} x LEFT JOIN {t} y ON x.k = y.k + 1) AS a, "
            "(SELECT count(x.v) FROM {t} x LEFT JOIN {t} y ON x.k = y.k + 1) AS b",
            (2, 3),
            [2],
            id="left-join-side",
        ),
        pytest.param(
            "SELECT (SELECT max(x.v) FROM {t} x JOIN {t} y ON x.k = y.k + 1) AS a, "
            "(SELECT max(p.v) FROM {t} p JOIN {t} q ON p.k = q.k + 1) AS b",
            (20, 20),
            [1],
            id="renamed-aliases",
        ),
        pytest.param(
            "SELECT (SELECT max(x.v - y.v) FROM {t} x JOIN {t} y ON x.k = y.k + 1) AS a, "
            "(SELECT max(y.v - x.v) FROM {t} x JOIN {t} y ON x.k = y.k + 1) AS b",
            (10, 15),
            [2],
            id="both-sides",
        ),
        pytest.param(
            "SELECT (SELECT max(s.k) FROM (SELECT k FROM {t} WHERE v > 6) s) AS a, "
            "(SELECT max(s.k) FROM (SELECT k FROM {t} WHERE v > 15) s) AS b",
            (2, 2),
            [2],
            id="same-alias-other-body",
        ),
    ],
)
def test_self_join_sides_are_told_apart(spark, self_join_table, query, expected, slots):
    """Aliases are ignored when subqueries are compared, but the two sides of a self-join are not."""
    df = spark.sql(query.format(t=self_join_table))
    assert tuple(df.collect()[0]) == expected
    if not is_jvm_spark():
        assert scalar_subquery_slots(df) == slots


def test_differently_named_references_share_one_result(spark, tables):
    t = tables["t"]
    catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
    df = spark.sql(
        f"SELECT (SELECT max(x) FROM {t}) AS a, (SELECT max(T.x) FROM {t} AS T) AS b, "  # noqa: S608
        f"(SELECT max(v.x) FROM {t} v) AS c, (SELECT max(x) FROM default.{t}) AS d, "
        f"(SELECT max(x) FROM {catalog}.default.{t}) AS e"
    )
    assert [tuple(row) for row in df.collect()] == [(20, 20, 20, 20, 20)]
    if not is_jvm_spark():
        assert scalar_subquery_slots(df) == [1]


def test_repeated_aggregates_next_to_input_file_name_share_one_result(spark, tables):
    t = tables["t"]
    df = spark.sql(
        f"SELECT input_file_name() <> '' AS f, (SELECT max(x) FROM {t}) AS a, (SELECT max(x) FROM {t}) AS b "  # noqa: S608
        f"FROM {t}"
    )
    assert [tuple(row) for row in df.collect()] == [(True, 20, 20)] * 4
    if not is_jvm_spark():
        assert scalar_subquery_slots(df) == [1]


def ntz(*args):
    """A datetime without a time zone (for timestamp columns without a time zone)."""
    return datetime.datetime(*args, tzinfo=datetime.timezone.utc).replace(tzinfo=None)


# Each column has distinct minimum and maximum values, so that a wrongly shared body shows.
ARROW_COLUMNS = [
    ("i8", pa.int8(), [1, -3, None]),
    ("i16", pa.int16(), [1, -3, None]),
    ("i32", pa.int32(), [1, -3, None]),
    ("i64", pa.int64(), [1, -3, None]),
    ("u8", pa.uint8(), [1, 200, None]),
    ("u16", pa.uint16(), [1, 60000, None]),
    ("u32", pa.uint32(), [1, 4000000000, None]),
    ("u64", pa.uint64(), [1, 2**63 + 5, None]),
    ("f16", pa.float16(), [1.5, -2.0, None]),
    ("f32", pa.float32(), [1.5, -0.0, None]),
    ("f64", pa.float64(), [1.5, -2.5, None]),
    ("dec", pa.decimal128(10, 2), [decimal.Decimal("1.25"), decimal.Decimal("-3.50"), None]),
    ("dec256", pa.decimal256(40, 3), [decimal.Decimal("1.250"), decimal.Decimal("-3.500"), None]),
    ("str", pa.string(), ["b", "a", None]),
    ("large_str", pa.large_string(), ["b", "a", None]),
    ("bin", pa.binary(), [b"b", b"a", None]),
    ("large_bin", pa.large_binary(), [b"b", b"a", None]),
    ("fixed_bin", pa.binary(2), [b"bb", b"aa", None]),
    ("bool", pa.bool_(), [True, False, None]),
    ("date32", pa.date32(), [datetime.date(2000, 1, 8), datetime.date(2000, 1, 1), None]),
    ("date64", pa.date64(), [datetime.date(2000, 1, 8), datetime.date(2000, 1, 1), None]),
    ("ts_s", pa.timestamp("s"), [ntz(2000, 1, 8), ntz(2000, 1, 1), None]),
    ("ts_ms", pa.timestamp("ms"), [ntz(2000, 1, 8), ntz(2000, 1, 1), None]),
    ("ts_us", pa.timestamp("us"), [ntz(2000, 1, 8), ntz(2000, 1, 1), None]),
    ("ts_ns", pa.timestamp("ns"), [ntz(2000, 1, 8), ntz(2000, 1, 1), None]),
    (
        "ts_us_utc",
        pa.timestamp("us", tz="UTC"),
        [
            datetime.datetime(2000, 1, 8, tzinfo=datetime.timezone.utc),
            datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc),
            None,
        ],
    ),
    ("time32_s", pa.time32("s"), [datetime.time(1, 2, 3), datetime.time(0, 0, 1), None]),
    ("time64_us", pa.time64("us"), [datetime.time(1, 2, 3), datetime.time(0, 0, 1), None]),
    ("time64_ns", pa.time64("ns"), [datetime.time(1, 2, 3), datetime.time(0, 0, 1), None]),
    ("dur_us", pa.duration("us"), [datetime.timedelta(seconds=5), datetime.timedelta(seconds=1), None]),
    ("dur_ns", pa.duration("ns"), [datetime.timedelta(seconds=5), datetime.timedelta(seconds=1), None]),
    ("list", pa.list_(pa.int32()), [[2, 1], [1, 5], None]),
    ("large_list", pa.large_list(pa.int32()), [[2, 1], [1, 5], None]),
    ("fixed_list", pa.list_(pa.int32(), 2), [[2, 1], [1, 5], None]),
    ("struct", pa.struct([("a", pa.int32()), ("b", pa.string())]), [{"a": 2, "b": "x"}, {"a": 1, "b": "y"}, None]),
    ("dict", pa.dictionary(pa.int32(), pa.string()), ["b", "a", None]),
]
# Columns that support count but not min/max.
ARROW_COUNT_ONLY_COLUMNS = [
    ("map", pa.map_(pa.string(), pa.int32()), [[("k", 1)], [("k", 2)], None]),
    ("null", pa.null(), [None, None, None]),
]


@pytest.fixture(scope="module")
def arrow_table(spark, tmp_path_factory):
    name = f"aggregate_reuse_arrow_{uuid.uuid4().hex[:8]}"
    location = tmp_path_factory.mktemp("aggregate_reuse_arrow") / name
    location.mkdir()
    columns = ARROW_COLUMNS + ARROW_COUNT_ONLY_COLUMNS
    table = pa.table({c: pa.array(values, type=t) for c, t, values in columns})
    pq.write_table(table, location / "part-0.parquet")
    spark.sql(f"CREATE TABLE {name} USING parquet LOCATION '{escape_sql_string_literal(str(location))}'")
    yield name
    spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.spark(skip_if_jvm=True, reason="Arrow types that JVM Spark does not support")
@pytest.mark.parametrize(
    "column",
    [
        # TODO: Float16 scalar subqueries panic in local-cluster mode, with or without sharing.
        pytest.param(
            c,
            marks=pytest.mark.skipif(
                c == "f16" and os.environ.get("SAIL_MODE") == "local-cluster",
                reason="Float16 scalar subqueries fail in local-cluster mode",
            ),
        )
        for c, _, _ in ARROW_COLUMNS
    ],
)
def test_repeated_aggregate_scalar_subqueries_over_arrow_types(spark, arrow_table, column):
    # The results are compared in the query, since the client cannot decode every Arrow type.
    df = spark.sql(
        f"SELECT (SELECT max({column}) FROM {arrow_table}) <=> (SELECT max({column}) FROM {arrow_table}) AS same, "  # noqa: S608
        f"(SELECT max({column}) FROM {arrow_table}) <=> (SELECT min({column}) FROM {arrow_table}) AS other, "
        f"(SELECT count({column}) FROM {arrow_table}) AS n"
    )
    assert [tuple(row) for row in df.collect()] == [(True, False, 2)]
    assert sorted(scalar_subquery_slots(df)) == [3]


@pytest.mark.spark(skip_if_jvm=True, reason="Arrow types that JVM Spark does not support")
@pytest.mark.parametrize("column", [c for c, _, _ in ARROW_COUNT_ONLY_COLUMNS])
def test_repeated_count_scalar_subqueries_over_arrow_types(spark, arrow_table, column):
    df = spark.sql(
        f"SELECT (SELECT count({column}) FROM {arrow_table}) AS a, (SELECT count({column}) FROM {arrow_table}) AS b"  # noqa: S608
    )
    expected = 0 if column == "null" else 2
    assert [tuple(row) for row in df.collect()] == [(expected, expected)]
    assert scalar_subquery_slots(df) == [1]
