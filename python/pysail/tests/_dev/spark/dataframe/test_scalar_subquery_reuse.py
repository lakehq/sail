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
            [2],
            id="monotonically-increasing-id",
        ),
    ],
)
def test_repeated_scalar_subqueries_over_extension_nodes(spark, tables, query, expected, slots):
    check(spark.sql(query.format(**tables)), expected, slots)


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
