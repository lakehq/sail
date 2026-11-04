"""Correlated grouping must equal literal-bound evaluation of the same inner SQL.

DataFusion 1635ed9 grouping.slt preserves empty grand totals, including duplicate
empty sets. Spark 4.2 SPARK-29701 intentionally differs for mixed empty sets.
The literal-bound queries never contain a subquery or use the domain rewrite.
FIXME: Align mixed/duplicate empty-input grouping with Spark in both correlated
and literal-bound paths before changing the expectations below (for example,
unmatched mixed-set generators currently return 1 rather than Spark's NULL).
"""

import pytest
from pyspark.errors import UnsupportedOperationException
from pyspark.sql.types import DoubleType, LongType

from pysail.testing.spark.session import spark_connect_server


@pytest.fixture(scope="module", params=["local", "local-cluster"])
def remote(request):
    with spark_connect_server(envs={"SAIL_MODE": request.param, "SAIL_CLUSTER__TASK_MAX_ATTEMPTS": "1"}) as server:
        yield server.remote


@pytest.fixture(scope="module")
def grouped_tables(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("grouped_scalar_domain")
    inner = spark.createDataFrame([("0", 1), ("0", 2), ("00", None), ("1", None), (None, 5)], "k string, v long")
    bindings = [(0, "0"), (0, "00"), (1, "1"), (2, "2"), (0, "0"), (None, None), (None, "0"), (0, None)]
    outer = spark.createDataFrame(
        [(index, *binding) for index, binding in enumerate(bindings)], "row_id int, id long, s string"
    )
    for name, frame in [("grouped_domain_inner", inner), ("grouped_domain_outer", outer)]:
        path = str(root / name)
        frame.write.parquet(path)
        spark.read.parquet(path).createOrReplaceTempView(name)
    try:
        yield bindings
    finally:
        spark.catalog.dropTempView("grouped_domain_inner")
        spark.catalog.dropTempView("grouped_domain_outer")


def literal(value, data_type):
    text = "NULL" if value is None else repr(value)
    return f"CAST({text} AS {data_type})"


@pytest.mark.parametrize(
    ("grouping", "projection", "having", "consumer", "expected"),
    [
        pytest.param(grouping, "explode(array(c, c + 1))", "true", "max(z)", [3, 2, 2, 1, 3, 1, 1, 1], id=name)
        for name, grouping in [("sets", "GROUPING SETS ((k), ())"), ("rollup", "ROLLUP(k)"), ("cube", "CUBE(k)")]
    ]
    + [
        pytest.param(
            "GROUPING SETS ((k), ())",
            "explode(array(c, c + 1))",
            "count(*) < 0",
            "max(z)",
            [None] * 8,
            id="having-reject-empty",
        ),
        pytest.param(
            "GROUPING SETS ((k), ())",
            "explode(array(c, c + 1))",
            "count(*) = 0",
            "max(z)",
            [None, None, None, 1, None, 1, 1, 1],
            id="having-accept-empty",
        ),
        pytest.param(
            "GROUPING SETS ((k), ())",
            "explode(array(c, c + 1))",
            "grouping(k) = 1",
            "max(z)",
            [3, 2, 2, 1, 3, 1, 1, 1],
            id="having-grouping-bit",
        ),
        pytest.param(
            "GROUPING SETS ((k), ())",
            "explode(CAST(array() AS ARRAY<BIGINT>))",
            "true",
            "max(z)",
            [None] * 8,
            id="empty-generator",
        ),
        pytest.param(
            "GROUPING SETS ((k), ())",
            "explode(array(CAST(NULL AS BIGINT)))",
            "true",
            "max(z)",
            [None] * 8,
            id="null-element",
        ),
        pytest.param(
            "GROUPING SETS ((k), ())",
            "explode(CAST(NULL AS ARRAY<BIGINT>))",
            "true",
            "max(z)",
            [None] * 8,
            id="null-array",
        ),
        pytest.param(
            "GROUPING SETS ((k), ())",
            "total",
            "true",
            "max(z)",
            [3, None, None, None, 3, None, None, None],
            id="legitimate-null-sum",
        ),
        pytest.param(
            "GROUPING SETS ((k), ())",
            "largest",
            "true",
            "sum(z)",
            [4, None, None, None, 4, None, None, None],
            id="legitimate-null-max",
        ),
        pytest.param(
            "GROUPING SETS ((k), (k))",
            "explode(array(c, c + 1))",
            "true",
            "max(z)",
            [3, 2, 2, None, 3, None, None, None],
            id="duplicate-nonempty",
        ),
        pytest.param("GROUPING SETS ((k), (), ())", "gid", "true", "sum(z)", [2] * 8, id="duplicate-empty-masked-id"),
        pytest.param(
            "GROUPING SETS ((k), ())",
            "CASE WHEN k IS NULL THEN c + 1 ELSE 100L END",
            "true",
            "max(z)",
            [100, 100, 100, 1, 100, 1, 1, 1],
            id="omitted-user-key",
        ),
    ],
)
def test_grouped_scalar_matches_bound_scan(spark, grouped_tables, grouping, projection, having, consumer, expected):
    inner = f"""SELECT {consumer} FROM (
        SELECT {projection} AS z FROM (
            SELECT k, count(*) AS c, sum(v) AS total, max(v) AS largest, grouping_id(k) AS gid
            FROM grouped_domain_inner i
            WHERE CAST(i.k AS BIGINT) = o.id AND i.k = o.s
            GROUP BY {grouping} HAVING {having}
        ) grouped
    ) generated"""
    correlated = spark.sql(f"SELECT row_id, ({inner}) AS n FROM grouped_domain_outer o ORDER BY row_id")
    assert correlated.schema["n"].dataType == LongType()
    assert correlated.schema["n"].nullable
    assert [row.n for row in correlated.collect()] == expected
    for index, (identifier, string) in enumerate(grouped_tables):
        bound = inner.replace("o.id", literal(identifier, "BIGINT")).replace("o.s", literal(string, "STRING"))
        assert spark.sql(bound).collect()[0][0] == expected[index]
    assert spark.sql(f"SELECT ({inner}) AS n FROM grouped_domain_outer o WHERE row_id < 0").collect() == []


@pytest.mark.parametrize(
    ("sets", "value", "matched", "missing"),
    [
        ("((k), ())", "grouping_id(k) * 10 + grouping(k) + 1", 13, 12),
        ("((k), (), ())", "grouping_id(k) * 10 + grouping(k) + 1", 25, 24),
        ("((k), (k), (), ())", "grouping_id(k) * 10 + grouping(k) + 1", 26, 24),
        ("((k), (k))", "grouping_id(k) * 10 + grouping(k) + 1", 2, None),
        ("((), ())", "grouping_id() + 1", 2, 2),
    ],
    ids=["mixed", "duplicate-empty", "mixed-duplicates", "duplicate-nonempty", "all-empty"],
)
def test_grouping_ids_preserve_occurrences(spark, grouped_tables, sets, value, matched, missing):
    inner = f"""SELECT sum(z) FROM (
        SELECT {value} z FROM grouped_domain_inner i
        WHERE CAST(i.k AS BIGINT) = o.id AND i.k = o.s
        GROUP BY GROUPING SETS {sets}
    ) groups"""
    expected = [matched, matched, matched, missing, matched, missing, missing, missing]
    result = spark.sql(f"SELECT row_id, ({inner}) n FROM grouped_domain_outer o ORDER BY row_id")
    assert result.schema["n"].dataType == LongType()
    assert [row.n for row in result.collect()] == expected
    for index, (identifier, string) in enumerate(grouped_tables):
        bound = inner.replace("o.id", literal(identifier, "BIGINT")).replace("o.s", literal(string, "STRING"))
        assert spark.sql(bound).collect()[0][0] == expected[index]


@pytest.mark.parametrize("width", [8, 16, 32, 63])
def test_duplicate_grouping_widths(spark, grouped_tables, width):
    columns = [f"c{index}" for index in range(width)]
    full = ",".join(columns)
    subtotal = ",".join(columns[:-1])
    projection = ",".join(f"v AS {name}" for name in columns)
    inner = f"""SELECT sum(z) FROM (
        SELECT grouping_id({full}) * 10 + grouping({columns[-1]}) + 1 AS z
        FROM (SELECT k,{projection} FROM grouped_domain_inner) i
        WHERE i.k = o.s AND CAST(i.k AS BIGINT) = o.id
        GROUP BY GROUPING SETS (({full}), ({full}), ({subtotal}), ({subtotal}))
    ) groups"""
    # Two v groups for '0'; duplicate full rows contribute 4, duplicate
    # subtotals contribute 48. NULL-valued '00'/'1' each have one v group.
    expected = [52, 26, 26, None, 52, None, None, None]
    result = spark.sql(f"SELECT row_id, ({inner}) n FROM grouped_domain_outer o ORDER BY row_id")
    assert [row.n for row in result.collect()] == expected
    for index, (identifier, string) in enumerate(grouped_tables):
        bound = inner.replace("o.id", literal(identifier, "BIGINT")).replace("o.s", literal(string, "STRING"))
        assert spark.sql(bound).collect()[0][0] == expected[index]


@pytest.mark.parametrize("data_type", ["BIGINT", "FLOAT"])
@pytest.mark.parametrize(
    ("consumer", "matched", "missing", "result_type"),
    [("min(z)", 1, None, LongType()), ("avg(z)", 1.5, None, DoubleType()), ("count(z)", 4, 0, LongType())],
)
def test_masked_grouping_having_keeps_native_terminals(
    spark, grouped_tables, data_type, consumer, matched, missing, result_type
):
    inner = f"""SELECT {consumer} FROM (
        SELECT k AS z FROM VALUES (1L), (2L) AS i(k)
        GROUP BY GROUPING SETS ((k), (k)) HAVING grouping_id(k) = o.g
    ) q"""
    # Literal-bound evaluation retains both occurrences. MIN/AVG cannot hide a
    # newly unsupported native path; COUNT also detects leaking the ordinal.
    assert spark.sql(inner.replace("o.g", f"CAST(0 AS {data_type})")).collect()[0][0] == matched
    exact = spark.sql(f"SELECT o.g, ({inner}) AS n FROM VALUES (CAST(0 AS {data_type})) AS o(g)")
    assert [tuple(row) for row in exact.collect()] == [(0, matched)]
    assert exact.schema["n"].dataType == result_type
    scan_inner = inner.replace("VALUES (1L), (2L) AS i(k)", "(SELECT v AS k FROM grouped_domain_inner WHERE k = '0') i")
    result = spark.sql(f"""SELECT row_id, ({scan_inner}) AS n
        FROM (SELECT row_id, CAST(id AS {data_type}) AS g FROM grouped_domain_outer) o ORDER BY row_id""")
    assert [row.n for row in result.collect()] == [
        matched,
        matched,
        missing,
        missing,
        matched,
        missing,
        missing,
        matched,
    ]
    for binding in [0, 1, None]:
        assert spark.sql(scan_inner.replace("o.g", literal(binding, data_type))).collect()[0][0] == (
            matched if binding == 0 else missing
        )


def grouping_chain(depth, empty=False):
    body = "SELECT k FROM (SELECT v AS k FROM grouped_domain_inner WHERE k = '0' AND v = 1) i WHERE i.k = o.id"
    sets = "((k), ())" if empty else "((k), (k))"
    for _ in range(depth):
        body = f"SELECT k, count(*) AS c FROM ({body}) q GROUP BY GROUPING SETS {sets}"
    return f"SELECT sum(c) FROM ({body}) q"


@pytest.mark.timeout(60)
@pytest.mark.parametrize("depth", [2, 4, 8, 12])
def test_native_safe_grouping_chain(spark, grouped_tables, depth):
    inner = grouping_chain(depth)
    result = spark.sql(f"SELECT ({inner}) AS n FROM grouped_domain_outer o WHERE row_id = 2")
    assert result.collect()[0].n == 4
    assert spark.sql(inner.replace("o.id", "CAST(1 AS BIGINT)")).collect()[0][0] == 4


@pytest.mark.timeout(60)
def test_grouped_expansion_budget_declines_without_changing_native_path(spark, grouped_tables):
    inner = grouping_chain(12, empty=True)
    # The original compact plan is valid with a literal binding, but domain
    # expansion would repeatedly duplicate both scans and empty-set completion.
    assert spark.sql(inner.replace("o.id", "CAST(1 AS BIGINT)")).collect()[0][0] == 6
    with pytest.raises(UnsupportedOperationException, match="ScalarSubquery"):
        spark.sql(f"SELECT ({inner}) AS n FROM grouped_domain_outer o WHERE row_id = 2").collect()
    assert (
        spark.sql(f"SELECT ({grouping_chain(12)}) AS n FROM grouped_domain_outer o WHERE row_id = 2").collect()[0].n
        == 4
    )


def test_nested_grouping_bits_and_filter_stage(spark, grouped_tables):
    inner = """SELECT sum(z) FROM (
        SELECT CAST(grouping(c1) AS BIGINT) AS z FROM (
            SELECT k, v AS c1, 1 AS c2, 1 AS c3, 1 AS c4, 1 AS c5, count(*) AS n
            FROM grouped_domain_inner i WHERE i.k = o.s GROUP BY k, v
        ) lower_groups WHERE CAST(k AS BIGINT) = o.id
        GROUP BY GROUPING SETS ((k,c1,c2,c3,c4,c5), (k,c1,c2,c3,c4,c5), (k,c2,c3,c4,c5))
    ) upper_groups"""
    expected = [1, 1, 1, None, 1, None, None, None]
    result = spark.sql(f"SELECT row_id, ({inner}) n FROM grouped_domain_outer o ORDER BY row_id")
    assert [row.n for row in result.collect()] == expected
    for index, (identifier, string) in enumerate(grouped_tables):
        bound = inner.replace("o.id", literal(identifier, "BIGINT")).replace("o.s", literal(string, "STRING"))
        assert spark.sql(bound).collect()[0][0] == expected[index]
