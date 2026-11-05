"""Spark 4.2 InSubquery.nullable: any nullable left or original RHS key.

Fixed rows/schemas were checked on Spark 4.2.0 (32f7299601), UTC, ANSI=true,
legacy.inSubqueryNullability=false. Single struct keys differ from tuple keys.
"""

# SQL fragments come only from the fixed parametrizations and helpers in this test module.
# ruff: noqa: S608

import pytest
from pyspark.sql.types import BooleanType, IntegerType, LongType, StructField, StructType


def assert_result(spark, sql, rows, fields):
    frame = spark.sql(sql)
    assert frame.schema == StructType([StructField(name, dtype, nullable) for name, dtype, nullable in fields])
    assert sorted(tuple(row) for row in frame.collect()) == rows


@pytest.fixture
def membership_views(spark):
    def create(left_nullable, right_nullable, *, empty=False, actual_nulls=False):
        width = len(left_nullable)
        keys = ["k", "v"][:width]
        left = [(0, 1, 10), (1, 2, 20), (2, 3, 30), (3, 4, 40)]
        right = [(1, 10), (1, 10), (2, 21)]
        if actual_nulls:
            left += [(4, None, 10), (5, 1, None), (6, None, None)]
            right += [(None, 20), (2, None)]
        left_schema = StructType(
            [StructField("id", IntegerType(), False)]
            + [StructField(key, IntegerType(), nullable) for key, nullable in zip(keys, left_nullable, strict=True)]
        )
        right_schema = StructType(
            [StructField(key, IntegerType(), nullable) for key, nullable in zip(keys, right_nullable, strict=True)]
        )
        spark.createDataFrame([row[: width + 1] for row in left], left_schema).createOrReplaceTempView("in_outer")
        spark.createDataFrame([] if empty else [row[:width] for row in right], right_schema).createOrReplaceTempView(
            "in_inner"
        )

    try:
        yield create
    finally:
        spark.catalog.dropTempView("in_outer")
        spark.catalog.dropTempView("in_inner")


SCHEMA_CASES = [
    pytest.param((False,), (False,), "schema", id="scalar-nonnull"),
    pytest.param((True,), (False,), "schema", id="scalar-left-nullable"),
    pytest.param((False,), (True,), "schema", id="scalar-rhs-nullable"),
    pytest.param((True,), (True,), "schema", id="scalar-both-nullable"),
    *[
        pytest.param(
            (bool(mask & 1), bool(mask & 2)),
            (bool(mask & 4), bool(mask & 8)),
            "schema",
            id=f"tuple-nullability-{mask:04b}",
        )
        for mask in range(16)
    ],
]
EDGE_CASES = [
    pytest.param((True,) * width, (edge != "empty",) * width, edge, id=f"{kind}-{edge}")
    for width, kind in [(1, "scalar"), (2, "tuple")]
    for edge in ["empty", "actual-null", "correlated", "reuse"]
]


@pytest.mark.parametrize(("left_nullable", "right_nullable", "edge"), SCHEMA_CASES + EDGE_CASES)
def test_in_subquery_schema_and_values(spark, membership_views, left_nullable, right_nullable, edge):
    membership_views(left_nullable, right_nullable, empty=edge == "empty", actual_nulls=edge not in ("schema", "empty"))
    scalar = len(left_nullable) == 1
    lhs, rhs = ("o.k", "r.k") if scalar else ("(o.k, o.v)", "r.k, r.v")
    condition = "r.k = o.k" if scalar else "r.k = o.k AND r.v = o.v"
    correlation = " WHERE r.k <= o.id + 1" if edge == "correlated" else ""
    exists_correlation = " AND r.k <= o.id + 1" if edge == "correlated" else ""
    wrappers = (
        """
        , included AS reused_in, excluded AS reused_not_in,
          NOT included AS negated_in, NOT excluded AS negated_not_in,
          included IN (SELECT true) AS nested_in, excluded IN (SELECT true) AS nested_not_in
    """
        if edge == "schema"
        else ""
    )
    sql = f"""
        SELECT id,
          {lhs} IN (SELECT {rhs} FROM in_inner r{correlation}) AS included,
          {lhs} NOT IN (SELECT {rhs} FROM in_inner r{correlation}) AS excluded,
          EXISTS (SELECT 1 FROM in_inner r WHERE {condition}{exists_correlation}) AS present,
          NOT EXISTS (SELECT 1 FROM in_inner r WHERE {condition}{exists_correlation}) AS absent
          {", included AS reused" if edge == "reuse" else ""}
          {wrappers}
        FROM in_outer o
    """
    if edge == "empty":
        included, excluded = [False] * 4, [True] * 4
    elif edge == "schema":
        included = [True, scalar, False, False]
        excluded = [False, not scalar, True, True]
    elif scalar:
        included = [True, True, False, False, False, True, False]
        excluded = [False, False, edge == "correlated", edge == "correlated", False, False, False]
    else:
        included = [True, False, False, False, False, False, False]
        excluded = [False, False, True, True, False, False, False]
    rows = [
        (i, inc, exc, inc, not inc) + ((inc,) if edge == "reuse" else ())
        for i, (inc, exc) in enumerate(zip(included, excluded, strict=True))
    ]
    nullable = any(left_nullable) or any(right_nullable)
    fields = [("id", IntegerType(), False)] + [
        (name, BooleanType(), value)
        for name, value in [("included", nullable), ("excluded", nullable), ("present", False), ("absent", False)]
    ]
    if edge == "reuse":
        fields.append(("reused", BooleanType(), nullable))
    if edge == "schema":
        rows = [(*row, row[1], row[2], not row[1], not row[2], row[1], row[2]) for row in rows]
        fields += [
            (name, BooleanType(), nullable)
            for name in ["reused_in", "reused_not_in", "negated_in", "negated_not_in", "nested_in", "nested_not_in"]
        ]
    assert_result(spark, sql, rows, fields)


@pytest.fixture
def original_membership_views(spark):
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW t AS SELECT * FROM VALUES
          (1L, 'a', 10, 100), (2L, 'a', 20, 200), (3L, 'b', CAST(NULL AS INT), 300),
          (4L, 'b', 40, CAST(NULL AS INT)), (5L, CAST(NULL AS STRING), 50, 500),
          (6L, 'a', 10, 600), (7L, 'c', 70, 700) t(id, g, v, w)
    """).collect()
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW u AS SELECT * FROM VALUES
          (1L, 'a', 100), (2L, 'a', 200), (4L, 'b', 400), (8L, 'z', 800),
          (CAST(NULL AS BIGINT), 'n', 900) u(id, g, z)
    """).collect()
    try:
        yield
    finally:
        spark.catalog.dropTempView("t")
        spark.catalog.dropTempView("u")


@pytest.mark.parametrize("case", ["tuple-not-in", "alias-reuse", "nullable-tuples", "empty-scalar", "empty-tuple"])
def test_projected_membership_reference_queries(spark, original_membership_views, case):  # noqa: ARG001
    if case == "tuple-not-in":
        sql = "SELECT id, (id, g) NOT IN (SELECT id, g FROM u) AS b FROM t"
        rows = list(enumerate([False, False, True, False, False, True, True], start=1))
        fields = [("id", LongType(), False), ("b", BooleanType(), True)]
    elif case == "alias-reuse":
        sql = """
            SELECT id, EXISTS(SELECT 1 FROM VALUES (1L), (2L) u(k) WHERE u.k = t.id) AS x, x AS y
            FROM range(3) t
        """
        rows = [(0, False, False), (1, True, True), (2, True, True)]
        fields = [("id", LongType(), False), ("x", BooleanType(), False), ("y", BooleanType(), False)]
    elif case == "nullable-tuples":
        sql = """
            WITH r AS (
              SELECT * FROM VALUES (1, 10), (1, 10), (CAST(NULL AS INT), 20),
                                   (2, CAST(NULL AS INT)) r(k, v)
            )
            SELECT id,
              (k, v) IN (SELECT k, v FROM r) AS included,
              (k, v) NOT IN (SELECT k, v FROM r) AS excluded,
              EXISTS (SELECT 1 FROM r WHERE r.k = o.k AND r.v = o.v) AS present,
              NOT EXISTS (SELECT 1 FROM r WHERE r.k = o.k AND r.v = o.v) AS absent
            FROM VALUES (0, CAST(NULL AS INT), CAST(NULL AS INT)),
                        (1, CAST(NULL AS INT), 10), (2, 1, CAST(NULL AS INT)),
                        (3, 1, 10), (4, 1, 20), (5, 2, 10), (6, 2, 20),
                        (7, 3, 30), (8, 1, 10) o(id, k, v)
        """
        excluded_id = 7
        rows = [(i, i in (3, 8), i == excluded_id, i in (3, 8), i not in (3, 8)) for i in range(9)]
        fields = None
    else:
        lhs, rhs = ("k", "k") if case == "empty-scalar" else ("(k, v)", "k, v")
        sql = f"""
            SELECT id,
              {lhs} IN (SELECT {rhs} FROM VALUES (1, 10) r(k, v) WHERE false) AS included,
              {lhs} NOT IN (SELECT {rhs} FROM VALUES (1, 10) r(k, v) WHERE false) AS excluded,
              EXISTS (SELECT 1 FROM VALUES (1, 10) r(k, v) WHERE r.k = o.k AND false) AS present,
              NOT EXISTS (SELECT 1 FROM VALUES (1, 10) r(k, v) WHERE r.k = o.k AND false) AS absent
            FROM VALUES (0, CAST(NULL AS INT), CAST(NULL AS INT)),
                        (1, 1, CAST(NULL AS INT)), (2, CAST(NULL AS INT), 10), (3, 1, 10) o(id, k, v)
        """
        rows = [(i, False, True, False, True) for i in range(4)]
        fields = None
    if fields is None:
        fields = [("id", IntegerType(), False)] + [
            (name, BooleanType(), nullable)
            for name, nullable in [("included", True), ("excluded", True), ("present", False), ("absent", False)]
        ]
    assert_result(spark, sql, rows, fields)


@pytest.mark.parametrize("operator", ["IS NULL", "<=> false"])
def test_tuple_in_null_sensitive_filter(spark, membership_views, operator):
    membership_views((True, True), (True, True), actual_nulls=True)
    expected = [] if operator == "IS NULL" else [(i,) for i in range(1, 7)]
    assert_result(
        spark,
        f"SELECT id FROM in_outer o WHERE ((o.k, o.v) IN (SELECT r.k, r.v FROM in_inner r)) {operator}",
        expected,
        [("id", IntegerType(), False)],
    )


@pytest.mark.parametrize("operator", ["IS NULL", "<=> false"])
def test_scalar_in_null_sensitive_filter_spark_contract(spark, membership_views, operator):
    # Spark lowers these value-observing predicates to non-null existence marks.
    membership_views((True, True), (True, True), actual_nulls=True)
    expected = [] if operator == "IS NULL" else [(2,), (3,), (4,), (6,)]
    assert_result(
        spark,
        f"SELECT id FROM in_outer o WHERE (o.k IN (SELECT r.k FROM in_inner r)) {operator}",
        expected,
        [("id", IntegerType(), False)],
    )


@pytest.mark.parametrize(
    ("predicate", "ids"),
    [
        pytest.param("{included}", [0, 1, 5], id="direct-in"),
        pytest.param("{excluded}", [], id="direct-not-in"),
        pytest.param("NOT {included}", [], id="direct-negation"),
        pytest.param("id = 2 OR {included}", [0, 1, 2, 5], id="or-in"),
        pytest.param("id = 2 OR {excluded}", [2], id="or-not-in"),
        pytest.param("{included} IS NOT NULL", list(range(7)), id="is-not-null"),
        pytest.param("false <=> {included}", [2, 3, 4, 6], id="reversed-null-safe-comparison"),
        pytest.param("{included} <=> CAST(NULL AS BOOLEAN)", [], id="null-safe-null"),
        pytest.param("{included} IS FALSE", [2, 3, 4, 6], id="is-false"),
        pytest.param("{included} = false", [], id="ordinary-equality"),
        pytest.param("{included} != true", [], id="ordinary-inequality"),
        pytest.param("{excluded} = false", [0, 1, 5], id="ordinary-negated-equality"),
        pytest.param("{excluded} IS NULL", [], id="not-in-is-null"),
        pytest.param("{excluded} <=> false", list(range(7)), id="not-in-null-safe-false"),
        pytest.param("coalesce({included}, true)", [0, 1, 5], id="coalesce"),
        pytest.param("NOT coalesce({included}, false)", [2, 3, 4, 6], id="negated-coalesce"),
        pytest.param("CASE WHEN id >= 0 THEN {included} ELSE false END <=> false", [2, 3, 4, 6], id="case"),
        pytest.param("id > 0 AND {included} <=> false", [2, 3, 4, 6], id="and-null-sensitive"),
        pytest.param("id = 0 OR {included} <=> false", [0, 2, 3, 4, 6], id="or-null-sensitive"),
        pytest.param(
            "(o.k IN (SELECT r.k FROM in_inner r WHERE r.v <= o.v)) <=> false",
            [1, 2, 3, 4, 5, 6],
            id="correlated-in",
        ),
        pytest.param(
            "(o.k NOT IN (SELECT r.k FROM in_inner r WHERE r.v <= o.v)) <=> false",
            [0, 1, 2, 3, 4],
            id="correlated-not-in",
        ),
        pytest.param("(o.k IN (SELECT r.k FROM in_inner r WHERE false)) <=> false", list(range(7)), id="empty-in"),
        pytest.param("(o.k NOT IN (SELECT r.k FROM in_inner r WHERE false)) <=> false", [], id="empty-not-in"),
        pytest.param(
            "(o.k IN (SELECT count(*) FROM in_inner r WHERE false)) <=> false",
            list(range(7)),
            id="empty-global-aggregate",
        ),
        pytest.param(
            "(o.k IN (SELECT count(*) FROM in_inner r WHERE r.v = o.v)) <=> false",
            list(range(7)),
            id="correlated-aggregate",
        ),
        pytest.param(
            "(o.k IN (SELECT sum(r.k) FROM in_inner r WHERE r.v = o.v HAVING count(*) > 0)) <=> false",
            list(range(7)),
            id="correlated-having",
        ),
        pytest.param("(o.k > 0) IS NULL", [4, 6], id="nullable-boolean-control"),
        pytest.param("({included} = false) <=> false", list(range(7)), id="nested-equality"),
        pytest.param("({excluded} != true) <=> false", [2, 3, 4, 6], id="nested-inequality"),
        pytest.param("(NOT {included}) <=> false", list(range(7)), id="value-negation"),
        pytest.param("(NOT NOT {included}) <=> false", [2, 3, 4, 6], id="double-negation"),
        pytest.param("(NOT ({included} OR id = 2)) <=> false", list(range(7)), id="negated-or"),
        pytest.param("(NOT ({included} AND id = 0)) <=> false", [0], id="negated-and"),
        pytest.param("{included} <=> (o.k > 0)", [0, 1, 5], id="nullable-boolean-comparison"),
        pytest.param("({included} IN (SELECT false)) <=> false", [0, 1, 5], id="nested-in"),
    ],
)
def test_scalar_in_filter_contexts(spark, membership_views, predicate, ids):
    # RewritePredicateSubquery runs after Boolean simplification in Spark 4.2:
    # ordinary equality to false becomes NOT IN, while nullable <=> false does not.
    membership_views((True, True), (True, True), actual_nulls=True)
    predicate = predicate.format(
        included="(o.k IN (SELECT r.k FROM in_inner r))",
        excluded="(o.k NOT IN (SELECT r.k FROM in_inner r))",
    )
    assert_result(
        spark, f"SELECT id FROM in_outer o WHERE {predicate}", [(i,) for i in ids], [("id", IntegerType(), False)]
    )


def test_in_single_nonnull_struct_key(spark, membership_views):
    membership_views((True, True), (True, True), actual_nulls=True)
    assert_result(
        spark,
        """SELECT id, named_struct('k',k,'v',v) IN
           (SELECT named_struct('k',k,'v',v) FROM in_inner) AS included FROM in_outer""",
        [(i, i == 0) for i in range(7)],
        [("id", IntegerType(), False), ("included", BooleanType(), False)],
    )


@pytest.mark.parametrize("reuse", [False, True], ids=["nested-expression", "lateral-aliases"])
def test_nested_tuple_membership_nullability(spark, membership_views, reuse):
    membership_views((False, True), (False, True))
    predicate = "(o.k, o.v) IN (SELECT r.k, r.v FROM in_inner r)"
    if reuse:
        sql = f"""SELECT id, {predicate} AS included,
            CASE WHEN id >= 0 THEN included ELSE false END AS nested,
            nested AS reused, nested IN (SELECT true) AS outer_in FROM in_outer o"""
        rows = [(i, i == 0, i == 0, i == 0, i == 0) for i in range(4)]
        names = ["included", "nested", "reused", "outer_in"]
    else:
        sql = (
            f"SELECT id, (CASE WHEN id >= 0 THEN {predicate} ELSE false END) IN (SELECT true) AS nested FROM in_outer o"
        )
        rows = [(i, i == 0) for i in range(4)]
        names = ["nested"]
    assert_result(
        spark,
        sql,
        rows,
        [("id", IntegerType(), False)] + [(name, BooleanType(), True) for name in names],
    )
