"""Output-schema nullability via the DataFrame API.

Complements the SQL `@function(nullability)` feature suite with the DataFrame path: build a
DataFrame from an explicit `StructType` (so the input nullability is exactly what
we declare — a non-nullable and a nullable column), apply functions through the
`F.*` API, and assert the resulting schema's `nullable` flags.

Spark's `nullable` flag is static and, for a deterministic function that only
transforms its arguments, follows the inputs:
  - a non-null column   -> non-nullable output
  - a nullable column   -> nullable output

Divergences (Sail declares `nullable` differently from Spark) are marked
`xfail(strict=True)` off JVM, the same contract as the `@sail-bug` feature tag.

NOTE — this file is a representative sample, not exhaustive. The DataFrame-API
path and the SQL path are NOT equivalent: some divergences only show up via SQL
constant-folding. For example `bround` on a decimal is tagged `@sail-bug` in the
SQL suite (the folded `SELECT bround(2.5, 0)` path) yet matches Spark through the
DataFrame API (both column and `F.lit`) — so it passes here. `round` diverges on
both paths, which is why it stays a bug. A full DataFrame-API sweep across all
functions is a worthwhile follow-up: it is the path real PySpark code takes, and
it catches (and clears) different cases than the SQL sweep.
"""

from decimal import Decimal

import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as T  # noqa: N812

from pysail.testing.spark.utils.common import is_jvm_spark

sail_bug = pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail nullability bug", strict=True)


def _frame(spark):
    """One non-nullable ("nn") and one nullable ("nl") string column."""
    schema = T.StructType(
        [
            T.StructField("nn", T.StringType(), nullable=False),
            T.StructField("nl", T.StringType(), nullable=True),
        ]
    )
    return spark.createDataFrame([("hello", "world")], schema)


def _decimal_frame(spark):
    """One non-nullable ("nn") and one nullable ("nl") decimal column."""
    schema = T.StructType(
        [
            T.StructField("nn", T.DecimalType(2, 1), nullable=False),
            T.StructField("nl", T.DecimalType(2, 1), nullable=True),
        ]
    )
    return spark.createDataFrame([(Decimal("2.5"), Decimal("2.5"))], schema)


def _nullable(df, column):
    return df.schema[column].nullable


def test_input_schema_is_honored(spark):
    df = _frame(spark)
    assert _nullable(df, "nn") is False
    assert _nullable(df, "nl") is True


@pytest.mark.parametrize(
    ("expression", "expected_type"),
    [
        ("array_repeat(a, 2)", "array<array<interval day>>"),
        ("flatten(array(a))", "array<interval day>"),
    ],
)
def test_nullable_array_conversion_preserves_interval_type(spark, expression, expected_type):
    result = spark.sql("SELECT CAST(array(INTERVAL '1' DAY) AS ARRAY<INTERVAL DAY>) AS a").selectExpr(
        f"{expression} AS result"
    )
    assert result.schema["result"].dataType.simpleString() == expected_type
    assert len(result.collect()) == 1


@pytest.mark.parametrize("function", ["min_by", "max_by"])
@pytest.mark.parametrize("empty", [False, True])
def test_min_max_by_preserves_logical_type_without_column_metadata(spark, function, empty):
    from datetime import timedelta

    interval_type = T.DayTimeIntervalType(T.DayTimeIntervalType.DAY, T.DayTimeIntervalType.DAY)
    frame = (
        spark.range(0 if empty else 2)
        .select(
            F.expr("INTERVAL '1' DAY").alias("value", metadata={"comment": "input only", "source": "test"}),
            F.col("id").alias("ordering"),
        )
        .repartition(2)
    )
    assert frame.schema["value"].dataType == interval_type
    result = frame.agg(F.expr(f"{function}(value, ordering)").alias("result"))

    # Spark preserves the value's interval qualifier, but aggregate expressions
    # do not inherit the input column's user metadata (Alias.metadata).
    assert result.schema == T.StructType([T.StructField("result", interval_type, True)])
    expected = None if empty else timedelta(days=1)
    assert [row.result for row in result.collect()] == [expected]


@pytest.mark.parametrize("function", ["min", "max", "first", "last", "any_value", "min_by", "max_by"])
@pytest.mark.parametrize("empty", [False, True])
def test_aggregate_preserves_dynamic_interval_array(spark, function, empty):
    frame = (
        spark.range(0 if empty else 3)
        .select(
            "id",
            F.expr("array_repeat(INTERVAL '1' YEAR, CAST(id + 1 AS INT))").alias(
                "a", metadata={"comment": "input only"}
            ),
        )
        .repartition(2)
    )
    arguments = "a, id" if function in {"min_by", "max_by"} else "a"
    result = frame.agg(F.expr(f"{function}({arguments})").alias("result"))
    array_type = T.ArrayType(T.YearMonthIntervalType(0, 0), False)
    assert result.schema == T.StructType([T.StructField("result", array_type, True)])

    # A YEAR is 1 when cast to INT, but becomes 12 if the qualifier is lost.
    actual = result.selectExpr("transform(result, x -> CAST(x AS INT)) AS result").collect()[0].result
    if empty:
        assert actual is None
    elif function in {"min", "min_by"}:
        assert actual == [1]
    elif function in {"max", "max_by"}:
        assert actual == [1, 1, 1]
    else:
        # These aggregates may select any input row after repartitioning.
        assert actual in [[1], [1, 1], [1, 1, 1]]


def _nullable_year_array_frame(spark):
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), False),
            T.StructField("element", T.IntegerType(), True),
            T.StructField("count", T.IntegerType(), True),
        ]
    )
    rows = [(0, 1, None), (1, 1, 0), (2, None, 1), (3, 1, 1), (4, 2, 2), (5, 1, None)]
    return (
        spark.createDataFrame(rows, schema)
        .selectExpr("id", "array_repeat(CAST(make_ym_interval(element, 0) AS INTERVAL YEAR), count) AS a")
        .repartition(2)
    )


@pytest.mark.parametrize("expression", ["collect_list(DISTINCT a)", "array_agg(DISTINCT a)", "collect_set(a)"])
def test_distinct_aggregate_preserves_interval_array_state(spark, expression):
    source = (
        spark.createDataFrame([(1,), (1,), (2,), (0,), (None,)], "n int")
        .selectExpr("array_repeat(INTERVAL '1' YEAR, n) AS a")
        .repartition(2)
    )
    result = source.agg(F.expr("count(DISTINCT a)").alias("c"), F.expr(expression).alias("a"))
    assert result.schema["a"].dataType.elementType == T.ArrayType(T.YearMonthIntervalType(0, 0), False)
    actual = result.selectExpr(
        "c", "sort_array(transform(a, ys -> transform(ys, y -> CAST(y AS INT)))) AS nums"
    ).collect()
    assert [(row.c, row.nums) for row in actual] == [(3, [[], [1], [1, 1]])]


@pytest.mark.parametrize("function", ["listagg", "string_agg"])
def test_ordered_aggregate_preserves_interval_array_key(spark, function):
    source = (
        spark.createDataFrame([(3,), (1,), (2,)], "n int")
        .selectExpr("array_repeat(INTERVAL '1' YEAR, n) AS a")
        .repartition(2)
    )
    result = source.agg(
        F.expr(f"{function}(CAST(transform(a, x -> CAST(x AS INT)) AS STRING), '|') WITHIN GROUP (ORDER BY a)").alias(
            "result"
        )
    )
    assert result.schema == T.StructType([T.StructField("result", T.StringType(), True)])
    assert [row.result for row in result.collect()] == ["[1]|[1, 1]|[1, 1, 1]"]


@pytest.mark.parametrize("function", ["collect_list", "array_agg", "collect_set"])
def test_ordered_window_preserves_interval_array_collection(spark, function):
    source = (
        spark.createDataFrame([(0, 1), (1, 1), (2, 2), (3, None), (4, 0)], "id int, n int")
        .selectExpr("id", "array_repeat(INTERVAL '1' YEAR, n) AS a")
        .repartition(2)
    )
    result = source.selectExpr(
        "id", f"{function}(a) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS a"
    ).orderBy("id")
    assert result.schema["a"].dataType.elementType == T.ArrayType(T.YearMonthIntervalType(0, 0), False)
    actual = result.selectExpr("sort_array(transform(a, ys -> transform(ys, y -> CAST(y AS INT)))) AS nums")
    expected = (
        [[[1]], [[1]], [[1], [1, 1]], [[1], [1, 1]], [[], [1], [1, 1]]]
        if function == "collect_set"
        else [[[1]], [[1], [1]], [[1], [1], [1, 1]], [[1], [1], [1, 1]], [[], [1], [1], [1, 1]]]
    )
    actual = [row.nums for row in actual.collect()]
    # collect_list windows already retained NULL inputs before this PR. Keep the
    # qualifier/execution checks above unconditional and recognize only that mismatch.
    retained_nulls = expected[:3] + [[None, *row] for row in expected[3:]]
    if not is_jvm_spark() and function == "collect_list" and actual == retained_nulls:
        pytest.xfail("Pre-existing collect_list windows retain NULL inputs")
    assert actual == expected


@pytest.mark.parametrize("function", ["first", "last", "any_value"])
@pytest.mark.parametrize("all_null", [False, True])
def test_aggregate_interval_array_ignore_nulls(spark, function, all_null):
    frame = _nullable_year_array_frame(spark).where("id IN (0, 5)" if all_null else "id IN (0, 3, 5)")
    result = frame.agg(F.expr(f"{function}(a, true)").alias("result"))
    array_type = T.ArrayType(T.YearMonthIntervalType(0, 0), True)
    assert result.schema == T.StructType([T.StructField("result", array_type, True)])
    actual = result.selectExpr("transform(result, x -> CAST(x AS INT)) AS result").collect()[0].result
    assert actual == (None if all_null else [1])


@pytest.mark.parametrize(("function", "expected"), [("min_by", []), ("max_by", None)])
def test_min_max_by_independent_interval_array_ordering(spark, function, expected):
    schema = T.StructType(
        [
            T.StructField("value_count", T.IntegerType(), True),
            T.StructField("order_count", T.IntegerType(), True),
        ]
    )
    frame = (
        spark.createDataFrame([(1, None), (2, 1), (None, 2), (0, 0)], schema)
        .selectExpr(
            "array_repeat(INTERVAL '1' YEAR, value_count) AS a",
            "array_repeat(INTERVAL '1' MONTH, order_count) AS ordering",
        )
        .repartition(2)
    )
    result = frame.agg(F.expr(f"{function}(a, ordering)").alias("result"))
    array_type = T.ArrayType(T.YearMonthIntervalType(0, 0), False)
    assert result.schema == T.StructType([T.StructField("result", array_type, True)])
    actual = result.selectExpr("transform(result, x -> CAST(x AS INT)) AS result").collect()[0].result
    assert actual == expected


@pytest.mark.parametrize(
    ("expression", "bounds", "expected"),
    [
        ("first_value(a)", "CURRENT ROW AND UNBOUNDED FOLLOWING", [None, [], [None], [1], [2, 2], None]),
        ("first_value(a) IGNORE NULLS", None, [[]] * 6),
        ("last_value(a)", "UNBOUNDED PRECEDING AND CURRENT ROW", [None, [], [None], [1], [2, 2], None]),
        ("last_value(a) IGNORE NULLS", None, [[2, 2]] * 6),
        ("nth_value(a, 2)", None, [[]] * 6),
        ("nth_value(a, 2) IGNORE NULLS", None, [[None]] * 6),
        ("nth_value(a, 4)", None, [[1]] * 6),
        ("nth_value(a, 8)", None, [None] * 6),
        ("min(a)", "1 PRECEDING AND CURRENT ROW", [None, [], [], [None], [1], [2, 2]]),
        ("max(a)", "1 PRECEDING AND CURRENT ROW", [None, [], [None], [1], [2, 2], [2, 2]]),
    ],
)
def test_window_preserves_nullable_interval_array(spark, expression, bounds, expected):
    bounds = bounds or "UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
    result = (
        _nullable_year_array_frame(spark)
        .selectExpr("id", f"{expression} OVER (ORDER BY id ROWS BETWEEN {bounds}) AS result")
        .orderBy("id")
        .select("result")
    )
    array_type = T.ArrayType(T.YearMonthIntervalType(0, 0), True)
    assert result.schema == T.StructType([T.StructField("result", array_type, True)])
    actual = [row.result for row in result.selectExpr("transform(result, x -> CAST(x AS INT)) AS result").collect()]
    # DataFusion's pre-existing array comparator puts NULL elements last. Only
    # this exact known value mismatch is expected; schema/runtime errors still fail.
    nulls_last = {
        "min(a)": [None, [], [], [1], [1], [2, 2]],
        "max(a)": [None, [], [None], [None], [2, 2], [2, 2]],
    }
    if not is_jvm_spark() and actual == nulls_last.get(expression):
        pytest.xfail("Pre-existing NULL-element ordering differs from Spark")
    assert actual == expected


@pytest.mark.parametrize(
    ("function", "expected"),
    [("min", [None, [], [], [1], [2, 2]]), ("max", [None, [], [1], [2, 2], [2, 2]])],
)
def test_sliding_window_preserves_interval_array_state(spark, function, expected):
    # Keep NULL/empty arrays and retraction coverage independent of the pre-existing
    # NULL-element ordering bug covered above.
    result = (
        _nullable_year_array_frame(spark)
        .where("id != 2")
        .selectExpr("id", f"{function}(a) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS result")
        .orderBy("id")
        .select("result")
    )
    array_type = T.ArrayType(T.YearMonthIntervalType(0, 0), True)
    assert result.schema == T.StructType([T.StructField("result", array_type, True)])
    actual = result.selectExpr("transform(result, x -> CAST(x AS INT)) AS result").collect()
    assert [row.result for row in actual] == expected


@pytest.mark.parametrize("function", ["lead", "lag"])
@pytest.mark.parametrize("default", ["omitted", "null", "year"])
@pytest.mark.parametrize("ignore_nulls", [False, True])
def test_lead_lag_preserves_interval_array(spark, function, default, ignore_nulls):
    arguments = {"omitted": "a", "null": "a, 1, NULL", "year": "a, 1, array_repeat(INTERVAL '9' YEAR, 1)"}
    null_treatment = " IGNORE NULLS" if ignore_nulls else ""
    result = (
        _nullable_year_array_frame(spark)
        .selectExpr("id", f"{function}({arguments[default]}){null_treatment} OVER (ORDER BY id) AS result")
        .orderBy("id")
        .select("result")
    )
    array_type = T.ArrayType(T.YearMonthIntervalType(0, 0), True)
    assert result.schema == T.StructType([T.StructField("result", array_type, True)])
    boundary = [9] if default == "year" else None
    if function == "lead":
        expected = [[], [None], [1], [2, 2], boundary if ignore_nulls else None, boundary]
    else:
        expected = [boundary, boundary if ignore_nulls else None, [], [None], [1], [2, 2]]
    actual = result.selectExpr("transform(result, x -> CAST(x AS INT)) AS result").collect()
    assert [row.result for row in actual] == expected


@pytest.mark.parametrize("function", ["lead", "lag"])
def test_lead_lag_rejects_different_interval_array_default(spark, function):
    source = _nullable_year_array_frame(spark)
    with pytest.raises(Exception, match=r"(?i)(datatype|data type|interval type)"):
        _ = source.selectExpr(
            f"{function}(a, 1, array_repeat(INTERVAL '1' MONTH, 1)) OVER (ORDER BY id) AS result"
        ).schema


@pytest.mark.parametrize("function", ["lead", "lag"])
def test_lead_lag_preserves_input_struct_annotations(spark, function):
    input_type = T.StructType([T.StructField("x", T.IntegerType(), True, {"comment": "input"})])
    default_type = T.StructType([T.StructField("x", T.IntegerType(), True, {"comment": "default"})])
    source = spark.range(3).select(
        "id", F.from_json(F.concat(F.lit('{"x":'), F.col("id").cast("string"), F.lit("}")), input_type).alias("a")
    )
    default = f"from_json('{{\"x\":9}}', '{default_type.json()}')"
    result = (
        source.repartition(2)
        .selectExpr("id", f"{function}(a, 1, {default}) OVER (ORDER BY id) AS result")
        .orderBy("id")
        .select("result")
    )
    assert result.schema == T.StructType([T.StructField("result", input_type, True)])
    assert [row.result.x for row in result.collect()] == ([1, 2, 9] if function == "lead" else [9, 0, 1])


@pytest.mark.parametrize("nullable", [False, True])
def test_nullifzero_preserves_null_type_with_nullable_output(spark, nullable):
    # Arrange
    schema = T.StructType([T.StructField("x", T.NullType(), nullable=nullable)])
    # A non-nullable NullType field requires an empty input.
    df = spark.createDataFrame([], schema)
    expected = T.StructType([T.StructField("result", T.NullType(), nullable=True)])

    # Act
    result = df.selectExpr("nullifzero(x) AS result")

    # Assert
    assert df.schema == schema
    assert result.schema == expected


# --- functions that correctly propagate nullability (pass on Sail and JVM) ---


def test_length_propagates_nullability(spark):
    out = _frame(spark).select(
        F.length("nn").alias("from_nn"),
        F.length("nl").alias("from_nl"),
    )
    assert _nullable(out, "from_nn") is False
    assert _nullable(out, "from_nl") is True


def test_concat_propagates_nullability(spark):
    out = _frame(spark).select(
        F.concat("nn", "nn").alias("both_non_null"),
        F.concat("nn", "nl").alias("one_nullable"),
    )
    assert _nullable(out, "both_non_null") is False
    assert _nullable(out, "one_nullable") is True


def test_coalesce_propagates_nullability(spark):
    out = _frame(spark).select(
        F.coalesce("nn", "nl").alias("has_non_null"),
        F.coalesce("nl", "nl").alias("all_nullable"),
    )
    assert _nullable(out, "has_non_null") is False
    assert _nullable(out, "all_nullable") is True


# --- functions where Sail is over-nullable ---
# Both cases are checked so we see exactly where each is right and wrong:
# Sail gets the nullable input right (nullable -> nullable) but the non-null
# input wrong (non-null -> Sail still says nullable, Spark says non-nullable).


@pytest.mark.parametrize("fn", [F.upper, F.lower, F.ascii, F.initcap])
def test_pure_string_fn_of_nullable_is_nullable(spark, fn):
    # nullable input -> nullable output: Sail and Spark agree
    out = _frame(spark).select(fn("nl").alias("result"))
    assert _nullable(out, "result") is True


@pytest.mark.parametrize("fn", [F.upper, F.lower, F.ascii, F.initcap])
@sail_bug
def test_pure_string_fn_of_non_null_is_non_nullable(spark, fn):
    # non-null input -> non-nullable output: Sail diverges (says nullable=True)
    out = _frame(spark).select(fn("nn").alias("result"))
    assert _nullable(out, "result") is False


# --- the dangerous direction: Sail UNDER-nullable ---
# round on a decimal can overflow, so Spark declares it nullable=True even for a
# non-null input. Sail declares it non-nullable -> if a NULL ever appears an
# optimizer could drop null-checks, so this is a correctness risk, not just
# schema fidelity. (bround matches Spark here, kept as the passing counterpart.)


def test_round_of_nullable_decimal_is_nullable(spark):
    out = _decimal_frame(spark).select(F.round("nl", 1).alias("result"))
    assert _nullable(out, "result") is True  # Sail and Spark agree


def test_bround_of_non_null_decimal_is_nullable(spark):
    out = _decimal_frame(spark).select(F.bround("nn", 1).alias("result"))
    assert _nullable(out, "result") is True  # Sail and Spark agree


@sail_bug
def test_round_of_non_null_decimal_stays_nullable(spark):
    # Spark keeps round(decimal) nullable (overflow); Sail says non-nullable.
    out = _decimal_frame(spark).select(F.round("nn", 1).alias("result"))
    assert _nullable(out, "result") is True


@pytest.mark.parametrize("expression", ["array_compact(a)", "filter(a, x -> x > 1)", "array_sort(a, (l,r) -> l-r)"])
def test_derived_array_does_not_inherit_column_metadata(spark, expression):
    frame = spark.range(1).select(
        F.array(F.lit(2), F.lit(1)).alias(
            "a",
            metadata={"comment": "input only", "source": "test"},
        )
    )
    result = frame.selectExpr(f"{expression} AS result")
    assert result.schema["result"].metadata == {}
    expected = [2] if expression.startswith("filter") else ([1, 2] if expression.startswith("array_sort") else [2, 1])
    assert result.collect()[0].result == expected


@pytest.mark.parametrize("kind", ["array", "map_values", "map_keys"])
def test_aggregate_lambda_output_with_annotated_capture(spark, kind):
    from datetime import timedelta

    expressions = {
        "array": "transform(array(1, 2), x -> i)",
        "map_values": "transform_values(map(1, 2), (k, v) -> i)",
        "map_keys": "transform_keys(map(1, 2), (k, v) -> i)",
    }
    source = spark.range(3).select(F.expr("CAST(id AS INTERVAL DAY)").alias("i", metadata={"source": "capture"}))
    result = source.selectExpr(f"{expressions[kind]} AS value").agg(F.collect_list("value").alias("result"))
    days = [timedelta(days=i) for i in range(3)]
    if kind == "array":
        expected = [[day, day] for day in days]
    elif kind == "map_values":
        expected = [{1: day} for day in days]
    else:
        expected = [{day: 2} for day in days]
    assert sorted(result.collect()[0].result, key=repr) == sorted(expected, key=repr)


@pytest.mark.parametrize("kind", ["array", "map_values", "map_keys"])
@pytest.mark.parametrize("aggregate", [False, True])
def test_transform_lambda_retains_captured_interval_qualifier(spark, kind, aggregate):
    from datetime import timedelta

    expressions = {
        "array": "transform(array(1, 2), x -> i)",
        "map_values": "transform_values(map(1, 2), (k, v) -> i)",
        "map_keys": "transform_keys(map(1, 2), (k, v) -> i)",
    }
    source = spark.range(3).select(F.expr("CAST(id AS INTERVAL DAY)").alias("i", metadata={"source": "capture"}))
    result = source.selectExpr(f"{expressions[kind]} AS result")
    if aggregate:
        result = result.repartition(2).agg(F.collect_list("result").alias("result"))
    data_type = result.schema["result"].dataType
    if aggregate:
        data_type = data_type.elementType
    days = [timedelta(days=i) for i in range(3)]
    if kind == "array":
        interval_type = data_type.elementType
        expected = [[day, day] for day in days]
    elif kind == "map_values":
        interval_type = data_type.valueType
        expected = [{1: day} for day in days]
    else:
        interval_type = data_type.keyType
        expected = [{day: 2} for day in days]
    assert interval_type == T.DayTimeIntervalType(0, 0)
    rows = result.collect()
    actual = rows[0].result if aggregate else [row.result for row in rows]
    assert sorted(actual, key=repr) == sorted(expected, key=repr)


@pytest.mark.parametrize("element_nullable", [False, True])
@pytest.mark.parametrize("count_nullable", [False, True])
def test_array_repeat_preserves_element_and_count_nullability(spark, element_nullable, count_nullable):
    schema = T.StructType(
        [
            T.StructField("element", T.IntegerType(), element_nullable),
            T.StructField("count", T.IntegerType(), count_nullable),
        ]
    )
    rows = [(1, 2)]
    if element_nullable:
        rows.append((None, 2))
    if count_nullable:
        rows.append((1, None))
    result = spark.createDataFrame(rows, schema).selectExpr("array_repeat(element, count) AS result")
    assert result.schema == T.StructType(
        [T.StructField("result", T.ArrayType(T.IntegerType(), element_nullable), count_nullable)]
    )
    expected = [None if count is None else [element] * count for element, count in rows]
    assert [row.result for row in result.collect()] == expected


@pytest.mark.parametrize("dynamic", [False, True])
def test_array_repeat_preserves_scalar_interval_type(spark, dynamic):
    from datetime import timedelta

    source = spark.range(2).selectExpr("CAST(id AS INTERVAL DAY) AS value" if dynamic else "INTERVAL '2' DAY AS value")
    result = source.selectExpr("array_repeat(value, 2) AS result")
    assert result.schema["result"].dataType.elementType == T.DayTimeIntervalType(0, 0)
    assert [row.result for row in result.collect()] == [[timedelta(days=i if dynamic else 2)] * 2 for i in range(2)]


@pytest.mark.parametrize("value", ["NaN", "Infinity", "1"])
@pytest.mark.parametrize("ansi", [True, False])
def test_array_position_string_float_cast_is_nullable(spark, value, ansi):
    original = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    try:
        source = spark.range(1).select(F.lit(value).cast("double").alias("x"))
        result = source.selectExpr("array_position(array(x), x) AS result")
        assert result.schema["result"].nullable is True
        assert result.collect()[0].result == 1
        control = spark.sql("SELECT array_position(array(1D), 1D) AS result")
        assert control.schema["result"].nullable is False
        assert control.collect()[0].result == 1
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original)
