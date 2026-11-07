"""Spark semantics of `collect_list`/`collect_set`/`listagg` windows and of nested `min`/`max`."""

import math

import pytest


def rows(spark, sql):
    return [tuple(row) for row in spark.sql(sql).collect()]


def test_window_collect_list_skips_nulls(spark):
    assert rows(
        spark,
        "SELECT id, collect_list(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) "
        "FROM VALUES (1, 1.0D), (2, NULL), (3, 3.0D) AS t(id, v) ORDER BY id",
    ) == [(1, [1.0]), (2, [1.0, 3.0]), (3, [3.0])]


@pytest.mark.parametrize("function", ["collect_list", "collect_set", "array_agg"])
def test_window_collect_of_an_empty_frame_is_an_empty_array(spark, function):
    assert rows(
        spark,
        f"SELECT id, {function}(v) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) "  # noqa: S608
        "FROM VALUES (1, 1), (2, 2) AS t(id, v) ORDER BY id",
    ) == [(1, []), (2, [1])]


def test_listagg_distinct_follows_the_requested_order(spark):
    assert rows(
        spark,
        "SELECT listagg(DISTINCT x, ',') WITHIN GROUP (ORDER BY x DESC) FROM VALUES ('a'), ('b'), ('a') AS t(x)",
    ) == [("b,a",)]


@pytest.mark.parametrize(
    ("values", "delimiter", "expected"),
    [
        ("('b'), ('a'), (NULL)", "NULL", "ab"),
        ("('b'), ('a')", "CAST(NULL AS STRING)", "ab"),
        ("(X'62'), (X'61')", "NULL", b"ab"),
        ("(X'62'), (X'61')", "CAST(NULL AS BINARY)", b"ab"),
        ("(CAST(NULL AS STRING))", "NULL", None),
    ],
    ids=["string", "typed_string", "binary", "typed_binary", "all_null"],
)
@pytest.mark.parametrize("function", ["listagg", "string_agg"])
def test_listagg_with_a_null_delimiter_has_no_delimiter(spark, function, values, delimiter, expected):
    assert rows(
        spark,
        f"SELECT {function}(x, {delimiter}) WITHIN GROUP (ORDER BY x) FROM VALUES {values} AS t(x)",  # noqa: S608
    ) == [(expected,)]


def test_listagg_with_a_null_delimiter_has_no_delimiter_in_distinct_and_window(spark):
    assert rows(
        spark,
        "SELECT listagg(DISTINCT x, NULL) WITHIN GROUP (ORDER BY x) FROM VALUES ('b'), ('a'), ('b') AS t(x)",
    ) == [("ab",)]
    assert rows(
        spark,
        "SELECT id, listagg(x, NULL) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) "
        "FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(id, x) ORDER BY id",
    ) == [(1, "a"), (2, "ab"), (3, "bc")]
    assert rows(
        spark,
        "SELECT id, listagg(b, NULL) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) "
        "FROM VALUES (1, X'61'), (2, X'62') AS t(id, b) ORDER BY id",
    ) == [(1, b"a"), (2, b"ab")]


@pytest.mark.parametrize(
    ("values", "expected_min", "expected_max"),
    [
        ("(array(1, 2)), (array(1, NULL))", [1, None], [1, 2]),
        ("(array(1, 2)), (array(1))", [1], [1, 2]),
        ("(named_struct('a', 1, 'b', 'x')), (named_struct('a', CAST(NULL AS INT), 'b', 'z'))", (None, "z"), (1, "x")),
        ("SELECT array(named_struct('a', CAST(id AS INT))) AS v FROM range(2)", [(0,)], [(1,)]),
    ],
    ids=["array_null_element", "array_length", "struct_null_field", "array_of_struct"],
)
@pytest.mark.parametrize("mode", ["ungrouped", "grouped", "window"])
def test_min_max_of_nested_values(spark, values, expected_min, expected_max, mode):
    table = f"({values})" if values.startswith("SELECT") else f"VALUES {values} AS t(v)"
    if mode == "ungrouped":
        sql = f"SELECT min(v), max(v) FROM {table}"  # noqa: S608
    elif mode == "grouped":
        sql = f"SELECT min(v), max(v) FROM (SELECT 1 AS g, v FROM {table}) GROUP BY g"  # noqa: S608
    else:
        sql = (
            "SELECT min(v) OVER w, max(v) OVER w FROM "  # noqa: S608
            f"(SELECT monotonically_increasing_id() AS o, v FROM {table}) "
            "WINDOW w AS (ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1"
        )

    def plain(value):
        if isinstance(value, list):
            return [plain(v) for v in value]
        if hasattr(value, "asDict"):
            return tuple(plain(v) for v in value)
        return value

    actual = spark.sql(sql).collect()[0]
    assert (plain(actual[0]), plain(actual[1])) == (expected_min, expected_max)


FLOAT_VALUES = (
    "VALUES (1, -0.0D), (1, 0.0D), (1, CAST('NaN' AS DOUBLE)), (1, CAST('NaN' AS DOUBLE)), (1, NULL), "
    "(2, -0.0D), (2, -0.0D) AS t(k, v)"
)


def spelled(value):
    """Spells floats with `repr` so that `-0.0` differs from `0.0` and NaN equals NaN.
    Arrays are sorted since the order of `collect_set` is unspecified."""
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, list):
        return sorted((spelled(v) for v in value), key=repr)
    if isinstance(value, dict):
        return tuple(sorted((k, spelled(v)) for k, v in value.items()))
    if hasattr(value, "asDict"):
        return tuple(spelled(v) for v in value)
    return value


@pytest.mark.parametrize(
    "aggregate", ["collect_set(v)", "collect_list(DISTINCT v)", "array_agg(DISTINCT v)", "collect_set(DISTINCT v)"]
)
@pytest.mark.parametrize("data_type", ["DOUBLE", "FLOAT"])
@pytest.mark.parametrize("grouped", [True, False], ids=["grouped", "ungrouped"])
def test_distinct_collect_normalizes_negative_zero(spark, aggregate, data_type, grouped):
    table = f"(SELECT k, CAST(v AS {data_type}) AS v FROM {FLOAT_VALUES})"
    if grouped:
        sql = f"SELECT k, {aggregate} FROM {table} GROUP BY k ORDER BY k"  # noqa: S608
        expected = [(1, ["0.0", "nan"]), (2, ["0.0"])]
    else:
        sql = f"SELECT {aggregate} FROM {table}"  # noqa: S608
        expected = [(["0.0", "nan"],)]
    assert [tuple(spelled(v) for v in row) for row in spark.sql(sql).collect()] == expected


def test_distinct_collect_normalizes_negative_zero_in_nested_values(spark):
    sql = (
        "SELECT k, collect_set(array(v)), collect_set(named_struct('a', v)), "  # noqa: S608
        f"collect_list(DISTINCT map('m', v)) FROM {FLOAT_VALUES} GROUP BY k ORDER BY k"
    )
    assert [tuple(spelled(v) for v in row) for row in spark.sql(sql).collect()] == [
        (
            1,
            [["0.0"], ["nan"], [None]],
            [("0.0",), ("nan",), (None,)],
            [(("m", "0.0"),), (("m", "nan"),), (("m", None),)],
        ),
        (2, [["0.0"]], [("0.0",)], [(("m", "0.0"),)]),
    ]


def test_window_collect_set_normalizes_negative_zero(spark):
    assert [
        tuple(spelled(v) for v in row)
        for row in spark.sql(
            "SELECT id, collect_set(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) "
            "FROM VALUES (1, -0.0D), (2, 0.0D), (3, -0.0D), (4, CAST('NaN' AS DOUBLE)) AS t(id, v) ORDER BY id"
        ).collect()
    ] == [(1, ["0.0"]), (2, ["0.0"]), (3, ["0.0"]), (4, ["0.0", "nan"])]


def test_collect_list_keeps_negative_zero(spark):
    assert [
        tuple(spelled(v) for v in row)
        for row in spark.sql(f"SELECT k, collect_list(v) FROM {FLOAT_VALUES} GROUP BY k ORDER BY k").collect()  # noqa: S608
    ] == [(1, ["-0.0", "0.0", "nan", "nan"]), (2, ["-0.0", "-0.0"])]


@pytest.mark.parametrize("data_type", ["DOUBLE", "FLOAT"])
@pytest.mark.parametrize(
    "values",
    ["(1, 1.0D), (1, -CAST('NaN' AS DOUBLE))", "(1, -CAST('NaN' AS DOUBLE)), (1, 1.0D)"],
    ids=["nan_last", "nan_first"],
)
def test_grouped_min_max_treat_negative_nan_as_nan(spark, data_type, values):
    # Spark treats all NaN values as the same value, which is larger than any other value.
    [(maximum, minimum)] = rows(
        spark,
        f"SELECT max(CAST(x AS {data_type})), min(CAST(x AS {data_type})) FROM VALUES {values} AS t(k, x) GROUP BY k",  # noqa: S608
    )
    assert math.isnan(maximum)
    assert minimum == 1.0


@pytest.mark.parametrize(
    ("frame", "expected"),
    [
        (
            "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
            [(1, [1.0, None]), (2, [1.0, None, 3.0]), (3, [None, 3.0]), (4, [None])],
        ),
        (
            "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
            [(1, [1.0, None, 3.0]), (2, [1.0, None, 3.0]), (3, [1.0, None, 3.0]), (4, [None])],
        ),
    ],
)
def test_window_collect_list_respect_nulls_keeps_nulls(spark, frame, expected):
    actual = rows(
        spark,
        f"SELECT id, collect_list(v) RESPECT NULLS OVER (PARTITION BY k ORDER BY id {frame}) "  # noqa: S608
        "FROM VALUES (1, 1, 1.0D), (2, 1, NULL), (3, 1, 3.0D), (4, 2, NULL) AS t(id, k, v) ORDER BY id",
    )
    assert actual == expected


@pytest.mark.parametrize("function", ["min", "max"])
@pytest.mark.parametrize(
    "values",
    ["(map(1, 2))", "(array(map(1, 2))), (array(map(1, 3)))", "(named_struct('m', map(1, 2)))"],
    ids=["map", "array_of_map", "struct_of_map"],
)
@pytest.mark.parametrize("mode", ["ungrouped", "grouped", "window"])
def test_min_max_reject_types_that_contain_a_map(spark, function, values, mode):
    # Spark cannot order a map.
    sql = {
        "ungrouped": f"SELECT {function}(v) FROM VALUES {values} AS t(v)",
        "grouped": f"SELECT k, {function}(v) FROM (SELECT 1 AS k, v FROM VALUES {values} AS t(v)) GROUP BY k",
        "window": f"SELECT {function}(v) OVER (ORDER BY 1 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) "
        f"FROM VALUES {values} AS t(v)",
    }[mode]
    with pytest.raises(Exception, match="(?i)ordering"):
        spark.sql(sql).collect()
