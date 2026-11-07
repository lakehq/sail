"""Result types of `coalesce`, `nvl2` and `nullif`, as in Spark."""

import datetime

import pytest


def result(spark, sql):
    df = spark.sql(sql)
    return df.schema[0].dataType.simpleString(), [row[0] for row in df.collect()]


@pytest.mark.parametrize("function", ["coalesce", "nvl", "ifnull"])
def test_date_and_timestamp_coerce_to_timestamp(spark, function):
    spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    try:
        assert result(spark, f"SELECT {function}(CAST(NULL AS DATE), TIMESTAMP'2024-01-01 10:00:00')") == (
            "timestamp",
            [datetime.datetime(2024, 1, 1, 10, 0)],  # noqa: DTZ001
        )
        assert result(spark, f"SELECT {function}(DATE'2024-01-02', TIMESTAMP'2024-01-01 10:00:00')") == (
            "timestamp",
            [datetime.datetime(2024, 1, 2, 0, 0)],  # noqa: DTZ001
        )
    finally:
        spark.conf.unset("spark.sql.session.timeZone")


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT nvl2('a', 1, 2)", ("int", [1])),
        ("SELECT nvl2(NULL, 2, 1)", ("int", [1])),
        ("SELECT nvl2(s, a, b) FROM VALUES ('x', 1, 10L), (NULL, 2, 20L) AS t(s, a, b)", ("bigint", [1, 20])),
        ("SELECT nvl2(named_struct('x', 1), 'y', 'n')", ("string", ["y"])),
    ],
)
def test_nvl2_result_type(spark, sql, expected):
    assert result(spark, sql) == expected


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT nullif(a, b) FROM VALUES (1, 1L), (2, 3L) AS t(a, b)", ("int", [None, 2])),
        ("SELECT nullif(b, a) FROM VALUES (1, 1L), (2, 3L) AS t(a, b)", ("bigint", [None, 3])),
        ("SELECT nullif(2, 1.5D)", ("int", [2])),
        ("SELECT nullif(CAST(1.50 AS DECIMAL(5, 2)), 1)", ("decimal(5,2)", [__import__("decimal").Decimal("1.50")])),
    ],
)
def test_nullif_returns_the_first_argument_type(spark, sql, expected):
    assert result(spark, sql) == expected


@pytest.mark.parametrize("time_zone", ["UTC", "America/Los_Angeles"])
def test_nullif_compares_date_and_timestamp_in_the_session_time_zone(spark, time_zone):
    spark.conf.set("spark.sql.session.timeZone", time_zone)
    try:
        assert result(spark, "SELECT nullif(DATE'2024-01-02', TIMESTAMP'2024-01-02 00:00:00')") == ("date", [None])
        assert result(spark, "SELECT nullif(DATE'2024-01-02', TIMESTAMP'2024-01-02 00:00:01')") == (
            "date",
            [datetime.date(2024, 1, 2)],
        )
    finally:
        spark.conf.unset("spark.sql.session.timeZone")


@pytest.mark.parametrize(
    "sql",
    ["SELECT nullif(1, NULL)", "SELECT nullif(1, CAST(NULL AS INT))", "SELECT nullif(1, 2)", "SELECT nullif(1, 2L)"],
)
def test_nullif_result_is_nullable(spark, sql):
    assert spark.sql(sql).schema[0].nullable


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        (
            # 02:30 does not exist on this day in the time zone, so the values must not be
            # converted to TIMESTAMP.
            "SELECT nullif(TIMESTAMP_NTZ'2024-03-10 02:30:00', TIMESTAMP_NTZ'2024-03-10 03:30:00')",
            ("timestamp_ntz", [datetime.datetime(2024, 3, 10, 2, 30)]),  # noqa: DTZ001
        ),
        ("SELECT nullif(TIMESTAMP_NTZ'2024-03-10 00:00:00', DATE'2024-03-10')", ("timestamp_ntz", [None])),
        ("SELECT nullif(DATE'2024-03-10', TIMESTAMP'2024-03-10 00:00:00')", ("date", [None])),
        ("SELECT nullif(DATE'2024-11-03', TIMESTAMP'2024-11-03 00:00:01')", ("date", [datetime.date(2024, 11, 3)])),
        (
            "SELECT nullif(TIMESTAMP_NTZ'2024-03-10 02:30:00', TIMESTAMP'2024-03-10 03:30:00')",
            ("timestamp_ntz", [None]),
        ),
    ],
)
def test_nullif_compares_timestamps_in_a_time_zone_with_daylight_saving_time(spark, sql, expected):
    spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    try:
        assert result(spark, sql) == expected
    finally:
        spark.conf.unset("spark.sql.session.timeZone")


@pytest.mark.parametrize(
    ("sql", "error"),
    [
        ("SELECT coalesce(NULL, raise_error('boom'), 1)", "boom"),
        ("SELECT coalesce(raise_error('boom'), 1)", "boom"),
        ("SELECT nvl(raise_error('boom'), 1)", "boom"),
        ("SELECT ifnull(raise_error('boom'), 1)", "boom"),
        ("SELECT coalesce(assert_true(id < 1), id) FROM range(3)", "(?i)is not true"),
        ("SELECT coalesce((SELECT NULL FROM range(2)), 1)", "(?i)more than one row"),
    ],
)
def test_coalesce_evaluates_null_arguments_that_raise_errors(spark, sql, error):
    with pytest.raises(Exception, match=error):
        spark.sql(sql).collect()
