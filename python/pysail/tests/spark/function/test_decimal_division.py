import math
from decimal import Decimal

import pyspark.sql.functions as F  # noqa: N812
import pytest


@pytest.fixture
def df(spark):
    return spark.sql("SELECT * FROM VALUES (1, 1, 0, NULL), (2, NULL, 1, 2.0), (3, 3, 4, 3.5) AS tab(a, b, c, d)")


def column(df, expr):
    actual = df.select(expr.alias("x"))
    return actual.schema["x"].dataType.simpleString(), [row.x for row in actual.collect()]


def test_decimal_division_result_type(df):
    assert column(df, df.d / 3) == ("decimal(7,6)", [None, Decimal("0.666667"), Decimal("1.166667")])
    assert column(df, df.d / 7) == ("decimal(7,6)", [None, Decimal("0.285714"), Decimal("0.500000")])
    assert column(df, df.d / df.d) == ("decimal(8,6)", [None, Decimal("1.000000"), Decimal("1.000000")])
    # The integer column is a `decimal(10,0)` and the literal is a `decimal(1,0)`.
    assert column(df, df.d / df.b / 3) == ("decimal(15,14)", [None, None, Decimal("0.38888888888900")])
    assert column(df, 3 - df.a + df.b * df.c - df.d / df.b) == (
        "decimal(23,12)",
        [None, None, Decimal("10.833333333333")],
    )
    # The precision is reduced to 38 digits by giving up scale.
    assert column(df, F.expr("cast(d as decimal(38,10)) / 3")) == (
        "decimal(38,10)",
        [None, Decimal("0.6666666667"), Decimal("1.1666666667")],
    )


def test_integer_literal_with_decimal_has_its_digits(df):
    # The literal is a `decimal(1,0)`, while an integer column would be a `decimal(10,0)`.
    assert column(df, F.expr("1 - d")) == ("decimal(3,1)", [None, Decimal("-1.0"), Decimal("-2.5")])
    assert column(df, F.expr("d * (1 - d)")) == ("decimal(6,2)", [None, Decimal("-2.00"), Decimal("-8.75")])
    assert column(df, F.expr("d + a")) == ("decimal(12,1)", [None, Decimal("4.0"), Decimal("6.5")])


def test_case_of_decimal_and_integer_has_wider_decimal(df):
    # Outside arithmetic the integer literal is a `decimal(10,0)`, as an integer column would be.
    assert column(df, F.expr("CASE WHEN a > 1 THEN d * 2 ELSE 0 END")) == (
        "decimal(11,1)",
        [Decimal("0.0"), Decimal("4.0"), Decimal("7.0")],
    )


def test_decimal_window_average_rounds_half_up(spark):
    df = spark.sql("SELECT avg(x) OVER () AS x FROM VALUES (2.0), (2.0), (1.0) AS t(x)")
    assert df.schema["x"].dataType.simpleString() == "decimal(6,5)"
    assert [row.x for row in df.collect()] == [Decimal("1.66667")] * 3


def test_decimal_division_with_double_is_double(df):
    assert column(df, df.d / 2.5) == ("double", [None, 0.8, 1.4])


def test_decimal_and_double_have_double_type(spark):
    df = spark.sql('SELECT * FROM VALUES (0, float("NAN"), NULL), (1, NULL, 2.0), (2, 2.1, 3.5) AS tab(a, b, c)')
    assert column(df, F.coalesce(df.a, "b", df.c)) == ("double", [0.0, 1.0, 2.0])
    assert column(df, F.least(df.a, "b", df.c)) == ("double", [0.0, 1.0, 2.0])
    data_type, values = column(df, F.greatest(df.a, "b", df.c))
    assert data_type == "double"
    assert math.isnan(values[0])
    assert values[1:] == [2.0, 3.5]
