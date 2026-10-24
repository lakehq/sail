import math
from decimal import Decimal

import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.sql.window import Window


@pytest.fixture
def spark_without_ansi(spark):
    original = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    try:
        yield spark
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original)


def order_by_b(row):
    # NULL first and NaN last, as in the window ordering.
    if row.b is None:
        return -math.inf
    return math.inf if math.isnan(row.b) else row.b


def test_lag_lead_default_that_cannot_be_cast_is_null(spark_without_ansi):
    df = spark_without_ansi.sql(
        'SELECT * FROM VALUES (0, float("NAN"), NULL), (1, NULL, 2.0), (1, 2.1, 3.5), (0, 0.5, 1.0) AS tab(a, b, c)'
    )
    window = Window.orderBy("b")
    actual = df.select(
        "b",
        F.lag(df.c, -1, float("nan")).over(window).alias("lag"),
        F.lead(df.c, 1, float("nan")).over(window).alias("lead"),
    )
    assert actual.schema["lag"].dataType.simpleString() == "decimal(2,1)"
    rows = sorted(actual.collect(), key=order_by_b)
    # The default is only used for the last row, and NaN is NULL when it is cast to the decimal type of `c`.
    assert [(row.lag, row.lead) for row in rows] == [
        (Decimal("1.0"), Decimal("1.0")),
        (Decimal("3.5"), Decimal("3.5")),
        (None, None),
        (None, None),
    ]
