from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DayTimeIntervalType, DecimalType, LongType, YearMonthIntervalType

from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.mark.parametrize("function", [F.approx_percentile, F.percentile_approx])
def test_approx_percentile_connect_interface(spark, function):
    df = spark.createDataFrame([(0,), (1,), (2,), (10,), (None,)], ["value"])
    result = df.select(
        function("value", F.lit(0.5), F.lit(100)).alias("scalar"),
        function("value", [0.5, 0.4, 0.1], 100).alias("array"),
        function("value", (0.0, 1.0)).alias("tuple"),
    )
    assert result.first().asDict() == {"scalar": 1, "array": [1, 1, 0], "tuple": [0, 10]}
    assert result.schema["scalar"].dataType == LongType()
    assert result.schema["array"].dataType == ArrayType(LongType(), containsNull=False)


def test_approx_percentile_partial_merge(spark):
    df = spark.range(1000, numPartitions=4)
    result = df.groupBy((F.col("id") % 2).alias("g")).agg(
        F.percentile_approx("id", [0.0, 0.5, 1.0], 1000000).alias("p")
    )
    assert [row.asDict() for row in result.orderBy("g").collect()] == [
        {"g": 0, "p": [0, 498, 998]},
        {"g": 1, "p": [1, 499, 999]},
    ]


@pytest.mark.parametrize(
    ("value", "precision", "scale", "expected"),
    [
        ("2.675123456789123456", 38, 18, Decimal("2.675123456789123600")),
        ("123456789012345678901.1234567890", 38, 10, Decimal("123456789012345680000.0000000000")),
        ("99999999999999999999999999999999999999", 38, 0, None),
        # Spark on Java 17 uses Double.toString's representation at this tie.
        ("100000000000000000000000", 38, 0, Decimal("99999999999999990000000")),
    ],
)
def test_approx_percentile_decimal_materialization(spark, value, precision, scale, expected):
    # Spark's SQL string rendering reads the raw Decimal's runtime scale, whereas
    # collecting the result materializes the declared type. BDD show tests cannot
    # observe the precision/scale adjustment (or overflow-to-null) at this boundary.
    result = spark.sql(f"SELECT percentile_approx(CAST('{value}' AS DECIMAL({precision},{scale})), 0.5) AS p")
    assert result.schema["p"].dataType == DecimalType(precision, scale)
    assert result.first().p == expected


@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="Arrow cannot represent NULL decimal-overflow elements in Spark's containsNull=false array",
    strict=True,
)
def test_approx_percentile_decimal_array_overflow(spark):
    result = spark.sql("""
        SELECT percentile_approx(
            CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)), array(0.5)
        ) AS p
    """)
    assert result.schema["p"].dataType == ArrayType(DecimalType(38, 0), containsNull=False)
    assert result.first().p == [None]


def test_approx_percentile_decimal_array_type(spark):
    result = spark.sql("""
        SELECT percentile_approx(
            CAST(2.675123456789123456 AS DECIMAL(38,18)), array(0D, 0.5D, 1D)
        ) AS p
    """)
    assert result.schema["p"].dataType == ArrayType(DecimalType(38, 18), containsNull=False)
    assert result.first().p == [Decimal("2.675123456789123600")] * 3


@pytest.mark.parametrize("function", ["approx_percentile", "percentile_approx"])
@pytest.mark.parametrize(
    ("qualifier", "expected_type"),
    [("MONTH", YearMonthIntervalType(1, 1)), ("SECOND", DayTimeIntervalType(3, 3))],
)
def test_approx_percentile_interval_qualifiers(spark, function, qualifier, expected_type):
    result = spark.sql(f"""
        SELECT {function}(v, 0.5) AS scalar, {function}(v, array(0.5)) AS array
        FROM VALUES (INTERVAL '1' {qualifier}), (INTERVAL '2' {qualifier}) AS t(v)
    """)
    assert result.schema["scalar"].dataType == expected_type
    assert result.schema["array"].dataType == ArrayType(expected_type, containsNull=False)
    assert result.select(F.size("array")).first()[0] == 1
