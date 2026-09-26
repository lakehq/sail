from decimal import Decimal

import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import ArrayType, DayTimeIntervalType, DecimalType, LongType, YearMonthIntervalType

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version


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
        ("100000000000000000000000", 38, 0, Decimal(99999999999999990000000)),
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
    # Function names and interval qualifiers are fixed pytest parameters.
    result = spark.sql(f"""
        SELECT {function}(v, 0.5) AS scalar, {function}(v, array(0.5)) AS array
        FROM VALUES (INTERVAL '1' {qualifier}), (INTERVAL '2' {qualifier}) AS t(v)
    """)  # noqa: S608
    assert result.schema["scalar"].dataType == expected_type
    assert result.schema["array"].dataType == ArrayType(expected_type, containsNull=False)
    assert result.select(F.size("array")).first()[0] == 1


@pytest.mark.parametrize("window", [False, True])
def test_approx_percentile_column_parameter_foldability(spark, window):
    percentage = F.array_compact(F.array(F.lit(0.0), F.lit(None).cast("double"), F.lit(1.0)))
    aggregate = F.percentile_approx("value", percentage)
    if window:
        aggregate = aggregate.over(Window.partitionBy())
    with pytest.raises(Exception, match=r"(?i)foldable"):
        spark.createDataFrame([(1,), (2,)], ["value"]).select(aggregate).collect()


@pytest.mark.skipif(pyspark_version() >= (4,), reason="ARRAY_APPEND is foldable with constants only before Spark 4")
def test_approx_percentile_legacy_array_append_parameter(spark):
    percentages = F.array_append(F.array(F.lit(0.5)), F.lit(1.0))
    result = spark.createDataFrame([(1,), (2,)], ["value"]).select(F.percentile_approx("value", percentages))
    assert result.first()[0] == [1, 2]


@pytest.mark.parametrize("window", [False, True])
@pytest.mark.parametrize("parameter", ["percentage", "accuracy"])
def test_approx_percentile_python_udf_parameters_are_not_foldable(spark, window, parameter):
    @F.udf("double" if parameter == "percentage" else "int")
    def constant():
        return 0.5 if parameter == "percentage" else 100

    percentage = constant() if parameter == "percentage" else F.lit(0.5)
    accuracy = constant() if parameter == "accuracy" else F.lit(100)
    aggregate = F.percentile_approx("id", percentage, accuracy)
    if window:
        aggregate = aggregate.over(Window.partitionBy())
    with pytest.raises(Exception, match=r"(?i)foldable"):
        spark.range(4).select(aggregate).collect()


def test_approx_percentile_typeof_python_udf_remains_foldable(spark):
    @F.udf("double")
    def fail_if_evaluated():
        message = "TYPEOF must not evaluate its input"
        raise AssertionError(message)

    percentage = F.when(F.typeof(fail_if_evaluated()) == "double", F.lit(0.5)).otherwise(F.lit(0.0))
    result = spark.range(4).select(F.percentile_approx("id", percentage).alias("p"))
    assert result.first().p == 1


@pytest.mark.skipif(pyspark_version() >= (4,), reason="ENCODE is foldable with constants only before Spark 4")
def test_approx_percentile_legacy_encode_parameter(spark):
    accuracy = F.length(F.encode(F.lit("abcd"), "UTF-8"))
    result = spark.createDataFrame([(1,), (2,)], ["value"]).select(F.percentile_approx("value", F.lit(0.5), accuracy))
    assert result.first()[0] == 1
