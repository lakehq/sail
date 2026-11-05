from decimal import Decimal

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import Row, Window
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import ArrayType, IntegerType

from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.fixture(params=["approx_percentile", "percentile_approx"])
def function(request):
    return request.param


def test_approx_percentile_selects_observations(spark, function):
    df = spark.sql("SELECT value FROM VALUES (0), (1), (2), (10), (CAST(NULL AS INT)) AS t(value)")
    result = df.select(
        getattr(F, function)("value", 0.5).alias("scalar"),
        getattr(F, function)("value", [1.0, 0.5, 0.4, 0.1, 0.0, 0.5], 1000000).alias("array"),
    )
    assert result.collect() == [Row(scalar=1, array=[10, 1, 1, 0, 0, 1])]
    assert result.schema["scalar"].dataType == IntegerType()
    assert result.schema["array"].dataType == ArrayType(IntegerType(), containsNull=False)
    assert result.schema["scalar"].nullable
    assert result.schema["array"].nullable


@pytest.mark.parametrize("data_type", ["tinyint", "smallint", "int", "bigint", "float", "double", "decimal(10,2)"])
def test_approx_percentile_preserves_numeric_type(spark, function, data_type):
    df = spark.sql(f"SELECT CAST(value AS {data_type}) AS value FROM VALUES (0), (1), (2), (10) AS t(value)")  # noqa: S608
    result = df.select(getattr(F, function)("value", [0.0, 0.5, 1.0], 1000000).alias("value"))
    expected = [Decimal("0.00"), Decimal("1.00"), Decimal("10.00")] if data_type.startswith("decimal") else [0, 1, 10]
    assert result.collect() == [Row(value=expected)]
    assert result.schema["value"].dataType == ArrayType(df.schema["value"].dataType, containsNull=False)


def test_approx_percentile_foldable_parameters(spark, function):
    result = spark.sql(
        f"SELECT {function}(value, array(1.0 / 2, CAST(1 AS DOUBLE)), 100 * 100) AS value "  # noqa: S608
        "FROM VALUES (0), (1), (2), (10) AS t(value)"
    )
    assert result.collect() == [Row(value=[1, 10])]


@pytest.mark.parametrize("rows", [[], [(None,), (None,)]], ids=["empty", "all-null"])
def test_approx_percentile_empty_input(spark, function, rows):
    condition = "true" if rows else "false"
    df = spark.sql(f"SELECT CAST(NULL AS INT) AS value FROM range(2) WHERE {condition}")  # noqa: S608
    result = df.select(
        getattr(F, function)("value", 0.5).alias("scalar"),
        getattr(F, function)("value", [0.0, 0.5, 1.0]).alias("array"),
    )
    assert result.collect() == [Row(scalar=None, array=None)]


def test_approx_percentile_empty_percentages(spark, function):
    assert spark.range(4).select(getattr(F, function)("id", []).alias("value")).collect() == [Row(value=None)]


def test_approx_percentile_merges_partitioned_groups(spark, function):
    df = spark.range(1000, numPartitions=11).selectExpr("id % 7 AS key", "CAST(id AS INT) AS value")
    result = df.groupBy("key").agg(getattr(F, function)("value", [1.0, 0.5, 0.0], 1000000).alias("value"))
    expected = []
    for key in range(7):
        values = list(range(key, 1000, 7))
        expected.append(Row(key=key, value=[values[-1], values[(len(values) - 1) // 2], values[0]]))
    assert result.orderBy("key").collect() == expected


def test_approx_percentile_window(spark, function):
    df = spark.sql("SELECT id, value FROM VALUES (0, 0), (1, 1), (2, 2), (3, 10) AS t(id, value)")
    frame = Window.orderBy("id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    result = df.select(
        "id",
        getattr(F, function)("value", 0.5).over(frame).alias("scalar"),
        getattr(F, function)("value", [1.0, 0.5, 0.0]).over(frame).alias("array"),
    )
    expected = [[0, 0, 0], [1, 0, 0], [2, 1, 0], [10, 1, 0]]
    assert result.orderBy("id").collect() == [
        Row(id=i, scalar=values[1], array=values) for i, values in enumerate(expected)
    ]


def test_approx_percentile_filter(spark, function):
    result = spark.sql(
        f"SELECT {function}(value, 0.5) FILTER (WHERE value > 1) AS filtered_value "  # noqa: S608
        "FROM VALUES (1), (1), (1), (2), (3), (NULL) AS t(value)"
    )
    assert result.collect() == [Row(filtered_value=2)]


@pytest.mark.skipif(is_jvm_spark(), reason="Sail does not support DISTINCT for approximate percentiles")
def test_approx_percentile_rejects_distinct(spark, function):
    with pytest.raises(PySparkException, match="does not support DISTINCT"):
        spark.sql(f"SELECT {function}(DISTINCT value, 0.75) FROM VALUES (1), (1), (1), (2), (3) AS t(value)").collect()  # noqa: S608


@pytest.mark.parametrize(
    ("percentage", "accuracy"),
    [
        ("-0.1", "10000"),
        ("1.1", "10000"),
        ("CAST('NaN' AS DOUBLE)", "10000"),
        ("NULL", "10000"),
        ("array(0.5, NULL)", "10000"),
        ("array(0.5, 1.1)", "10000"),
        ("id / 10.0", "10000"),
        ("rand()", "10000"),
        ("random()", "10000"),
        ("0.5", "0"),
        ("0.5", "-1"),
        ("0.5", "2147483648"),
        ("0.5", "NULL"),
        ("0.5", "1.5"),
        ("0.5", "id + 1"),
        ("0.5", "CAST(rand() * 10 + 1 AS BIGINT)"),
    ],
)
def test_approx_percentile_rejects_invalid_parameters(spark, function, percentage, accuracy):
    with pytest.raises(PySparkException):
        spark.sql(f"SELECT {function}(id, {percentage}, {accuracy}) FROM range(4)").collect()  # noqa: S608


def test_approx_percentile_maximum_accuracy(spark, function):
    result = spark.sql(f"SELECT {function}(id, array(0.0, 0.5, 1.0), 2147483647) AS value FROM range(4)")  # noqa: S608
    assert result.collect() == [Row(value=[0, 1, 3])]


@pytest.mark.skipif(is_jvm_spark(), reason="Sail preserves observations that Spark rounds through double")
@pytest.mark.parametrize(
    ("data_type", "values"),
    [
        ("bigint", ["9007199254740993", "9007199254740995", "9007199254740997"]),
        ("decimal(38,18)", ["12345678901234567890.123456789012345678", "12345678901234567890.123456789012345679"]),
    ],
)
def test_approx_percentile_preserves_exact_observations(spark, function, data_type, values):
    literals = ", ".join(f"(CAST('{value}' AS {data_type}))" for value in values)
    result = spark.sql(f"SELECT {function}(value, array(0.0, 0.5, 1.0)) AS value FROM VALUES {literals} AS t(value)")  # noqa: S608
    convert = int if data_type == "bigint" else Decimal
    expected = [convert(values[i]) for i in [0, (len(values) - 1) // 2, -1]]
    assert result.collect() == [Row(value=expected)]
