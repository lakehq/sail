import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.sql import Row


@pytest.fixture
def numbers(spark):
    return spark.createDataFrame([(1,), (3,), (12,), (-1,), (None,)], "n int")


def test_conv_reads_integer_columns_as_digits_of_the_base(numbers):
    df = numbers.select(F.conv("n", 2, 16).alias("hex"))
    assert df.collect() == [
        Row(hex="1"),
        Row(hex="0"),
        Row(hex="1"),
        Row(hex="FFFFFFFFFFFFFFFF"),
        Row(hex=None),
    ]


def test_conv_stops_at_the_first_invalid_digit(spark):
    df = spark.createDataFrame([("12",), ("1x1",), ("zz",), ("",), (None,)], "s string")
    actual = df.select(F.conv("s", 16, 10).alias("n"))
    assert actual.collect() == [Row(n="18"), Row(n="1"), Row(n="0"), Row(n=None), Row(n=None)]


def test_conv_of_negative_numbers(spark):
    df = spark.sql("SELECT conv('-10', 16, -10) a, conv('10', 16, -10) b, conv('-1', 10, 16) c")
    assert df.collect() == [Row(a="-16", b="16", c="FFFFFFFFFFFFFFFF")]


def test_conv_with_invalid_bases_is_null(spark):
    df = spark.sql("SELECT conv('1', 1, 10) a, conv('1', 10, 37) b, conv('1', 10, -37) c")
    assert df.collect() == [Row(a=None, b=None, c=None)]
