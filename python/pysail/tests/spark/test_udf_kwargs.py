import pandas as pd
import pyspark
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.functions import col, lit, udf, udtf
from pyspark.sql.types import IntegerType, Row, StringType

pytestmark = pytest.mark.skipif(
    int(pyspark.__version__.split(".")[0]) < 4,  # noqa: PLR2004
    reason="UDF/UDTF keyword arguments require PySpark 4+",
)


@pytest.fixture(scope="module")
def df(spark):
    return spark.createDataFrame(
        [Row(a=1, b=2), Row(a=3, b=4)],
        schema="a integer, b integer",
    )


# --- UDF keyword argument tests ---


@pytest.fixture(scope="module")
def udf_add():
    @udf(IntegerType())
    def add(x, y):
        return x + y

    return add


@pytest.fixture(scope="module")
def udf_concat():
    @udf(StringType())
    def concat(a, b):
        return str(a) + str(b)

    return concat


def test_udf_all_kwargs(df, udf_add):
    assert_frame_equal(
        df.select(udf_add(x=col("a"), y=col("b")).alias("sum")).sort("sum").toPandas(),
        pd.DataFrame({"sum": [3, 7]}, dtype="int32"),
    )


def test_udf_mixed_args_and_kwargs(df, udf_add):
    assert_frame_equal(
        df.select(udf_add(col("a"), y=col("b")).alias("sum")).sort("sum").toPandas(),
        pd.DataFrame({"sum": [3, 7]}, dtype="int32"),
    )


def test_udf_kwargs_reversed_order(df, udf_concat):
    """Keyword arguments matched by name regardless of order."""
    assert_frame_equal(
        df.select(udf_concat(b=col("b"), a=col("a")).alias("r")).sort("r").toPandas(),
        pd.DataFrame({"r": ["12", "34"]}),
    )


# --- UDTF keyword argument tests ---


@pytest.fixture(scope="module")
def udtf_point():
    @udtf(returnType="x: int, y: int")
    class PointUDTF:
        def eval(self, x, y):
            yield x, y

    return PointUDTF


@pytest.fixture(scope="module")
def udtf_concat():
    @udtf(returnType="result: string")
    class ConcatUDTF:
        def eval(self, a, b):
            yield (str(a) + str(b),)

    return ConcatUDTF


def test_udtf_all_kwargs(udtf_point):
    assert_frame_equal(
        udtf_point(x=lit(1), y=lit(2)).toPandas(),
        pd.DataFrame({"x": [1], "y": [2]}, dtype="int32"),
    )


def test_udtf_mixed_args_and_kwargs(udtf_point):
    assert_frame_equal(
        udtf_point(lit(1), y=lit(2)).toPandas(),
        pd.DataFrame({"x": [1], "y": [2]}, dtype="int32"),
    )


def test_udtf_kwargs_reversed_order(udtf_concat):
    """Keyword arguments matched by name regardless of order."""
    assert_frame_equal(
        udtf_concat(b=lit("world"), a=lit("hello")).toPandas(),
        pd.DataFrame({"result": ["helloworld"]}),
    )
