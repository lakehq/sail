import pandas as pd
import pyspark
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.functions import array, col, lit, udf, udtf
from pyspark.sql.types import ArrayType, DataType, IntegerType, LongType, Row, StringType, StructType

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


def test_udtf_row_output_invalid_scalar_raises_pickle_exception():
    from pyspark.errors.exceptions.connect import PickleException

    @udtf(returnType="x: boolean")
    class RowOutputUDTF:
        def eval(self):
            yield (Row(a=0, b=1.1, c=2),)

    with pytest.raises(PickleException, match="PickleException"):
        RowOutputUDTF().collect()


@pytest.mark.skipif(
    tuple(int(x) for x in pyspark.__version__.split(".")[:2]) < (4, 1),
    reason="Arrow UDTF tests require PySpark 4.1+",
)
def test_arrow_udtf_type_conversion_error_class_is_preserved():
    from pyspark.errors.exceptions.connect import PythonException

    @udtf(returnType="x: boolean", useArrow=True)
    class ArrowOutputUDTF:
        def eval(self):
            yield (1,)

    with pytest.raises(PythonException, match="UDTF_ARROW_TYPE_CONVERSION_ERROR"):
        ArrowOutputUDTF().collect()


@pytest.mark.skipif(
    tuple(int(x) for x in pyspark.__version__.split(".")[:2]) < (4, 1),
    reason="UDTF analyze tests require PySpark 4.1+",
)
def test_udtf_analyze_preserves_array_type():
    from pyspark.sql.udtf import AnalyzeArgument, AnalyzeResult

    @udtf
    class AnalyzeArrayUDTF:
        @staticmethod
        def analyze(a: AnalyzeArgument) -> AnalyzeResult:
            assert isinstance(a, AnalyzeArgument)
            assert isinstance(a.dataType, DataType)
            assert a.isTable is False
            return AnalyzeResult(StructType().add("a", a.dataType))

        def eval(self, a):
            yield (a,)

    df = AnalyzeArrayUDTF(array(lit(1), lit(2), lit(3)))
    assert df.schema == StructType().add("a", ArrayType(IntegerType(), containsNull=False))
    assert df.collect() == [Row(a=[1, 2, 3])]


@pytest.mark.skipif(
    tuple(int(x) for x in pyspark.__version__.split(".")[:2]) < (4, 1),
    reason="UDTF table arguments require PySpark 4.1+",
)
def test_udtf_analyze_marks_table_argument(spark):
    from pyspark.sql.udtf import AnalyzeArgument, AnalyzeResult

    @udtf
    class AnalyzeTableUDTF:
        @staticmethod
        def analyze(a: AnalyzeArgument) -> AnalyzeResult:
            assert isinstance(a, AnalyzeArgument)
            assert isinstance(a.dataType, StructType)
            assert a.isTable is True
            return AnalyzeResult(StructType().add("a", a.dataType[0].dataType))

        def eval(self, a: Row):
            yield (a["id"],)

    spark.udtf.register("analyze_table_udtf", AnalyzeTableUDTF)
    df = spark.sql("SELECT * FROM analyze_table_udtf(TABLE (SELECT id FROM range(0, 3)))")
    assert df.schema["a"].dataType == LongType()
    assert df.collect() == [Row(a=0), Row(a=1), Row(a=2)]
