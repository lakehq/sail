import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.errors import AnalysisException
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType

from pysail.tests.spark.dataframe.udt import NamedPythonUDT, UnnamedPythonUDT


@pytest.mark.parametrize(
    ("source", "target"),
    [
        (VectorUDT(), UnnamedPythonUDT()),
        (UnnamedPythonUDT(), VectorUDT()),
        (UnnamedPythonUDT(), NamedPythonUDT()),
        (NamedPythonUDT(), UnnamedPythonUDT()),
    ],
)
def test_incompatible_udt_cast_is_analysis_error(spark, source, target):
    df = spark.createDataFrame([(None,)], StructType([StructField("value", source)]))
    with pytest.raises(AnalysisException):
        df.select(F.col("value").cast(target)).collect()


@pytest.mark.parametrize(
    ("data_type", "value"),
    [(VectorUDT(), Vectors.dense(1.0, 2.0)), (UnnamedPythonUDT(), None), (NamedPythonUDT(), None)],
)
def test_same_udt_cast_preserves_type_and_value(spark, data_type, value):
    schema = StructType([StructField("value", data_type)])
    df = spark.createDataFrame([(value,), (None,)], schema)
    result = df.select(F.col("value").cast(data_type))
    assert result.schema == schema
    assert result.collect() == [Row(value=value), Row(value=None)]
