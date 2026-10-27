import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.errors import AnalysisException

# A Parquet column written with its Arrow dictionary type is read back as a dictionary-encoded
# column, which Spark sees as the plain type of its values.
ACCEPTED = {
    "string": pa.array(["0.25", "0.25", "1"]),
    "double": pa.array([0.25, 0.25, 1.0]),
    "int32": pa.array([1, 4, 4], pa.int32()),
}
REJECTED = {
    "boolean": pa.array([True, False, True]),
    "date": pa.array([datetime.date(2024, 1, 2)] * 3),
}


@pytest.fixture
def ansi(spark, request):
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", request.param)
    yield request.param
    spark.conf.set("spark.sql.ansi.enabled", previous)


def dictionary_view(spark, tmp_path, name, values):
    path = tmp_path / f"dictionary_{name}.parquet"
    pq.write_table(pa.table({"c": values.dictionary_encode()}), path)
    view = f"unary_double_dictionary_{name}"
    spark.read.parquet(str(path)).createOrReplaceTempView(view)
    return spark.table(view)


@pytest.mark.parametrize("ansi", ["false", "true"], indirect=True)
@pytest.mark.parametrize("name", list(ACCEPTED))
@pytest.mark.parametrize("fn", ["sin", "acos", "acosh", "ln", "sqrt", "expm1", "rint"])
def test_unary_double_accepts_dictionary_encoded_column(spark, tmp_path, ansi, name, fn):  # noqa: ARG001
    dataframe = dictionary_view(spark, tmp_path, name, ACCEPTED[name])
    plain = {"string": "STRING", "double": "DOUBLE", "int32": "INT"}[name]
    direct = dataframe.selectExpr(f"{fn}(CAST(c AS {plain})) AS r").collect()
    actual = dataframe.selectExpr(f"{fn}(c) AS r").collect()
    # Compare the rendering: `acosh(0.25)` is NaN, and NaN != NaN.
    assert repr(actual) == repr(direct)


@pytest.mark.parametrize("name", list(REJECTED))
@pytest.mark.parametrize("fn", ["sin", "sqrt", "rint"])
def test_unary_double_rejects_dictionary_encoded_column(spark, tmp_path, name, fn):
    dataframe = dictionary_view(spark, tmp_path, name, REJECTED[name])
    with pytest.raises(AnalysisException, match=r"DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE"):
        dataframe.selectExpr(f"{fn}(c) AS r").collect()
