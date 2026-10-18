import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def dictionary_string_view(spark, tmp_path):
    # A Parquet column written with its Arrow dictionary type is read back as a dictionary-encoded
    # string, which Spark sees as a plain STRING.
    path = tmp_path / "dictionary_string.parquet"
    pq.write_table(pa.table({"c": pa.array(["0.5", "0.5", "1"]).dictionary_encode()}), path)
    spark.read.parquet(str(path)).createOrReplaceTempView("unary_double_dictionary_string")
    return "unary_double_dictionary_string"


@pytest.mark.parametrize("ansi", ["false", "true"])
@pytest.mark.parametrize("fn", ["sin", "acos", "acosh", "ln"])
def test_unary_double_accepts_dictionary_encoded_string(spark, dictionary_string_view, fn, ansi):
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    direct = spark.sql(f"SELECT {fn}(CAST(c AS STRING)) AS r FROM {dictionary_string_view}").collect()
    actual = spark.sql(f"SELECT {fn}(c) AS r FROM {dictionary_string_view}").collect()
    # Compare the rendering: `acosh(0.5)` is NaN, and NaN != NaN.
    assert repr(actual) == repr(direct)
