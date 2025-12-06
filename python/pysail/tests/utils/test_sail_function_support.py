from inspect import cleandoc

from pysail.utils.sail_function_support import (
    _check_sail_pyspark_compatibility,
    _load_sail_support_data,
)


def test_load_sail_support_data():
    support_data = _load_sail_support_data()
    assert support_data[("pyspark.sql.functions", "count")] == "supported"
    assert support_data[("pyspark.sql.DataFrame", "join")] == "supported"


def test_check_sail_pyspark_compatibility(tmp_path):
    code = cleandoc(
        """
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql import DataFrame

        spark: SparkSession = SparkSession.builder.getOrCreate()
        df: DataFrame = spark.range(10)
        df.filter(F.col("value") > F.lit(5)).collect()
        """
    )
    path = tmp_path / "snippet.py"
    path.write_text(code, encoding="utf-8")
    expected = {
        ("pyspark.sql.DataFrame", "collect", "supported"): 1,
        ("pyspark.sql.DataFrame", "filter", "supported"): 1,
        ("pyspark.sql.functions", "col", "unknown"): 1,
        ("pyspark.sql.functions", "lit", "unknown"): 1,
        ("pyspark.sql.session.SparkSession", "getOrCreate", "unknown"): 1,
        ("pyspark.sql.session.SparkSession", "range", "supported"): 1,
    }
    assert _check_sail_pyspark_compatibility(tmp_path) == expected
