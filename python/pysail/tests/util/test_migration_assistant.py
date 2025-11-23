from inspect import cleandoc
from pysail.util.migration_assistant import check_sail_function_coverage


def test_check_sail_function_coverage(tmp_path):
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
        ("pyspark.sql.dataframe", "collect", "✅ supported"): 1,
        ("pyspark.sql.dataframe", "filter", "✅ supported"): 1,
        ("pyspark.sql.functions", "col", "❔ unknown"): 1,
        ("pyspark.sql.functions", "lit", "❔ unknown"): 1,
    }
    assert expected == check_sail_function_coverage(tmp_path)
