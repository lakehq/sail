from collections import Counter
from inspect import cleandoc

from pysail.util.pyspark_function_scanner import scan_file


def test_import_module_with_alias(tmp_path):
    code = cleandoc(
        """
        import pyspark.sql.functions as F
        import pyspark.sql.window as W
        F.col("a")
        F.when(True, 1)
        W.Window.partitionBy("x")
        """
    )
    path = tmp_path / "snippet.py"
    path.write_text(code, encoding="utf-8")
    assert Counter(
        {
            ("pyspark.sql.functions", "col"): 1,
            ("pyspark.sql.functions", "when"): 1,
            ("pyspark.sql.window", "partitionBy"): 1,
        }
    ) == scan_file(path)


def test_import_function_directly(tmp_path):
    code = cleandoc(
        """
        from pyspark.sql.functions import lit
        lit("x")
        """
    )
    path = tmp_path / "snippet.py"
    path.write_text(code, encoding="utf-8")
    assert Counter(
        {
            ("pyspark.sql.functions", "lit"): 1,
        }
    ) == scan_file(path)


def test_import_whole_module(tmp_path):
    code = cleandoc(
        """
        from pyspark.sql import functions
        from pyspark.sql import Window
        functions.lit("x")
        Window.partitionBy("x")
        Window.orderBy("y")
        """
    )
    path = tmp_path / "snippet.py"
    path.write_text(code, encoding="utf-8")
    assert Counter(
        {
            ("pyspark.sql.functions", "lit"): 1,
            ("pyspark.sql.window", "partitionBy"): 1,
            ("pyspark.sql.window", "orderBy"): 1,
        }
    ) == scan_file(path)


def test_import_star_wildcard(tmp_path):
    code = cleandoc(
        """
        from pyspark.sql.functions import *
        lit("x")
        col("x")
        """
    )
    path = tmp_path / "snippet.py"
    path.write_text(code, encoding="utf-8")
    assert Counter(
        {
            ("pyspark.sql.functions", "lit"): 1,
            ("pyspark.sql.functions", "col"): 1,
        }
    ) == scan_file(path)


def test_structtype(tmp_path):
    code = cleandoc(
        """
        from pyspark.sql.types import StructType
        import json

        json_str = "..."

        scheme = StructType.fromJson(json.loads(json_str))
        scheme.simpleString()
        scheme.jsonValue()

        """
    )
    path = tmp_path / "snippet.py"
    path.write_text(code, encoding="utf-8")
    assert Counter(
        {
            # ("pyspark.sql.types.StructType", "fromJson"): 1,  # is not recognized yet...
            ("pyspark.sql.types.StructType", "simpleString"): 1,
            ("pyspark.sql.types.StructType", "jsonValue"): 1,
        }
    ) == scan_file(path)


def test_column(tmp_path):
    code = cleandoc(
        """
        from pyspark.sql import SparkSession
        import pyspark.sql.functions as F

        spark: SparkSession = SparkSession.builder.appName("TestApp").getOrCreate()
        df = spark.range(10).toDF("value")
        df.filter(df.height.isNotNull()).collect()

        df.select(
            df["value"].endswith("y"),
            df["value"].cast("STRING"),
            F.col("value").eqNullSafe(42),
        ).show()
        """
    )
    path = tmp_path / "snippet.py"
    path.write_text(code, encoding="utf-8")
    assert Counter(
        {
            ("pyspark.sql.column", "cast"): 1,
            ("pyspark.sql.column", "endswith"): 1,
            ("pyspark.sql.column", "eqNullSafe"): 1,
            ("pyspark.sql.column", "isNotNull"): 1,
            ("pyspark.sql.dataframe", "collect"): 1,
            ("pyspark.sql.dataframe", "filter"): 1,
            ("pyspark.sql.dataframe", "select"): 1,
            ("pyspark.sql.dataframe", "show"): 1,
            ("pyspark.sql.dataframe", "toDF"): 1,
            ("pyspark.sql.functions", "col"): 1,
        }
    ) == scan_file(path)
