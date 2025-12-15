from collections import Counter
from inspect import cleandoc

from pysail.utils.pyspark_function_scanner import _scan_directory, _scan_file


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
    assert _scan_file(path) == Counter(
        {
            ("pyspark.sql.functions", "col"): 1,
            ("pyspark.sql.functions", "when"): 1,
            ("pyspark.sql.Window", "partitionBy"): 1,
        }
    )


def test_import_function_directly(tmp_path):
    code = cleandoc(
        """
        from pyspark.sql.functions import lit
        lit("x")
        """
    )
    path = tmp_path / "snippet.py"
    path.write_text(code, encoding="utf-8")
    assert _scan_file(path) == Counter(
        {
            ("pyspark.sql.functions", "lit"): 1,
        }
    )


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
    assert _scan_file(path) == Counter(
        {
            ("pyspark.sql.functions", "lit"): 1,
            ("pyspark.sql.Window", "partitionBy"): 1,
            ("pyspark.sql.Window", "orderBy"): 1,
        }
    )


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
    assert _scan_file(path) == Counter(
        {
            ("pyspark.sql.functions", "lit"): 1,
            ("pyspark.sql.functions", "col"): 1,
        }
    )


def test_infer_structtype(tmp_path):
    code = cleandoc(
        """
        from pyspark.sql.types import StructType
        import json

        json_str = "..."

        schema = StructType.fromJson(json.loads(json_str))
        schema.simpleString()
        schema.jsonValue()

        """
    )
    path = tmp_path / "snippet.py"
    path.write_text(code, encoding="utf-8")
    assert _scan_file(path) == Counter(
        {
            # ("pyspark.sql.types.StructType", "fromJson"): 1,  # is not recognized yet...
            ("pyspark.sql.types.StructType", "simpleString"): 1,
            ("pyspark.sql.types.StructType", "jsonValue"): 1,
        }
    )


def test_infer_column(tmp_path):
    code = cleandoc(
        """
        from pyspark.sql import SparkSession
        import pyspark.sql.functions as F

        spark: SparkSession = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(
            schema="id INTEGER, name STRING, value INTEGER",
            data=[
                (1, "alice", 10),
                (2, "bob", 20),
            ],
        )

        df.filter(
            F.col("name").like("a%")
            | (F.col("name").substr(1, 3) == F.lit("bob"))
            | F.col("id").isin([1, 5])
        ).show()
        """
    )
    path = tmp_path / "snippet.py"
    path.write_text(code, encoding="utf-8")
    assert _scan_file(path) == Counter(
        {
            ("pyspark.sql.Column", "isin"): 1,
            ("pyspark.sql.Column", "like"): 1,
            ("pyspark.sql.Column", "substr"): 1,
            ("pyspark.sql.DataFrame", "filter"): 1,
            ("pyspark.sql.DataFrame", "show"): 1,
            ("pyspark.sql.functions", "col"): 3,
            ("pyspark.sql.functions", "lit"): 1,
            ("pyspark.sql.session.SparkSession", "createDataFrame"): 1,
            ("pyspark.sql.session.SparkSession", "getOrCreate"): 1,
        }
    )


def test_infer_dataframe_agg(tmp_path):
    code = cleandoc(
        """
        from pyspark.sql import functions as F
        from pyspark.sql import SparkSession

        spark: SparkSession = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
        df.agg({"age": "max"}).show()

        df.agg(
            F.coalesce(F.sum("age"), F.lit(0))
        ).show()
        """
    )
    path = tmp_path / "snippet.py"
    path.write_text(code, encoding="utf-8")
    assert _scan_file(path) == Counter(
        {
            ("pyspark.sql.DataFrame", "agg"): 2,
            ("pyspark.sql.DataFrame", "show"): 2,
            ("pyspark.sql.functions", "sum"): 1,
            ("pyspark.sql.functions", "lit"): 1,
            ("pyspark.sql.functions", "coalesce"): 1,
            ("pyspark.sql.session.SparkSession", "createDataFrame"): 1,
            ("pyspark.sql.session.SparkSession", "getOrCreate"): 1,
        }
    )


def test_scan_directory(tmp_path):
    code = cleandoc(
        """
        import pyspark.sql.functions as F
        import pyspark.sql.window as W
        F.col("a")
        F.when(True, 1)
        W.Window.partitionBy("x")
        """
    )
    for i in range(3):
        path = tmp_path / f"snippet_{i}.py"
        path.write_text(code, encoding="utf-8")

    assert _scan_directory(tmp_path) == Counter(
        {
            ("pyspark.sql.functions", "col"): 3,
            ("pyspark.sql.functions", "when"): 3,
            ("pyspark.sql.Window", "partitionBy"): 3,
        }
    )
