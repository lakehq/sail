from collections import Counter
from inspect import cleandoc

from pysail.util.pyspark_function_scanner import _parse_and_scan


def test_import_module_with_alias():
    code = cleandoc(
        """
        import pyspark.sql.functions as F
        from pyspark.sql import window as W
        F.col("a")
        F.when(True, 1)
        W.partitionBy("x")
        """
    )
    result = _parse_and_scan(code)
    assert result == Counter(
        {
            ("pyspark.sql.functions", "col"): 1,
            ("pyspark.sql.functions", "when"): 1,
            ("pyspark.sql.window", "partitionBy"): 1,
        }
    )


def test_import_function_directly():
    code = cleandoc(
        """
        from pyspark.sql.functions import lit
        lit("x")
        """
    )
    result = _parse_and_scan(code)
    assert result == Counter(
        {
            ("pyspark.sql.functions", "lit"): 1,
        }
    )


def test_import_whole_module():
    code = cleandoc(
        """
        from pyspark.sql import functions
        from pyspark.sql import Window
        from pyspark.sql import window
        functions.lit("x")
        Window.partitionBy("x")
        window.orderBy("y")
        """
    )
    result = _parse_and_scan(code)
    assert result == Counter(
        {
            ("pyspark.sql.functions", "lit"): 1,
            ("pyspark.sql.window", "partitionBy"): 1,
            ("pyspark.sql.window", "orderBy"): 1,
        }
    )
