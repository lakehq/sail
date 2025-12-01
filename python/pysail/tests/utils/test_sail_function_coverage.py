from inspect import cleandoc

from markdown_it import MarkdownIt
from unittest.mock import patch
from pysail.utils.sail_function_coverage import (
    _check_sail_function_coverage,
    _extract_function_coverage_from_md,
    _extract_tables_from_tokens,
    _postprocess_tables,
)


def test_extract_tables():
    markdown_content = cleandoc(
        """
        # Some heading

        ## some section (table 1)

        | A | B |
        | - | - |
        | 1 | 3 |
        | 2 | 4 |

        ## some other section (table 2)

        | C | D | E |
        | - | - | - |
        | 1 | 3 | 5 |
        | 2 | 4 | 6 |

        # Some other heading (for noise)
        """
    )
    md = MarkdownIt("commonmark").enable("table")
    tokens = md.parse(markdown_content)

    expected = [
        [["A", "B"], ["1", "3"], ["2", "4"]],
        [["C", "D", "E"], ["1", "3", "5"], ["2", "4", "6"]],
    ]
    assert _extract_tables_from_tokens(tokens) == expected


def test_postprocess_table():
    table = [
        ["Function", "Support"],
        ["`function.len` (length)", ":white_check_mark:"],
        ["lower case: `lower`", ":x:"],
        ["`rtrim`", ":construction:"],
        ["foo", ":bar:"],
        ["`bar`", ":foo:"],
    ]

    expected = {
        "len": "✅ supported",
        "lower": "❌ not supported",
        "rtrim": "🚧 in progress",
        "bar": "❔ unknown",
    }

    assert _postprocess_tables([table]) == expected


def test_extract_function_coverage_from_md():
    def _md_content():
        return cleandoc(
            """
            # Scalar Functions (1)

            | Function | Support |
            | - | - |
            | `len` | :white_check_mark: |

            # Scalar Functions (2)

            | Function | Support |
            | - | - |
            | `lower` | :x: |
            | `rtrim` | :construction: |
            """
        )

    with patch(
        target="pysail.utils.sail_function_coverage._load_markdown",
        return_value=_md_content(),
    ):
        result = _extract_function_coverage_from_md(["https://ignored.example/md.md"])

    expected = {
        "len": "✅ supported",
        "lower": "❌ not supported",
        "rtrim": "🚧 in progress",
    }
    assert result == expected


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
        ("pyspark.sql.DataFrame", "collect", "✅ supported"): 1,
        ("pyspark.sql.DataFrame", "filter", "✅ supported"): 1,
        ("pyspark.sql.functions", "col", "❔ unknown"): 1,
        ("pyspark.sql.functions", "lit", "❔ unknown"): 1,
        ("pyspark.sql.session.SparkSession", "getOrCreate", "❔ unknown"): 1,
        ("pyspark.sql.session.SparkSession", "range", "✅ supported"): 1,
    }
    assert _check_sail_function_coverage(tmp_path) == expected
