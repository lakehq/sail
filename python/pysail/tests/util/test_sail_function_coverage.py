from inspect import cleandoc

from markdown_it import MarkdownIt

from pysail.util.sail_function_coverage import (
    check_sail_function_coverage,
    extract_tables_from_tokens,
    postprocess_tables,
    extract_function_coverage_from_md,
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
    assert expected == extract_tables_from_tokens(tokens)


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

    assert expected == postprocess_tables([table])


def test_extract_function_coverage_from_md(tmp_path):
    md_file_1_content = cleandoc(
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
    md_file_1 = tmp_path / "scalar.md"
    md_file_1.write_text(md_file_1_content, encoding="utf-8")

    expected = {
        "len": "✅ supported",
        "lower": "❌ not supported",
        "rtrim": "🚧 in progress",
    }
    # result = extract_function_coverage_from_md(tmp_path)
    assert expected == extract_function_coverage_from_md(tmp_path)


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
    assert expected == check_sail_function_coverage(tmp_path)
