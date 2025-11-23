from inspect import cleandoc

from markdown_it import MarkdownIt

from pysail.util.sail_function_coverage import (
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

    assert [
        [["A", "B"], ["1", "3"], ["2", "4"]],
        [["C", "D", "E"], ["1", "3", "5"], ["2", "4", "6"]],
    ] == extract_tables_from_tokens(tokens)


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
