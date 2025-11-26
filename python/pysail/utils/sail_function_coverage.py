from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING

from markdown_it import MarkdownIt

from python.pysail.utils.pyspark_function_scanner import scan_directory

if TYPE_CHECKING:
    from markdown_it.token import Token


def load_markdown(path: str | list) -> str:
    """Load Markdown content from a file or directory of .md files."""

    paths = [path] if not isinstance(path, list) else path

    md_str = []
    for path in paths:
        base = Path(path)
        if not base.exists():
            msg = "Path not found: %s"
            raise FileNotFoundError(msg, path)

        if base.suffix == ".md" and base.is_file():
            md_str.append(base.read_text(encoding="utf-8"))

        md_str.extend(
            p.read_text(encoding="utf-8") for p in base.rglob("*.md") if p.is_file()
        )

    return "\n".join(md_str)


def extract_tables_from_tokens(tokens: list[Token]) -> list[list[list[str]]]:
    """Extract tables as lists of rows and cells from markdown-it tokens."""

    tables = []
    current_table = []
    current_row = []

    # Flags to track where we are in the structure
    inside_table = False
    inside_row = False

    for token in tokens:
        if token.type == "table_open":
            inside_table = True
            current_table = []
        elif token.type == "table_close":
            inside_table = False
            tables.append(current_table)
        elif token.type == "tr_open":
            inside_row = True
            current_row = []
        elif token.type == "tr_close":
            inside_row = False
            current_table.append(current_row)
        elif token.type == "inline" and inside_row and inside_table:
            # In markdown-it, the text content of a cell (th/td)
            # is contained within an 'inline' token.
            current_row.append(token.content)

    return tables


def extract_function_name(text: str) -> list[str]:
    """
    Extract function names from markdown table cell text.
    If functions are qualified (e.g., module.function), only the function name is returned.
    """
    functions = re.findall(r"`([^`]*)`", text)
    return [func.split(".")[-1].replace("()", "") for func in functions]


def decode_md_support_label(label: str) -> str:
    """Decode markdown emoji-style support labels into readable status strings."""

    stripped_label = label.strip()
    mappings = {
        ":white_check_mark:": "✅ supported",
        ":construction:": "🚧 in progress",
        ":x:": "❌ not supported",
    }

    for icon, new_label in mappings.items():
        if icon in stripped_label:
            return new_label

    return "❔ unknown"


def postprocess_tables(tables: list[list[list[str]]]) -> dict[str, str]:
    """Map parsed tables to function support statuses."""

    result = {}

    for table in tables:
        for fn_cell, label_cell in table[1:]:  # skip header row
            for key in extract_function_name(fn_cell):
                result[key] = decode_md_support_label(label_cell)

    return result


def extract_function_coverage_from_md(path: str) -> dict[str, str]:
    """Parse Markdown and extract function coverage tables."""

    md_str = load_markdown(path)
    md = MarkdownIt("commonmark").enable("table")
    tokens = md.parse(md_str)
    tables = extract_tables_from_tokens(tokens)
    return postprocess_tables(tables)


def check_sail_function_coverage(path: str) -> Counter[tuple[str, str, str]]:
    """Scan a directory for PySpark function usage and cross-refernce it with sail docs for compatibility."""
    sail_coverage = {
        **{
            ("pyspark.sql.functions", function_name): label
            for function_name, label in extract_function_coverage_from_md(
                [
                    Path("docs/guide/functions/aggregate.md"),
                    Path("docs/guide/functions/generator.md"),
                    Path("docs/guide/functions/scalar.md"),
                    Path("docs/guide/functions/window.md"),
                ]
            ).items()
        },
        **{
            ("pyspark.sql.DataFrame", function_name): label
            for function_name, label in extract_function_coverage_from_md(
                Path("docs/guide/dataframe/features.md")
            ).items()
        },
        **{
            ("pyspark.sql.session.SparkSession", function_name): label
            for function_name, label in extract_function_coverage_from_md(
                Path("docs/guide/functions/table.md")
            ).items()
        },
    }

    pyspark_function_usage = scan_directory(Path(path))

    counter: Counter[tuple[str, str, str]] = Counter()
    for key, count in pyspark_function_usage.items():
        counter[(*key, sail_coverage.get(key, "❔ unknown"))] = count

    return counter


def format_output(counts: Counter[tuple[str, str, str]], fmt: str) -> str:
    """Format results as text, CSV or JSON."""
    if fmt == "json":
        results = [
            {
                "module": mod,
                "function": func,
                "sail_support_status": support,
                "count": cnt,
            }
            for (mod, func, support), cnt in counts.most_common()
        ]
        return json.dumps(results, indent=2)

    if fmt == "csv":
        results = [
            (mod, func, support, cnt)
            for (mod, func, support), cnt in counts.most_common()
        ]
        header = [("module", "function", "sail_support_status", "count")]

        return "\n".join(
            ";".join(str(item) for item in row) for row in header + results
        )

    if not counts:
        return "No relevant function calls found."

    lines = ["", "=== Usage Counts ==="]
    # Sort by module name, then descending by call count
    sorted_items = sorted(counts.items(), key=lambda x: (x[0][0], -x[1]))

    current_mod = None
    for (mod, func, support), cnt in sorted_items:
        if mod != current_mod:
            lines.append(f"\n[{mod}]")
            current_mod = mod
        lines.append(f"  .{func}: {cnt} ({support})")

    return "\n".join(lines)
