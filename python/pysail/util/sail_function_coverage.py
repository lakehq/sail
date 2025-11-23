#!/usr/bin/env python3
import re
from pathlib import Path
from markdown_it import MarkdownIt
from markdown_it.token import Token


def load_markdown(path: str | list) -> str:
    """Load Markdown content from a file or directory of .md files."""

    if not isinstance(path, list):
        paths = [path]
    else:
        paths = path

    md_str = []
    for path in paths:
        base = Path(path)
        if not base.exists():
            raise FileNotFoundError("Path not found: %s", path)

        if base.suffix == ".md" and base.is_file():
            md_str.append(base.read_text(encoding="utf-8"))

        for p in base.rglob("*.md"):
            if p.is_file():
                md_str.append(p.read_text(encoding="utf-8"))

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
        for row in table[1:]:  # skip header row
            if len(row) >= 2:
                keys = extract_function_name(row[0])
                value = decode_md_support_label(row[1])
                for key in keys:
                    result[key] = value

    return result


def extract_function_coverage_from_md(path: str) -> dict[str, str]:
    """Parse Markdown and extract function coverage tables."""

    md_str = load_markdown(path)
    md = MarkdownIt("commonmark").enable("table")
    tokens = md.parse(md_str)
    tables = extract_tables_from_tokens(tokens)
    return postprocess_tables(tables)
