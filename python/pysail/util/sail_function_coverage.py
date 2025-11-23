import re
from pathlib import Path
from markdown_it import MarkdownIt
from markdown_it.token import Token


def extract_function_coverage_from_md(path: str) -> list[list[list[str]]]:
    md_str = load_markdown(path)
    md = MarkdownIt("commonmark").enable("table")
    tokens = md.parse(md_str)
    tables = extract_tables_from_tokens(tokens)
    return postprocess_tables(tables)


def load_markdown(path: str) -> str:
    base = Path(path)
    if not base.exists():
        raise FileNotFoundError(f"Path not found: {path}")

    if base.suffix == ".md" and base.is_file():
        return base.read_text(encoding="utf-8")

    md_str = []
    for p in base.rglob("*.md"):
        if p.is_file():
            md_str.append(p.read_text(encoding="utf-8"))

    return "\n".join(md_str)


def extract_tables_from_tokens(tokens: list[Token]) -> list[list[list[str]]]:
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
    return re.findall(r"`([^`]*)`", text)


def decode_support_label(label: str) -> str:
    mapping = {
        ":white_check_mark:": "✅ supported",
        ":construction:": "🚧 in progress",
        ":x:": "❌ not supported",
    }
    return mapping.get(label.strip(), "❔ unknown")


def postprocess_tables(tables: list[list[list[str]]]) -> dict[str, str]:
    result = {}

    for table in tables:
        for row in table[1:]:  # skip header row
            if len(row) >= 2:
                keys = extract_function_name(row[0])
                value = decode_support_label(row[1])
                for key in keys:
                    result[key] = value
    return result
