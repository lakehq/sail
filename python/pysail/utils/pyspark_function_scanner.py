"""
Scan Python files and Jupyter notebooks for usage of functions from
specified modules (TARGET_MODULES). Returns total count of function
calls per module and function name.
"""

from __future__ import annotations

import ast
import json
import logging
from collections import Counter
from typing import TYPE_CHECKING

import jedi

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

# Modules to track
TARGET_MODULES: frozenset[str] = frozenset(
    {
        "pyspark.sql.Column",
        "pyspark.sql.DataFrame",
        "pyspark.sql.functions",
        "pyspark.sql.session.SparkSession",
        "pyspark.sql.types.StructType",
        "pyspark.sql.Window",
    }
)
TARGET_MODULES_LOOKUP_HELPER = {t.lower(): t for t in TARGET_MODULES}

SUPPORTED_EXTENSIONS = {".py", ".ipynb"}
MAGIC_PREFIXES = ("%", "!", "?")


class CallSiteLocator(ast.NodeVisitor):
    """
    AST Visitor that only cares about finding the coordinates (Line, Column)
    of function calls. We hand these coordinates to Jedi for resolution.
    """

    def __init__(self):
        # Using a set to avoid duplicate coordinates
        self.locations: set[tuple[int, int]] = set()

    def visit_Call(self, node: ast.Call) -> None:
        """
        Identify the 'hotspot' of the function call.
        """
        func = node.func

        # 1. Prefer the end of the expression (works best for Attributes like `df.select`)
        # Jedi usually prefers the cursor at the end of the method name.
        # end_col_offset is exclusive, so we subtract 1 to land on the last char.
        if getattr(func, "end_lineno", None) is not None and getattr(func, "end_col_offset", None) is not None:
            self.locations.add((func.end_lineno, max(func.end_col_offset - 1, 0)))

        # 2. Fallback: Start of the expression
        # Essential for ast.Name (e.g., `col(...)`) or older Python versions
        # without end_lineno info.
        elif hasattr(func, "lineno") and hasattr(func, "col_offset"):
            self.locations.add((func.lineno, func.col_offset))

        self.generic_visit(node)


def _resolve_calls_with_jedi(source: str, file_path: Path | None = None) -> Counter[tuple[str, str]]:
    """
    1. Parse code with AST to find where functions are called.
    2. Ask Jedi to infer what is being called at those locations.
    """
    # 1. Fast scan for function call locations using AST
    try:
        tree = ast.parse(source)
    except SyntaxError:
        logger.exception("Syntax error in %s (or 'source')", file_path)
        return Counter()

    locator = CallSiteLocator()
    locator.visit(tree)

    if not locator.locations:
        return Counter()

    # 2. Initialize Jedi Script
    try:
        script = jedi.Script(code=source, path=file_path)
    except (RuntimeError, OSError, ValueError, TypeError):
        logger.exception("Jedi initialization failed for %s", file_path)
        return Counter()

    counts: Counter[tuple[str, str]] = Counter()

    # 3. Infer types at specific coordinates
    for line, col in locator.locations:
        try:
            definitions = script.infer(line, col)
        except (RuntimeError, OSError, ValueError, TypeError) as e:
            logger.warning("Jedi inference failed for line %i, col %i in file %s: %s", line, col, file_path, e)
            # skip this location, do not abort the whole file
            continue

        for definition in definitions:
            full_name = definition.full_name
            if not full_name:
                continue

            full_name_lower = full_name.lower()
            matching_module = None
            for (
                target_module_lower,
                target_module,
            ) in TARGET_MODULES_LOOKUP_HELPER.items():
                if full_name_lower.startswith(target_module_lower):
                    matching_module = target_module
                    break
            if matching_module:
                counts[(matching_module, full_name.split(".")[-1])] += 1

    return counts


def _extract_notebook_code(content: str) -> str:
    """Extract Python code from Jupyter notebook JSON."""
    try:
        nb = json.loads(content)
    except json.JSONDecodeError:
        return ""

    lines = []
    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue

        source = cell.get("source", [])
        cell_lines = source if isinstance(source, list) else source.splitlines(keepends=True)

        for line in cell_lines:
            # Comment out magic commands (%, !, ?) to keep line numbers aligned
            # but prevent syntax errors
            if line.lstrip().startswith(MAGIC_PREFIXES):
                lines.append(f"# {line}")
            else:
                lines.append(line)

        # Add a newline after each cell to ensure separation
        lines.append("\n")

    return "".join(lines)


def _scan_file(path: Path) -> Counter[tuple[str, str]]:
    """Scan a single .py or .ipynb file."""
    try:
        content = path.read_text(encoding="utf-8")

        if path.suffix == ".ipynb":
            content = _extract_notebook_code(content)

        if not content.strip():
            return Counter()

        return _resolve_calls_with_jedi(content, path)

    except OSError:
        logger.exception("Failed to read %s", path)
        return Counter()


def _scan_directory(base: Path) -> Counter[tuple[str, str]]:
    """Recursively scan directory for .py and .ipynb files."""
    total: Counter[tuple[str, str]] = Counter()

    # Collect all files first to show progress if needed,
    # or just iterate directly.
    if base.is_dir():
        files = [p for p in base.rglob("*") if p.suffix in SUPPORTED_EXTENSIONS and p.is_file()]
    elif base.is_file() and base.suffix in SUPPORTED_EXTENSIONS:
        files = [base]
    else:
        msg = f"Path is not a directory or supported file: {base}"
        raise ValueError(msg)

    total_files = len(files)
    logger.info("Found %d files to scan.", total_files)
    if total_files > 20:  # noqa: PLR2004
        logger.info("Scanning is going to take some time...")

    for i, path in enumerate(files, 1):
        if i % 10 == 0:
            logger.info("Scanning file %d/%d ...", i, total_files)
        total.update(_scan_file(path))

    return total
