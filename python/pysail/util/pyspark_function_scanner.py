#!/usr/bin/env python3
"""
Scan Python files and Jupyter notebooks for usage of functions from
specified modules (TARGET_MODULES). Returns total count of function
calls per module and function name.
"""

import argparse
import ast
import json
import logging
from collections import Counter
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# Modules to track (stored lowercase for case-insensitive matching)
TARGET_MODULES: frozenset[str] = frozenset(
    {
        "pyspark.sql.functions",
        "pyspark.sql.window",
    }
)

# Parent packages for "from X import module" style imports
TARGET_PACKAGES: frozenset[str] = frozenset(
    m.rsplit(".", 1)[0] for m in TARGET_MODULES if "." in m
)

SUPPORTED_EXTENSIONS = {".py", ".ipynb"}
MAGIC_PREFIXES = ("%", "!", "?")


class PysparkFunctionScanner(ast.NodeVisitor):
    """AST visitor tracking imports and function calls for target modules."""

    __slots__ = ("module_aliases", "direct_imports", "calls")

    def __init__(self):
        self.module_aliases: dict[str, str] = {}  # alias -> full module
        self.direct_imports: dict[str, tuple[str, str]] = (
            {}
        )  # local name -> (module, func)
        self.calls: Counter[tuple[str, str]] = Counter()

    def _register_module_alias(self, full_name: str, alias: str | None = None) -> None:
        """Register an alias for a tracked module."""
        name_lower = full_name.lower()
        if name_lower in TARGET_MODULES:
            local_name = alias or full_name.rsplit(".", 1)[-1]
            self.module_aliases[local_name] = name_lower

    def visit_Import(self, node: ast.Import) -> None:
        # import pyspark.sql.functions as F
        for alias in node.names:
            self._register_module_alias(alias.name, alias.asname)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if not node.module:
            return self.generic_visit(node)

        module_lower = node.module.lower()

        if module_lower in TARGET_MODULES:
            # from pyspark.sql.functions import col, lit as L
            for alias in node.names:
                if alias.name == "*":
                    logging.warning(
                        "Wildcard import from %s; calls may be missed", node.module
                    )
                    continue
                local = alias.asname or alias.name
                self.direct_imports[local] = (module_lower, alias.name)

        elif module_lower in TARGET_PACKAGES:
            # from pyspark.sql import functions as F
            for alias in node.names:
                full_module = f"{module_lower}.{alias.name.lower()}"
                if full_module in TARGET_MODULES:
                    self._register_module_alias(full_module, alias.asname or alias.name)

        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func

        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            # F.col(...) where F is module alias
            module = self.module_aliases.get(func.value.id)
            if module:
                self.calls[(module, func.attr)] += 1

        elif isinstance(func, ast.Name):
            # col(...) where col was directly imported
            origin = self.direct_imports.get(func.id)
            if origin:
                self.calls[origin] += 1

        self.generic_visit(node)


def parse_and_scan_code(source: str) -> Counter[tuple[str, str]]:
    """Parse source code and return function call counts."""
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        logging.error("Syntax error: %s", e)
        return Counter()

    scanner = PysparkFunctionScanner()
    scanner.visit(tree)
    return scanner.calls


def extract_notebook_code(content: str) -> str:
    """Extract Python code from Jupyter notebook JSON."""
    nb = json.loads(content)
    lines = []

    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        source = cell.get("source", [])
        code = "".join(source) if isinstance(source, list) else source
        lines.append(code)

    # Comment out magic commands to avoid syntax errors
    return "\n".join(
        f"# {ln}" if ln.lstrip().startswith(MAGIC_PREFIXES) else ln for ln in lines
    )


def scan_file(path: Path) -> Counter[tuple[str, str]]:
    """Scan a single .py or .ipynb file."""
    try:
        content = path.read_text(encoding="utf-8")

        if path.suffix == ".ipynb":
            content = extract_notebook_code(content)

        return parse_and_scan_code(content)

    except (OSError, json.JSONDecodeError) as e:
        logging.error("Failed to read %s: %s", path, e)
        return Counter()


def scan_directory(base: Path) -> Counter[tuple[str, str]]:
    """Recursively scan directory for .py and .ipynb files."""
    total: Counter[tuple[str, str]] = Counter()

    for path in base.rglob("*"):
        if path.suffix in SUPPORTED_EXTENSIONS:
            total.update(scan_file(path))

    return total


def format_output(counts: Counter[tuple[str, str]], fmt: str) -> str:
    """Format results as text or JSON."""
    if fmt == "json":
        results = [
            {"module": mod, "function": func, "count": cnt}
            for (mod, func), cnt in counts.most_common()
        ]
        return json.dumps(results, indent=2)

    if not counts:
        return f"No function calls found for {', '.join(sorted(TARGET_MODULES))}."

    lines = ["", "=== Usage Counts ==="]
    for (mod, func), cnt in counts.most_common():
        lines.append(f"  {mod}.{func}: {cnt}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=f"Scan Python files for usage of {', '.join(sorted(TARGET_MODULES))}."
    )
    parser.add_argument("directory", type=Path, help="Directory to scan recursively")
    parser.add_argument(
        "-o",
        "--output",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    args = parser.parse_args()

    if not args.directory.exists():
        raise SystemExit(f"Directory not found: {args.directory}")

    logging.info("Scanning: %s", args.directory)
    counts = scan_directory(args.directory)
    logging.info("Scan complete.")

    print(format_output(counts, args.output))


if __name__ == "__main__":
    main()
