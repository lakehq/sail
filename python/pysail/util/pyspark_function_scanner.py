#!/usr/bin/env python3
"""
The purpose of this script is to scan Python files and Jupyter notebooks
for usage of functions from a specified set of modules (`TARGET_MODULES`).
It returns the total count of function calls per module and function name.
"""

import argparse
import ast
import json
import logging
from collections import Counter
from pathlib import Path
from typing import Optional, Set, Tuple, Dict


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# Relevant modules to track
TARGET_MODULES = {
    "pyspark.sql.functions",
    "pyspark.sql.window",
}

# Derived set of package roots to optimize checks (e.g., "pyspark.sql")
TARGET_PACKAGES = {
    module.rsplit(".", 1)[0] for module in TARGET_MODULES if "." in module
}


class PysparkFunctionScanner(ast.NodeVisitor):
    """
    AST Visitor that tracks imports and function calls for specific target modules.
    """

    def __init__(self):
        self.module_aliases: Dict[str, str] = {}
        self.direct_imports: Dict[str, Tuple[str, str]] = {}
        self.calls: Counter = Counter()
        self.wildcard_warning = False

    def _register_alias(self, full_name: str, alias: Optional[str] = None) -> None:
        """Registers a local alias for a tracked module."""
        if full_name in TARGET_MODULES:
            local_name = alias or full_name.rsplit(".", 1)[-1]
            self.module_aliases[local_name] = full_name

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self._register_alias(alias.name, alias.asname)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if not node.module:
            return self.generic_visit(node)

        module_name = node.module.lower()

        # Case 1: from pyspark.sql.functions import col
        if module_name in TARGET_MODULES:
            for alias in node.names:
                if alias.name == "*":
                    self.wildcard_warning = True
                    continue
                local = alias.asname or alias.name
                self.direct_imports[local] = (module_name, alias.name)

        # Case 2: from pyspark.sql import functions as F
        elif module_name in TARGET_PACKAGES:
            for alias in node.names:
                full_imported_module = f"{module_name}.{alias.name}".lower()
                if full_imported_module in TARGET_MODULES:
                    self._register_alias(
                        full_imported_module, alias.asname or alias.name
                    )

        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func

        # Case: F.col(...) -> where F is an alias for a target module
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            alias = func.value.id
            if alias in self.module_aliases:
                self.calls[(self.module_aliases[alias], func.attr)] += 1

        # Case: col(...) -> where col was imported directly
        elif isinstance(func, ast.Name) and func.id in self.direct_imports:
            origin, real_name = self.direct_imports[func.id]
            self.calls[(origin, real_name)] += 1

        self.generic_visit(node)


def parse_and_scan_code(source: str) -> Counter:
    scanner = PysparkFunctionScanner()
    try:
        tree = ast.parse(source)
    except SyntaxError:
        logging.error("Failed to parse source code.")
        return Counter()
    scanner.visit(tree)
    return scanner.calls


def scan_file(path: Path) -> Counter:
    try:
        raw_content = path.read_text(encoding="utf-8")
        if path.suffix == ".ipynb":
            nb = json.loads(raw_content)
            # only keep code cells
            code_cells = [
                c["source"] for c in nb.get("cells", []) if c.get("cell_type") == "code"
            ]
            code = "\n".join(
                "".join(lines) if isinstance(lines, list) else lines
                for lines in code_cells
            )
            # sanitize magic commands by commenting them out
            clean_code = "\n".join(
                f"# {ln}" if ln.lstrip().startswith(("%", "!", "?")) else ln
                for ln in code.splitlines()
            )
            return parse_and_scan_code(clean_code)
        elif path.suffix == ".py":
            return parse_and_scan_code(raw_content)
        else:
            raise ValueError(f"Unsupported file type {path.suffix}")
    except (IOError, UnicodeDecodeError, json.JSONDecodeError):
        logging.error("Failed to read file: %s", path)
        return Counter()


def scan_folder(base_path: Path) -> Counter:
    """Recursively scans a directory for .py and .ipynb files."""
    total = Counter()

    for file_path in filter(
        lambda p: p.suffix in {".py", ".ipynb"}, base_path.rglob("*")
    ):
        total.update(scan_file(file_path))

    return total


def main():
    parser = argparse.ArgumentParser(
        description=f"Scan Python files and Jupyter notebooks for usage of {', '.join(TARGET_MODULES)}."
    )
    parser.add_argument("directory", help="Directory to scan recursively")

    # Optional output format argument for machine readable result
    parser.add_argument(
        "-o",
        "--output",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    args = parser.parse_args()

    base = Path(args.directory)
    if not base.exists():
        raise SystemExit(f"Directory not found: {base}")

    logging.info("Starting scan in directory: %s", base)
    total = scan_folder(base)
    logging.info("Scan complete.")

    if args.output == "text":
        print("\n=== Usage Counts ===")
        if not total:
            print(f"No function calls found for {', '.join(TARGET_MODULES)}.")
            return

        for (module, func), count in total.most_common():
            print(f"  {module}.{func}: {count}")

    elif args.output == "json":
        # Convert Counter output to a list of dicts for JSON serialization
        results = [
            {"module": mod, "function": func, "count": count}
            for (mod, func), count in total.most_common()
        ]
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
