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
from typing import Optional, Tuple

# Relevant modules to track
TARGET_MODULES = {
    "pyspark.sql.functions",
    "pyspark.sql.window",
}

_TARGET_PACKAGES = {module_adr.rsplit(".", 1)[0] for module_adr in TARGET_MODULES}

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


class PysparkFunctionScanner(ast.NodeVisitor):
    def __init__(self):
        self.module_aliases: dict[str, str] = {}
        self.direct_imports: dict[str, Tuple[str, str]] = {}
        self.calls: Counter = Counter()

    def _handle_module_alias(self, full_name: str, alias: Optional[str] = None) -> None:
        if full_name.lower() in TARGET_MODULES:
            local_name = alias or full_name.rsplit(".", 1)[-1]
            self.module_aliases[local_name] = full_name

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self._handle_module_alias(alias.name, alias.asname)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        # Direct imports from tracked modules -> direct_imports
        if node.module.lower() in TARGET_MODULES:
            for alias in node.names:
                local = alias.asname or alias.name
                self.direct_imports[local] = (node.module, alias.name)
        elif node.module.lower() in _TARGET_PACKAGES:
            for alias in node.names:
                module_adr = f"{node.module}.{alias.name}".lower()
                if module_adr in TARGET_MODULES:
                    self.module_aliases[alias.asname or alias.name] = module_adr

        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            alias = func.value.id
            if alias in self.module_aliases:
                self.calls[(self.module_aliases[alias], func.attr)] += 1
        elif isinstance(func, ast.Name) and func.id in self.direct_imports:
            origin, real_name = self.direct_imports[func.id]
            self.calls[(origin, real_name)] += 1

        self.generic_visit(node)


def _sanitize_code(source: str) -> str:
    """Comment out IPython magics/shell commands so ast.parse won't fail."""
    prefixes = ("%%", "%", "!", "?")
    return "\n".join(
        ("# " + ln if ln.lstrip().startswith(prefixes) else ln)
        for ln in source.splitlines()
    )


def _parse_and_scan(source: str) -> Counter:
    scanner = PysparkFunctionScanner()
    try:
        tree = ast.parse(_sanitize_code(source))
    except SyntaxError:
        logging.error("Failed to parse source code.")
        return Counter()
    scanner.visit(tree)
    return scanner.calls


def scan_py_file(path: Path) -> Counter:
    try:
        text = path.read_text(encoding="utf-8")
    except (IOError, UnicodeDecodeError):
        logging.error("Failed to read file: %s", path)
        return Counter()
    return _parse_and_scan(text)


def scan_ipynb_file(path: Path) -> Counter:
    try:
        nb = json.loads(path.read_text(encoding="utf-8"))
    except (IOError, json.JSONDecodeError):
        logging.error("Failed to read/decode notebook JSON: %s", path)
        return Counter()

    cells = nb.get("cells", [])
    code = []
    for c in cells:
        if c.get("cell_type") == "code":
            src = c.get("source", "")
            code.append("".join(src) if isinstance(src, list) else src)

    return _parse_and_scan("\n\n".join(code))


def scan_folder(base_path: Path) -> Counter:
    """Recursively scans a directory for .py and .ipynb files."""
    total = Counter()

    for file_path in base_path.rglob("*"):
        if file_path.suffix == ".py":
            total.update(scan_py_file(file_path))
        elif file_path.suffix == ".ipynb":
            total.update(scan_ipynb_file(file_path))

    return total


def main():
    parser = argparse.ArgumentParser(
        description="Scan Python files and Jupyter notebooks for pyspark.sql function usage."
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
            print("No PySpark function calls found.")
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
