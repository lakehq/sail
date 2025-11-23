#!/usr/bin/env python3
import argparse
import logging
from pathlib import Path

from python.pysail.util.sail_function_coverage import (
    check_sail_function_coverage,
    format_output,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scan Python files for PySpark function usage and cross-reference it with sail docs for compatibility."
    )
    parser.add_argument(
        "directory",
        type=Path,
        help="Directory of Python files to scan recursively for PySpark functions.",
    )
    parser.add_argument(
        "-o",
        "--output",
        choices=["text", "json", "csv"],
        default="text",
        help="Output format (default: text).",
    )
    args = parser.parse_args()

    if not args.directory.exists():
        raise SystemExit(f"Directory not found: {args.directory}")

    logging.info("Scanning: %s", args.directory)

    # Suppress Jedi internal debug logging
    logging.getLogger("jedi").setLevel(logging.WARNING)

    counts = check_sail_function_coverage(args.directory)
    logging.info("Scan complete.")

    print(format_output(counts, args.output))


if __name__ == "__main__":
    main()
