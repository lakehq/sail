#!/usr/bin/env python3
import argparse
import logging
from collections import Counter
from pathlib import Path

from python.pysail.util.pyspark_function_scanner import format_output, scan_directory
from python.pysail.util.sail_function_coverage import extract_function_coverage_from_md

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def check_sail_function_coverage(path: str) -> Counter[tuple[str, str, str]]:
    """
    ...
    """
    sail_coverage = {
        **{
            ("pyspark.sql.functions", function_name): label
            for function_name, label in extract_function_coverage_from_md(
                [
                    Path("docs/guide/functions/scalar.md"),
                    Path("docs/guide/functions/aggregate.md"),
                    Path("docs/guide/functions/window.md"),
                ]
            ).items()
        },
        **{
            ("pyspark.sql.dataframe", function_name): label
            for function_name, label in extract_function_coverage_from_md(
                Path("docs/guide/dataframe/features.md")
            ).items()
        },
    }

    pyspark_function_usage = scan_directory(Path(path))

    counter: Counter[tuple[str, str, str]] = Counter()
    for key, count in pyspark_function_usage.items():
        counter[(*key, sail_coverage.get(key, "❔ unknown"))] = count

    return counter


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scan Python files for PySpark function usage and cross-reference it with sail docs for compatibility."
    )
    parser.add_argument("directory", type=Path, help="Directory to scan recursively")
    parser.add_argument(
        "-o",
        "--output",
        choices=["text", "json", "csv"],
        default="text",
        help="Output format (default: text)",
    )
    args = parser.parse_args()

    if not args.directory.exists():
        raise SystemExit(f"Directory not found: {args.directory}")

    logging.info("Scanning: %s", args.directory)

    # Suppress Jedi internal debug logging
    logging.getLogger("jedi").setLevel(logging.WARNING)

    counts = check_sail_function_coverage(args.directory)
    logging.info("Scan complete.")

    # TODO
    print(counts)


if __name__ == "__main__":
    main()
