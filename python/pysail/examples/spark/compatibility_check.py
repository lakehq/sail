import argparse
import logging
from pathlib import Path

from pysail.utils.logging_config import setup_logging
from pysail.utils.sail_function_support import (
    _check_sail_pyspark_compatibility,
    _format_output,
)

setup_logging()
logger = logging.getLogger(__name__)


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

    base_dir = Path(args.directory)
    if not base_dir.exists():
        msg = "Directory not found: %s"
        raise SystemExit(msg, args.directory)

    logger.info("Scanning: %s", base_dir)

    counts = _check_sail_pyspark_compatibility(base_dir)
    logger.info("Scan complete.")

    print(_format_output(counts, args.output))


if __name__ == "__main__":
    main()
