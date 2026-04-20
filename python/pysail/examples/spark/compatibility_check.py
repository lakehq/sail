import argparse
import logging
from pathlib import Path

from pysail.spark.utils._function_support import (
    check_sail_pyspark_compatibility,
    format_output,
)

logger = logging.getLogger(__name__)

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(module)s.%(funcName)s(): %(message)s"


def setup_logging(level: int = logging.INFO) -> None:
    """Idempotent logging setup."""
    root = logging.getLogger()
    if root.handlers:
        return  # end early if logging is already configured

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(_LOG_FORMAT))

    root.setLevel(level)
    root.addHandler(handler)

    # suppress noisy third-party loggers
    logging.getLogger("jedi").setLevel(logging.WARNING)


def main() -> None:
    setup_logging()
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
        msg = f"Directory not found: {args.directory}"
        raise SystemExit(msg)

    logger.info("Scanning: %s", base_dir)

    counts = check_sail_pyspark_compatibility(base_dir)
    logger.info("Scan complete.")

    print(format_output(counts, args.output))


if __name__ == "__main__":
    main()
