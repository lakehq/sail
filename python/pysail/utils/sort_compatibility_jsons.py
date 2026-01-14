"""Sort JSON files in the compatibility data directory by the `function` key.

Each file is expected to contain a top-level JSON array. The script sorts
that array in-place by the `function` attribute of each element.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import tempfile
from pathlib import Path
from typing import Final

from pysail.utils.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

EXPECTED_KEYS: Final[tuple[str]] = ("module", "function", "status")


def reorder_item(item: dict[str, str]) -> dict[str, str]:
    """
    reorder dictionary keys: module, function, status, then other keys alphabetically
    """
    ordered: dict[str, str] = {}
    missing_keys = {ek for ek in EXPECTED_KEYS if ek not in item}
    if missing_keys:
        msg = f"JSON item {item} does not contain key(s): {missing_keys}"
        raise KeyError(msg)
    for k in EXPECTED_KEYS:
        ordered[k] = item[k]

    # remaining keys
    remaining = [k for k in item if k not in EXPECTED_KEYS]
    for k in sorted(remaining):
        ordered[k] = item[k]
    return ordered


def sort_json_file(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    data = json.loads(text)
    if not isinstance(data, list):
        msg = f"{path} does not contain a JSON array"
        raise TypeError(msg)

    data = [reorder_item(it) for it in data]

    data.sort(key=lambda item: tuple(item[key] for key in EXPECTED_KEYS))

    # write atomically to avoid data loss
    with tempfile.NamedTemporaryFile("w", delete=False, dir=path.parent, encoding="utf-8") as tmpf:
        json.dump(data, tmpf, indent=2, ensure_ascii=False)
        tmpf.write("\n")
        tmpname = Path(tmpf.name)

    shutil.move(str(tmpname), str(path))


def main() -> None:
    default_dir = Path(__file__).resolve().parents[1] / "data" / "compatibility"
    parser = argparse.ArgumentParser(description="Sort compatibility JSON files by key")
    parser.add_argument("dir", nargs="?", type=Path, default=default_dir, help="Directory with JSON files to sort")
    args = parser.parse_args()

    if not args.dir.exists():
        msg = f"Directory not found: {args.dir}"
        raise SystemExit(msg)
    if not args.dir.is_dir():
        msg = f"Not a directory: {args.dir}"
        raise SystemExit(msg)

    json_files = sorted(p for p in args.dir.rglob("*.json") if p.is_file())
    if not json_files:
        logger.info("No .json files found in %s (including subdirectories)", args.dir)
        return

    for json_file in json_files:
        try:
            sort_json_file(json_file)
            logger.info("Sorted: %s", json_file)
        except (KeyError, json.JSONDecodeError):
            logger.exception("Failed to sort %s", json_file)


if __name__ == "__main__":
    main()
