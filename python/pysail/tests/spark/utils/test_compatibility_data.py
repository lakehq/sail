import json
from pathlib import Path
from typing import Final

import pysail

EXPECTED_KEYS: Final[tuple[str, ...]] = ("module", "function", "status")


def test_compatibility_json_files_are_sorted():
    base_dir = Path(pysail.__file__).parent / "data" / "compatibility"
    json_files = sorted(p for p in base_dir.rglob("*.json") if p.is_file())
    assert json_files, f"No JSON files found in {base_dir}"

    for json_file in json_files:
        data = json.loads(json_file.read_text(encoding="utf-8"))
        assert isinstance(data, list), f"{json_file} does not contain a JSON array"

        for item in data:
            assert isinstance(item, dict), f"Item {item!r} in {json_file} is not a dict"
            missing_keys = {k for k in EXPECTED_KEYS if k not in item}
            assert not missing_keys, f"Item {item} in {json_file} is missing keys: {missing_keys}"

        sort_keys = [tuple(item[k] for k in EXPECTED_KEYS) for item in data]
        assert sort_keys == sorted(sort_keys), (
            f"{json_file} is not sorted by {EXPECTED_KEYS}. "
            "Please sort the file by the 'module', 'function', and 'status' keys."
        )
