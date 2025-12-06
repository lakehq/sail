import json
from collections import Counter
from pathlib import Path
from typing import Literal

import pysail
from pysail.utils.pyspark_function_scanner import _scan_directory


def _load_sail_support_data() -> dict[tuple[str, str], str]:
    """Retrieve support information from internal JSON files."""
    sail_support_index: dict[tuple[str, str], str] = {}

    base_dir = Path(pysail.__file__).parent / "data" / "compatibility"

    for p in base_dir.rglob("*.json"):
        if p.is_file():
            data = json.loads(p.read_text(encoding="utf-8"))
            sail_support_index.update({(func["module"], func["function"]): func["status"] for func in data})

    return sail_support_index


def _check_sail_pyspark_compatibility(repo_path: Path) -> Counter[tuple[str, str, str]]:
    """Scan a directory for PySpark function usage and look up sail support."""
    sail_support_index = _load_sail_support_data()

    pyspark_function_usage = _scan_directory(repo_path)

    counter: Counter[tuple[str, str, str]] = Counter()
    for key, count in pyspark_function_usage.items():
        counter[(*key, sail_support_index.get(key, "unknown"))] = count

    return counter


def _decode_support_label(label: str) -> str:
    """Decode support labels into emoji-style status strings."""
    preprocessed_label = label.strip().lower()
    mappings = {
        "in progress": "🚧 in progress",
        "not supported": "❌ not supported",
        "supported": "✅ supported",
        "unknown": "❔ unknown",
    }
    return mappings.get(preprocessed_label, "❔ unknown")


def _format_output(counts: Counter[tuple[str, str, str]], fmt: Literal["json", "csv", "text"]) -> str:
    """Format results as text, CSV or JSON."""

    if fmt == "json":
        results = [
            {
                "module": mod,
                "function": func,
                "supported": support,
                "count": cnt,
            }
            for (mod, func, support), cnt in counts.most_common()
        ]

        return json.dumps(results, indent=2)

    if fmt == "csv":
        results = [(mod, func, support, cnt) for (mod, func, support), cnt in counts.most_common()]
        header = [("module", "function", "supported", "count")]

        return "\n".join(";".join(str(item) for item in row) for row in header + results)

    if fmt == "text":
        if not counts:
            return "No relevant function calls found."
        lines = ["", "=== Usage Counts ==="]
        # Sort by module name, then descending by call count
        sorted_items = sorted(counts.items(), key=lambda x: (x[0][0], -x[1]))

        current_mod = None
        for (mod, func, support), cnt in sorted_items:
            if mod != current_mod:
                lines.append(f"\n[{mod}]")
                current_mod = mod
            lines.append(f"  .{func}: {cnt} ({_decode_support_label(support)})")

        return "\n".join(lines)

    msg = f"Unsupported format: {fmt}"
    raise ValueError(msg)
