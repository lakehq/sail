"""Rewriting a recorded reference table.

The two tables next to this file -- `function_result_types.py` and `arithmetic_matrix_types.py` --
hold what BOTH engines answered when they were taken, so the same test can run against either and
say which column it is checking. They are Python and not JSON because nothing under
`python/pysail/tests` is JSON: the repo records data as syrupy snapshots or `.txt` files, and a
two-column table indexed by engine fits neither.

They are regenerated deliberately, one engine at a time, by the `__main__` block of the test that
reads them -- never by the test itself, so a divergence that changes has to be looked at.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pathlib

LINE_LENGTH = 120
# Spark first: the oracle column reads before the one it is measured against, and a fixed order
# keeps a regeneration diff down to the cells that actually changed.
COLUMNS = ("spark", "sail")


def write_reference(path: pathlib.Path, name: str, data: dict[str, dict[str, str]]) -> None:
    """Rewrite `path` as `name = {...}`, sorted, one entry per line where the line fits."""
    header = path.read_text().split(f"{name} = {{")[0].rstrip("\n")
    lines = [header, "", f"{name} = {{"]
    for key, value in sorted(data.items()):
        pairs = ", ".join(f'"{column}": {json.dumps(value[column])}' for column in COLUMNS if column in value)
        entry = f"    {json.dumps(key)}: {{{pairs}}},"
        if len(entry) > LINE_LENGTH:
            fields = "\n".join(
                f'        "{column}": {json.dumps(value[column])},' for column in COLUMNS if column in value
            )
            entry = f"    {json.dumps(key)}: {{\n{fields}\n    }},"
        lines.append(entry)
    lines.append("}")
    path.write_text("\n".join(lines) + "\n")
