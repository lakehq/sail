#!/usr/bin/env python3

"""
Save workspace timestamps with Git entry modes and object IDs as JSON Lines.
Registry source paths are relative to the Cargo registry/src directory and include
the registry identifier, crate name, and version.
"""

import json
import os
from itertools import chain
from pathlib import Path

from common import WorkspaceState


def main():
    state = WorkspaceState.load()
    for path, entry in state.entries.items():
        source = state.root / path
        if not source.exists():
            continue

        data = {"path": path, "mtime": int(source.stat().st_mtime), "mode": entry.mode, "oid": entry.oid}
        print(json.dumps(data))  # noqa: T201

    registry = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo")) / "registry" / "src"
    for package in registry.glob("*/*"):
        if not (package / ".cargo-ok").is_file():
            continue
        for path in chain([package], package.rglob("*")):
            if path.is_symlink():
                continue
            data = {
                "path": str(path.relative_to(registry)),
                "mtime": int(path.stat().st_mtime),
                "registry": True,
            }
            print(json.dumps(data))  # noqa: T201


if __name__ == "__main__":
    main()
