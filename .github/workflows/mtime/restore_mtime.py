#!/usr/bin/env python3

"""
Restore workspace timestamps when Git entry modes and blob/tree IDs match.
Published registry crate versions are immutable, so their file and directory
timestamps are restored without comparing contents. Dependencies must be unpacked
before restoration.
"""

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from common import WorkspaceState


@dataclass
class Counter:
    workspace: int = 0
    registry: int = 0


def main():
    state = WorkspaceState.load()
    registry_root = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo")) / "registry" / "src"
    counter = Counter()
    for line in sys.stdin:
        line = line.strip()  # noqa: PLW2901
        if not line:
            continue

        data = json.loads(line)
        path = data["path"]
        mtime = data["mtime"]
        if data.get("registry"):
            path = registry_root / path
            if not path.is_symlink() and path.exists():
                os.utime(path, (mtime, mtime))
                counter.registry += 1
            continue
        entry = state.entries.get(path)
        if entry is None or entry.mode != data.get("mode") or entry.oid != data.get("oid"):
            continue
        source = state.root / path
        if source.exists():
            os.utime(source, (mtime, mtime))
            counter.workspace += 1

    if counter.workspace:
        print(f"Restored timestamps for {counter.workspace} workspace path(s).")  # noqa: T201
    if counter.registry:
        print(f"Restored timestamps for {counter.registry} registry source path(s).")  # noqa: T201


if __name__ == "__main__":
    main()
