"""Shared helpers for saving and restoring timestamps."""

# Imported by standalone CI scripts rather than a Python package.
# ruff: noqa: INP001

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass
class GitEntry:
    mode: str
    oid: str


def git_output(*args, cwd=None):
    executable = shutil.which("git")
    if executable is None:
        msg = "Git executable was not found"
        raise FileNotFoundError(msg)
    return subprocess.run(
        [executable, *args],
        cwd=cwd,
        env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
        stdout=subprocess.PIPE,
        check=True,
    ).stdout


@dataclass
class WorkspaceState:
    root: Path
    entries: dict[str, GitEntry]

    @classmethod
    def load(cls) -> "WorkspaceState":
        """Find Git entry modes and blob/tree IDs for workspace paths."""
        root = Path(os.fsdecode(git_output("rev-parse", "--show-toplevel")).rstrip("\n")).resolve()
        entries = {}
        for entry in git_output("ls-tree", "-r", "-t", "-z", "--full-tree", "HEAD", cwd=root).split(b"\0"):
            if not entry:
                continue
            info, path = entry.split(b"\t", 1)
            mode, _kind, oid = info.split()
            entries[os.fsdecode(path)] = GitEntry(mode=mode.decode("ascii"), oid=oid.decode("ascii"))

        # CI starts from a clean checkout, leaves tracked files and the index unchanged,
        # has no tracked symlinks/submodules, and generates no inputs in watched source
        # directories before saving/restoring mtimes.
        # Checkout filters and line-ending settings must remain consistent across runs.
        return cls(root=root, entries=entries)
