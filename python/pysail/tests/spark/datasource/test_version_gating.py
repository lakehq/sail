"""Meta-test: every datasource test module that imports a version-specific PySpark
API must gate itself, so it is skipped (not errored) on Spark versions that lack it.

This guards against the failure that broke CI on Spark 3.5.7 / 4.0.1: a module
imported ``pyspark.sql.datasource`` (or ``pysail.spark.datasource.jdbc``, which needs
it) without a version gate, so collection errored on tiers where the API is absent.

Pure static inspection (AST) — no version-specific imports — so it runs on every tier.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_DIR = Path(__file__).parent

# A module needs gating if it imports either of these (the second transitively needs the first).
_GATED_IMPORTS = ("pyspark.sql.datasource", "pysail.spark.datasource.jdbc")

# Any one of these makes a module acceptably gated.
_GATE_TOKENS = ("pyspark_version", "importorskip", "allow_module_level")


def _imports_versioned_api(tree: ast.Module) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if any(node.module == m or node.module.startswith(m + ".") for m in _GATED_IMPORTS):
                return True
        elif isinstance(node, ast.Import) and any(
            alias.name.startswith(m) for alias in node.names for m in _GATED_IMPORTS
        ):
            return True
    return False


@pytest.mark.parametrize("path", sorted(_DIR.glob("test_*.py")), ids=lambda p: p.name)
def test_datasource_test_module_is_version_gated(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    if not _imports_versioned_api(ast.parse(text)):
        pytest.skip("does not import a version-specific DataSource API")
    assert any(token in text for token in _GATE_TOKENS), (
        f"{path.name} imports the Python DataSource API but has no version gate. "
        f"Add `if pyspark_version() < (4, 1): pytest.skip(..., allow_module_level=True)` "
        f"(or `pytest.importorskip`) at module top, or mark it `pytest.mark.integration`. "
        f"Without it, collection errors on Spark tiers that lack the API (broke CI on 3.5.7/4.0.1)."
    )
