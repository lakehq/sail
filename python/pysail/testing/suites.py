"""Infer suites before pytest applies its standard marker and keyword filters."""

import json
import os
import re
import sys
from collections import Counter
from hashlib import sha256
from pathlib import Path

import pytest
from packaging.specifiers import SpecifierSet
from packaging.version import Version

from pysail.testing.spark.markers import resolve_spark_markers
from pysail.testing.spark.utils.common import pyspark_version

# Paths are relative to tests (with the optional _dev prefix removed).
INTEGRATION_PATHS = (
    "celeborn",
    "catalog/glue",
    "catalog/hms",
    "catalog/iceberg",
    "catalog/unity",
    "spark/catalog/glue",
    "spark/catalog/hms",
    "spark/catalog/iceberg",
    "spark/catalog/unity",
)
INTEGRATION_GROUPS = {
    "hms": ("catalog/hms", "spark/catalog/hms"),
    "unity": ("catalog/unity", "spark/catalog/unity"),
    "iceberg-rest": ("catalog/iceberg", "spark/catalog/iceberg"),
    "delta": ("spark/delta",),
    "celeborn": ("celeborn", "spark/execution/test_shuffle_celeborn.py"),
    "glue": ("catalog/glue", "spark/catalog/glue"),
    "jdbc": ("spark/datasource/test_jdbc.py",),
}
SUITE_PATHS = {
    "udf": ("spark/udf", "spark/function/test_lazy_python_udf.py"),
    "function": ("spark/function",),
    "dataframe": ("spark/dataframe",),
    "delta": ("spark/delta",),
    "iceberg": ("spark/iceberg",),
    "datasource": ("spark/datasource",),
    "distributed": ("spark/execution",),
    "benchmark": ("spark/test_tpch.py", "spark/test_tpcds.py", "spark/test_clickbench.py"),
    "sql": (
        "spark/analyzer",
        "spark/catalog",
        "spark/dml",
        "spark/features",
        "spark/literal",
        "spark/optimizer",
        "spark/test_features.py",
        "spark/test_sql.py",
    ),
}


def matrix_versions() -> tuple[str | None, list[str]]:
    seed = os.environ.get("PYSAIL_TEST_SEED")
    raw = os.environ.get("PYSAIL_TEST_SPARK_VERSIONS")
    if seed is None and raw is None:
        return None, []
    msg = (
        "Set PYSAIL_TEST_SEED and PYSAIL_TEST_SPARK_VERSIONS to a seed and a JSON array "
        "of distinct versions including the installed PySpark version"
    )
    try:
        versions = json.loads(raw) if raw is not None else None
    except ValueError as error:
        raise pytest.UsageError(msg) from error
    if (
        seed is None
        or not isinstance(versions, list)
        or not versions
        or any(not isinstance(version, str) for version in versions)
    ):
        raise pytest.UsageError(msg)
    try:
        versions = sorted(versions, key=Version)
        parsed = {Version(version) for version in versions}
        current = Version(".".join(map(str, pyspark_version())))
    except ValueError as error:
        raise pytest.UsageError(msg) from error
    if len(parsed) != len(versions) or current not in parsed:
        raise pytest.UsageError(msg)
    return seed, versions


def assigned_versions(item: pytest.Item, suite: str, seed: str, versions: list[str]) -> list[str]:
    try:
        supports = resolve_spark_markers(item.iter_markers("spark"))
    except (TypeError, ValueError) as error:
        msg = f"{item.nodeid}: {error}"
        raise pytest.UsageError(msg) from error
    # Reasons and marker order do not change assignment. Repeated specifiers intersect.
    requirements = SpecifierSet(",".join(str(support.version) for support in supports if support.version))
    eligible = sorted((version for version in versions if Version(version) in requirements), key=Version)
    if not eligible or any(support.cross_version for support in supports):
        return eligible
    # Hash each group independently: collection can differ between client versions,
    # or when users apply -m, -k, or path filters. Neither changes group assignment.
    key = json.dumps([seed, suite, str(requirements)])
    index = int.from_bytes(sha256(key.encode()).digest(), "big") % len(eligible)
    return [eligible[index]]


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "suite(name): select an inferred test suite")
    config.addinivalue_line("markers", "integration(group): tests requiring external services")


def relative_test_path(path: Path) -> Path:
    # Installed sanity tests and externally supplied development tests have different roots.
    dev = sys.modules.get("pysail.tests._dev")
    roots = [Path(__file__).resolve().parents[1] / "tests"]
    if dev is not None:
        roots.insert(0, Path(dev.__file__).resolve().parent)
    for root in roots:
        if path.is_relative_to(root):
            relative = path.relative_to(root)
            return relative.relative_to("_dev") if relative.parts[0] == "_dev" else relative
    return path


def path_group(path: Path, groups: dict[str, tuple[str, ...]], default: str) -> str:
    return next((name for name, prefixes in groups.items() if any(path.is_relative_to(p) for p in prefixes)), default)


def marker_group(item: pytest.Item, marker_name: str) -> str | None:
    groups = set()
    for marker in item.iter_markers(marker_name):
        group = marker.kwargs.get("group", marker.args[0] if marker.args else None)
        if group is not None:
            validate_suite_name(group)
            groups.add(group)
    # BDD tags are a set: do not depend on their iteration order across workers.
    return min(groups) if groups else None


def validate_suite_name(name: str) -> None:
    if not isinstance(name, str) or re.fullmatch(r"[a-z0-9][a-z0-9-]*", name) is None:
        msg = f"Invalid suite name: {name!r}"
        raise pytest.UsageError(msg)


def infer_suite(item: pytest.Item, path: Path) -> str:
    if item.get_closest_marker("integration") is None and any(
        path.is_relative_to(prefix) for prefix in INTEGRATION_PATHS
    ):
        item.add_marker(pytest.mark.integration)
    if item.get_closest_marker("integration") is not None:
        return "integration-" + (marker_group(item, "integration") or path_group(path, INTEGRATION_GROUPS, "other"))
    if item.get_closest_marker("function") is not None:
        group = marker_group(item, "function")
        return f"function-{group}" if group else "function"
    return path_group(path, SUITE_PATHS, "default")


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]):
    # The pre-yield phase runs before pytest's own -m and -k selection hooks.
    for item in items:
        names = set()
        for marker in item.iter_markers("suite"):
            name = marker.kwargs.get("name")
            validate_suite_name(name)
            names.add(name)
        if len(names) > 1:
            msg = f"{item.nodeid}: conflicting suite markers: {', '.join(sorted(names))}"
            raise pytest.UsageError(msg)
        if not names:
            name = infer_suite(item, relative_test_path(item.path.resolve()))
            validate_suite_name(name)
            item.add_marker(pytest.mark.suite(name=name))

        scenario = getattr(getattr(item, "function", None), "__scenario__", None)
        filename = getattr(getattr(scenario, "feature", None), "filename", None)
        if filename:
            item.extra_keyword_matches.add(relative_test_path(Path(filename).resolve()).as_posix())

    yield

    # Other collection hooks attach Spark metadata to doctests and UDFs. Resolve
    # it only after those hooks and pytest's standard -m/-k filters have run.
    seed, versions = matrix_versions()
    raw_choices = os.environ.get("PYSAIL_TEST_SUITE_CHOICES")
    choices = None
    if raw_choices is not None:
        msg = "PYSAIL_TEST_SUITE_CHOICES must be a JSON array of enabled suite names"
        try:
            names = json.loads(raw_choices)
        except ValueError as error:
            raise pytest.UsageError(msg) from error
        if not isinstance(names, list):
            raise pytest.UsageError(msg)
        for name in names:
            validate_suite_name(name)
        choices = set(names)
    counts = Counter(item.get_closest_marker("suite").kwargs["name"] for item in items)
    manifest = os.environ.get("PYSAIL_TEST_SUITE_MANIFEST")
    if manifest is not None:
        if hasattr(config, "workerinput"):
            msg = "Suite discovery must run without pytest-xdist workers"
            raise pytest.UsageError(msg)
        inventory = [{"name": name, "test_count": count} for name, count in sorted(counts.items())]
        Path(manifest).write_text(json.dumps(inventory, indent=2) + "\n")

    remaining, deselected = [], []
    current = Version(".".join(map(str, pyspark_version()))) if seed is not None else None
    for item in items:
        name = item.get_closest_marker("suite").kwargs["name"]
        selected = choices is None or name in choices
        if selected and seed is not None:
            selected = any(Version(version) == current for version in assigned_versions(item, name, seed, versions))
        (remaining if selected else deselected).append(item)
    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = remaining
