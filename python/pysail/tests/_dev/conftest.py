import os
from pathlib import Path

import pytest

INTEGRATION_TEST_PATHS = [
    Path(__file__).parent / "celeborn",
    Path(__file__).parent / "catalog" / "glue",
    Path(__file__).parent / "catalog" / "hms",
    Path(__file__).parent / "catalog" / "iceberg",
    Path(__file__).parent / "catalog" / "unity",
    Path(__file__).parent / "spark" / "catalog" / "glue",
    Path(__file__).parent / "spark" / "catalog" / "hms",
    Path(__file__).parent / "spark" / "catalog" / "iceberg",
    Path(__file__).parent / "spark" / "catalog" / "unity",
]

# Keep these names in sync with the `catalog-tests.yml` job matrix. Group by service
# so related tests can reuse their containers and session-scoped fixtures.
INTEGRATION_TEST_SUITES = {
    "hms": ("catalog/hms", "spark/catalog/hms"),
    "unity": ("catalog/unity", "spark/catalog/unity"),
    "iceberg-rest": ("catalog/iceberg", "spark/catalog/iceberg"),
    "delta": ("spark/delta",),
    "celeborn": ("celeborn", "spark/execution/test_shuffle_celeborn.py"),
    "other": (),
}


def pytest_configure(config: pytest.Config) -> None:
    config.pluginmanager.import_plugin("pysail.testing.fixtures")
    config.addinivalue_line(
        "markers",
        "integration: mark test as requiring external services and deselected by default",
    )
    suite = os.environ.get("PYSAIL_INTEGRATION_SUITE")
    if suite is not None and suite not in INTEGRATION_TEST_SUITES:
        msg = f"Unknown PYSAIL_INTEGRATION_SUITE {suite!r}; expected one of: {', '.join(INTEGRATION_TEST_SUITES)}"
        raise pytest.UsageError(msg)

    config.pluginmanager.import_plugin("pysail.testing._dev.containers.celeborn")
    config.pluginmanager.import_plugin("pysail.testing._dev.containers.glue")
    config.pluginmanager.import_plugin("pysail.testing._dev.containers.hms")
    config.pluginmanager.import_plugin("pysail.testing._dev.containers.iceberg_rest")
    config.pluginmanager.import_plugin("pysail.testing._dev.containers.unity")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    # Add BDD feature file paths as an extra keyword to support test selection based on feature files.
    root = Path(__file__).resolve().parent
    for item in items:
        scenario = getattr(getattr(item, "function", None), "__scenario__", None)
        feature = getattr(scenario, "feature", None)
        filename = getattr(feature, "filename", None)
        if filename:
            path = Path(filename).resolve().relative_to(root)
            item.extra_keyword_matches.add(path.as_posix())

    for item in items:
        item_path = item.path.resolve()
        if any(item_path.is_relative_to(path.resolve()) for path in INTEGRATION_TEST_PATHS):
            item.add_marker(pytest.mark.integration)

    suite = os.environ.get("PYSAIL_INTEGRATION_SUITE")
    if suite is not None or not config.getoption("markexpr"):
        remaining = []
        deselected = []
        for item in items:
            integration = item.get_closest_marker("integration") is not None
            if suite is None:
                selected = not integration
            else:
                path = item.path.resolve()
                selected = (
                    integration
                    and path.is_relative_to(root)
                    and integration_test_suite(path.relative_to(root)) == suite
                )
            (remaining if selected else deselected).append(item)
        if deselected:
            config.hook.pytest_deselected(items=deselected)
            items[:] = remaining


def integration_test_suite(path: Path) -> str:
    """Assign a path relative to `pysail/tests/_dev` to exactly one integration suite."""
    for suite, prefixes in INTEGRATION_TEST_SUITES.items():
        if any(path.is_relative_to(prefix) for prefix in prefixes):
            return suite
    return "other"
