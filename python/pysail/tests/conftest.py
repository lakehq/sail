import importlib
import os
import sys
from pathlib import Path

import pytest

INTEGRATION_TEST_PATHS = [
    Path(__file__).parent / "celeborn",
    Path(__file__).parent / "catalog" / "glue",
    Path(__file__).parent / "catalog" / "hms",
    Path(__file__).parent / "catalog" / "iceberg_rest",
    Path(__file__).parent / "spark" / "catalog" / "glue",
    Path(__file__).parent / "spark" / "catalog" / "hms",
    Path(__file__).parent / "spark" / "catalog" / "iceberg_rest",
    Path(__file__).parent / "spark" / "catalog" / "unity",
]


def pytest_configure(config: pytest.Config) -> None:
    """Configure pytest.

    We include the tests in the installed package so that the user can test the installation
    via `pytest --pyargs pysail`.
    We must customize the configuration here instead of using `pytest.ini` or `pyproject.toml`
    since these files are not part of the installed package.
    """

    config.pluginmanager.import_plugin("pysail.testing.containers.celeborn")
    config.pluginmanager.import_plugin("pysail.testing.containers.glue")
    config.pluginmanager.import_plugin("pysail.testing.containers.hms")
    config.pluginmanager.import_plugin("pysail.testing.containers.iceberg_rest")

    # Note: configuration set via `config.inicfg` may not have an effect due to the cache used
    # in `config.getini()`. In such a case, we may have to clear the INI cache in `config`.
    # Since clearing the cache requires access to the private attribute of `config`, we do not
    # do it here unless absolutely necessary, to avoid compatibility issues with future versions
    # of pytest.
    # In common cases, plugins only access the configuration for the first time after this hook,
    # so the cache is not a problem.
    config.inicfg["doctest_optionflags"] = [
        "ELLIPSIS",
        "NORMALIZE_WHITESPACE",
        "IGNORE_EXCEPTION_DETAIL",
    ]

    # Default Syrupy snapshot format is Amber (`.ambr`), but we prefer standard YAML multi-doc files.
    default_ext = getattr(config.option, "default_extension", None)
    if default_ext is None:
        config.option.default_extension = "pysail.testing.snapshot.yaml.YamlSnapshotExtension"

    config.addinivalue_line(
        "markers",
        "yamlsnapshot: add metadata to customize the YAML snapshot",
    )
    config.addinivalue_line(
        "markers",
        "integration: mark test as requiring external services and deselected by default",
    )

    configure_sail_environment()


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    # Add BDD feature file paths as an extra keyword to support test selection based on feature files.
    package_root = Path(__file__).resolve().parents[1]
    for item in items:
        scenario = getattr(getattr(item, "function", None), "__scenario__", None)
        feature = getattr(scenario, "feature", None)
        filename = getattr(feature, "filename", None)
        if filename:
            path = Path(filename).resolve().relative_to(package_root)
            item.extra_keyword_matches.add(path.as_posix())

    for item in items:
        item_path = item.path.resolve()
        if any(item_path.is_relative_to(path.resolve()) for path in INTEGRATION_TEST_PATHS):
            item.add_marker(pytest.mark.integration)

    if not config.getoption("markexpr"):
        deselected = [item for item in items if item.get_closest_marker("integration")]
        if deselected:
            remaining = [item for item in items if item not in deselected]
            config.hook.pytest_deselected(items=deselected)
            items[:] = remaining


def configure_sail_environment():
    """Configure environment variables for PySail tests.

    The runtime configuration options cannot be changed, so we must ensure that
    the configuration is in place before the first server is created.
    """

    module = "pysail._native"

    if module in sys.modules:
        msg = "The PySail native module should not be imported before configuring the environment."
        raise RuntimeError(msg)

    # Set the default parallelism to a fixed value regardless of the
    # number of CPU cores to ensure deterministic test results, especially for
    # snapshot tests involving execution plans.
    os.environ["SAIL_EXECUTION__DEFAULT_PARALLELISM"] = "4"
    # Set the stack size explicitly to assist the configuration removal test.
    # And we need the larger stack size to support large query plans in the test.
    os.environ["SAIL_RUNTIME__STACK_SIZE"] = "16777216"

    # Ensure the native module can be imported successfully.
    # This allows this function to be future-proof in case we ever change the native module name.
    # If the native module fails to load, an exception will be raised here, so that we can
    # remember to change the module name used in this function accordingly.
    importlib.import_module(module)
