import importlib
import os
import sys

import pytest


def pytest_configure(config: pytest.Config) -> None:
    """Configure both installed sanity tests and development tests."""
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


@pytest.fixture(scope="session", autouse=True)
def sail_environment() -> None:
    """Configure environment variables for PySail tests.

    The runtime configuration options cannot be changed, so we must ensure that
    the configuration is in place before the first server is created.
    Native imports are deferred to test execution so collection needs no Sail build.
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
    # This allows this fixture to be future-proof in case we ever change the native module name.
    # If the native module fails to load, an exception will be raised here, so that we can
    # remember to change the module name used in this fixture accordingly.
    importlib.import_module(module)
