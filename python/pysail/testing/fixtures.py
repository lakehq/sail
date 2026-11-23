# SPDX-License-Identifier: Apache-2.0

import importlib
import os
import sys

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addini(
        "strict_markers",
        "Raise an error for unregistered markers",
        type="bool",
        default=True,
    )


def pytest_configure(config: pytest.Config) -> None:
    """Configure both installed sanity tests and development tests."""
    config.pluginmanager.import_plugin("pysail.testing.suites")
    if config.pluginmanager.has_plugin("doctest"):
        config.getini("doctest_optionflags")[:] = ["ELLIPSIS", "NORMALIZE_WHITESPACE", "IGNORE_EXCEPTION_DETAIL"]

    # Small test inputs intentionally exercise window plans without a partition key.
    config.addinivalue_line(
        "filterwarnings",
        r"ignore:WARN WindowExpression\x3a No Partition Defined for Window operation!:UserWarning:"
        r"pyspark\.sql\.connect\.expressions$",
    )
    # Older supported PySpark clients use these deprecated APIs internally.
    for warning in (
        r"ignore:is_categorical_dtype is deprecated:DeprecationWarning:pyspark\.sql\.pandas\.serializers$",
        r"ignore:is_datetime64tz_dtype is deprecated:DeprecationWarning:pyspark\.sql\.pandas\.types$",
        r"ignore:The 'warn' method is deprecated:DeprecationWarning:pyspark\.logger\.logger$",
    ):
        config.addinivalue_line("filterwarnings", warning)

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
