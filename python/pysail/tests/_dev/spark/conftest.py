from __future__ import annotations

import platform
import re
from typing import TYPE_CHECKING

import pytest
from packaging.version import Version

from pysail.testing.spark.fixtures import (
    local_timezone,
    remote,
    session_timezone,
    spark,
    spark_doctest,
)
from pysail.testing.spark.utils.common import is_jvm_spark

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["local_timezone", "remote", "session_timezone", "spark", "spark_doctest"]


def pytest_configure(config: pytest.Config) -> None:
    config.pluginmanager.import_plugin("pysail.testing.spark.markers")
    config.pluginmanager.import_plugin("pysail.testing.spark.doctest")
    # Register custom markers.
    config.addinivalue_line(
        "markers",
        "function(group): categorize a function BDD scenario",
    )
    # Load all pytest-bdd step modules.
    config.pluginmanager.import_plugin("pysail.testing._dev.spark.steps.files")
    config.pluginmanager.import_plugin("pysail.testing._dev.spark.steps.sql")
    config.pluginmanager.import_plugin("pysail.testing._dev.spark.steps.plan")
    config.pluginmanager.import_plugin("pysail.testing._dev.spark.steps.delta")
    config.pluginmanager.import_plugin("pysail.testing._dev.spark.steps.iceberg")


FUNCTION_TAG_PATTERN = re.compile(r"function\((.*)\)")
FUNCTION_TAG_VALUES = ["nullability", "columnargs", "lambda", "sketch"]
SPARK_TAG_PATTERN = re.compile(r"spark-(.*)")


def pytest_bdd_apply_tag(tag: str, function: Callable[..., object]) -> bool | None:
    if (m := FUNCTION_TAG_PATTERN.fullmatch(tag)) is not None:
        group = m.group(1)
        if group not in FUNCTION_TAG_VALUES:
            msg = f"invalid function tag: {tag}"
            raise ValueError(msg)
        pytest.mark.function(group=group)(function)
        return True

    if tag == "sail-only":
        pytest.mark.spark(skip_if_jvm=True, reason="Tests a Sail-only feature")(function)
        return True

    if tag == "skip-on-windows":
        pytest.mark.skipif(platform.system() == "Windows", reason="Not supported on Windows")(function)
        return True

    if tag == "sail-bug":
        # We set `strict=True` so that outdated markers are reported as errors so that we can remove them timely.
        pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)(function)
        return True

    if (m := SPARK_TAG_PATTERN.fullmatch(tag)) is not None:
        try:
            s = m.group(1)
            Version(s)
        except ValueError as e:
            msg = f"invalid Spark version tag: {tag}"
            raise ValueError(msg) from e
        pytest.mark.spark(version=f">={s}")(function)
        return True

    return None
