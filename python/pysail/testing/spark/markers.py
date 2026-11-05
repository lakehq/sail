"""Spark support metadata and collection-time skips for Python and BDD tests.

Use ``@pytest.mark.spark(version=">=4.1,<4.2", cross_version=True)`` for a UDF
that needs PySpark 4.1, or ``@pytest.mark.spark(skip_if_jvm=True, reason="...")``
for a Sail extension. ``version`` always refers to the installed PySpark client.
``cross_version`` records eligibility for a future matrix covering all versions.
Reasons describe the feature or backend difference. Each requirement gets its
own skip marker and reason, with a default reason when none is supplied.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pytest
from packaging.specifiers import SpecifierSet

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

if TYPE_CHECKING:
    from collections.abc import Iterable


@dataclass
class SparkSupport:
    version: SpecifierSet = field(default_factory=SpecifierSet)
    cross_version: bool = False
    skip_if_jvm: bool = False
    reason: str | None = None


def resolve_spark_markers(markers: Iterable[pytest.Mark]) -> list[SparkSupport]:
    """Preserve individual requirements and reject contradictory explicit flags.

    All version requirements must be met. Omitted flags do not override inherited
    values. ``cross_version`` is metadata for future test matrix selection.
    """
    supports = []
    flags = {}
    for marker in markers:
        support = SparkSupport()
        if marker.args or marker.kwargs.keys() - {"version", "cross_version", "skip_if_jvm", "reason"}:
            msg = "spark marker accepts only version, cross_version, skip_if_jvm, and reason keyword arguments"
            raise ValueError(msg)
        if "version" in marker.kwargs:
            version = marker.kwargs["version"]
            if not isinstance(version, str):
                msg = "spark marker version must be a string"
                raise TypeError(msg)
            support.version = SpecifierSet(version)
        for name in ("cross_version", "skip_if_jvm"):
            if name not in marker.kwargs:
                continue
            value = marker.kwargs[name]
            if not isinstance(value, bool):
                msg = f"spark marker {name} must be a bool"
                raise TypeError(msg)
            if name in flags and flags[name] != value:
                msg = f"conflicting spark marker values for {name}"
                raise ValueError(msg)
            flags[name] = value
            setattr(support, name, value)
        if "reason" in marker.kwargs:
            reason = marker.kwargs["reason"]
            if not isinstance(reason, str):
                msg = "spark marker reason must be a string"
                raise TypeError(msg)
            support.reason = reason
        supports.append(support)
    return supports


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        'spark(version="", cross_version=False, skip_if_jvm=False, reason=None): '
        "declare a PySpark version specifier, eligibility for testing all PySpark versions, "
        "and whether to skip on JVM Spark; repeated markers intersect version specifiers "
        "and must agree on explicitly supplied boolean flags",
    )


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # BDD tags are converted during collection and doctest markers are attached
    # by an earlier collection hook, so both go through the same resolver.
    version = ".".join(map(str, pyspark_version()))
    jvm = is_jvm_spark()
    for item in items:
        markers = list(item.iter_markers("spark"))
        if not markers:
            continue
        try:
            supports = resolve_spark_markers(markers)
        except (TypeError, ValueError) as error:
            msg = f"{item.nodeid}: {error}"
            raise pytest.UsageError(msg) from error
        for support in supports:
            if support.version:
                item.add_marker(
                    pytest.mark.skipif(
                        version not in support.version,
                        reason=support.reason or f"Requires PySpark {support.version}",
                    )
                )
            if support.skip_if_jvm:
                item.add_marker(pytest.mark.skipif(jvm, reason=support.reason or "Skipped on JVM Spark"))
