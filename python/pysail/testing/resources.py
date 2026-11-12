"""Weighted fixture resources for assigning tests to PySpark versions."""

from __future__ import annotations

import inspect
import math
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


F = TypeVar("F", bound="Callable")


@dataclass(frozen=True)
class Resource:
    name: str
    weight: float


def resource(*, name: str, weight: float) -> Callable[[F], F]:
    """Declare a fixture resource, placing this decorator below ``@pytest.fixture``.

    Weights are positive, finite scheduling priorities. A resource overrides
    feature-file or module affinity only when its weight exceeds 1. Consumers of the same name
    prefer the same PySpark version, subject to Spark compatibility markers.

    Static fixture dependencies propagate resources without executing fixtures.
    For dynamic dependencies, declare the resource on the requesting fixture.
    """
    if not isinstance(name, str) or not name.strip() or name != name.strip():
        msg = "Resource name must be a nonempty string without surrounding whitespace"
        raise ValueError(msg)
    if isinstance(weight, bool) or not isinstance(weight, (int, float)) or not math.isfinite(weight) or weight <= 0:
        msg = "Resource weight must be a positive finite number"
        raise ValueError(msg)

    def decorate(function: F) -> F:
        if not inspect.isfunction(function):
            msg = "Place @resource below @pytest.fixture on a fixture function"
            raise TypeError(msg)
        if hasattr(function, "__pysail_resource__"):
            msg = "A fixture may declare only one resource"
            raise ValueError(msg)
        function.__pysail_resource__ = Resource(name=name, weight=weight)
        return function

    return decorate


def fixture_resources(item: pytest.Item) -> Iterator[Resource]:
    """Inspect pytest fixture definitions without setting up their resources.

    Keep access to pytest internals here. The closest fixture definition wins,
    unless it requests the overridden fixture through its dependency chain.
    Direct parametrization replaces fixture definitions with parameter fixtures.
    """
    info = getattr(item, "_fixtureinfo", None)
    if info is None:
        return
    active = Counter()
    visited = set()

    def visit(name: str) -> Iterator[Resource]:
        definitions = info.name2fixturedefs.get(name, ())
        index = len(definitions) - 1 - active[name]
        if index < 0:
            return
        definition = definitions[index]
        if definition in visited:
            return
        visited.add(definition)
        declared = getattr(definition.func, "__pysail_resource__", None)
        if declared is not None:
            yield declared
        active[name] += 1
        for dependency in definition.argnames:
            yield from visit(dependency)
        active[name] -= 1

    for name in info.initialnames:
        yield from visit(name)


def affinity_key(item: pytest.Item, default: tuple[str, str]) -> tuple[str, str]:
    """Choose the strongest resource, rejecting conflicting declarations."""
    resources = {}
    weights = {}
    for declared in fixture_resources(item):
        previous = resources.setdefault(declared.name, declared.weight)
        if previous != declared.weight:
            msg = f"{item.nodeid}: resource {declared.name!r} has conflicting weights: {previous} and {declared.weight}"
            raise pytest.UsageError(msg)
        names = weights.setdefault(declared.weight, set())
        names.add(declared.name)
    for weight, names in sorted(weights.items()):
        if len(names) > 1:
            msg = f"{item.nodeid}: resources have equal weight {weight}: {', '.join(sorted(names))}"
            raise pytest.UsageError(msg)
    if resources:
        name = max(resources, key=resources.get)
        if resources[name] > 1:
            return "resource", name
    return default
