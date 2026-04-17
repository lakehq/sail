from __future__ import annotations

import re

from pytest_bdd import parsers, then, when


@when(parsers.parse("catalog functionExists for {function_name}"), target_fixture="function_exists_result")
def catalog_function_exists(function_name: str, spark) -> bool:
    """Invoke ``spark.catalog.functionExists`` for the given function name."""
    return spark.catalog.functionExists(function_name)


@then(parsers.parse("the function existence result is {expected}"))
def check_function_exists_result(expected: str, function_exists_result) -> None:
    expected_value = expected.strip().lower() == "true"
    assert function_exists_result == expected_value, (
        f"expected functionExists to return {expected_value}, got {function_exists_result!r}"
    )


@when(parsers.parse("catalog getFunction for {function_name}"), target_fixture="get_function_result")
def catalog_get_function(function_name: str, spark):
    """Invoke ``spark.catalog.getFunction`` for the given function name."""
    try:
        return spark.catalog.getFunction(function_name)
    except Exception as exc:  # noqa: BLE001
        return exc


@then(parsers.parse("the function attribute {attribute} is {value}"))
def check_function_attribute(attribute: str, value: str, get_function_result) -> None:
    if isinstance(get_function_result, Exception):
        msg = f"expected Function, got exception: {get_function_result!r}"
        raise AssertionError(msg)  # noqa: TRY004
    actual = getattr(get_function_result, attribute)
    expected = value.strip().lower() == "true" if isinstance(actual, bool) else value
    assert actual == expected, f"expected {attribute}={expected!r}, got {actual!r}"


@then(parsers.parse("the getFunction call raises an error matching {pattern}"))
def check_get_function_error(pattern: str, get_function_result) -> None:
    if not isinstance(get_function_result, Exception):
        msg = f"expected an exception, got: {get_function_result!r}"
        raise AssertionError(msg)  # noqa: TRY004
    assert re.search(pattern, str(get_function_result)), (
        f"expected error matching {pattern!r}, got: {get_function_result!r}"
    )
