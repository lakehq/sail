from __future__ import annotations

import pandas as pd
import pytest
from pytest_bdd import given, parsers, then, when


def _partition_count(df):
    def counter(_iterator):
        yield pd.DataFrame({"n": [1]})

    return df.mapInPandas(counter, schema="n: long").count()


@given(
    parsers.parse("a range DataFrame with {rows:d} rows and {partitions:d} partitions"),
    target_fixture="range_df",
)
def range_dataframe(rows, partitions, spark):
    return spark.range(0, rows, 1, partitions)


@when(
    parsers.parse("the DataFrame is coalesced to {n:d} partitions"),
    target_fixture="result_df",
)
def coalesce_dataframe(n, range_df):
    return range_df.coalesce(n)


@when(
    parsers.parse("the COALESCE hint is applied with {n:d} partitions"),
    target_fixture="result_df",
)
def coalesce_hint(n, range_df):
    return range_df.hint("COALESCE", n)


@then(parsers.parse('the physical plan contains "{fragment}"'))
def physical_plan_contains(fragment, result_df):
    plan = result_df._explain_string()  # noqa: SLF001
    assert fragment in plan, f"Expected {fragment!r} in plan:\n{plan}"


@then(parsers.parse("the partition count is {expected:d}"))
def check_partition_count(expected, result_df):
    actual = _partition_count(result_df)
    assert actual == expected, f"Expected {expected} partitions, got {actual}"


@then(parsers.parse('the operation fails with "{message}"'))
def operation_fails_with(message, result_df):
    with pytest.raises(Exception, match=message):
        _partition_count(result_df)
