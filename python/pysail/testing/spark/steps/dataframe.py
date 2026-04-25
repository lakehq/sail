from __future__ import annotations

import math

import pytest
from pytest_bdd import parsers, then, when
from pyspark.sql import Row
from pyspark.sql import functions as sf


def _sample_by_input(spark):
    return spark.createDataFrame(
        [
            Row(key=0, value=0, payload="zero"),
            Row(key=1, value=1, payload="one"),
            Row(key=2, value=2, payload="two"),
            Row(key=0, value=3, payload="three"),
        ]
    )


@when("DataFrame sampleBy selects matching strata", target_fixture="dataframe")
def dataframe_sample_by_matching_strata(spark):
    return _sample_by_input(spark).sampleBy("key", {0: 1.0, 1: 0.0, 2: 1.0}, seed=0).orderBy("key", "value")


@when("DataFrameStatFunctions sampleBy has no matching strata", target_fixture="dataframe")
def dataframe_stat_sample_by_no_matching_strata(spark):
    return _sample_by_input(spark).stat.sampleBy("key", {9: 1.0}, seed=0)


@when("DataFrame sampleBy has empty fractions", target_fixture="dataframe")
def dataframe_sample_by_empty_fractions(spark):
    return _sample_by_input(spark).sampleBy("key", {}, seed=0)


@when("DataFrame sampleBy uses a column expression", target_fixture="dataframe")
def dataframe_sample_by_column_expression(spark):
    return _sample_by_input(spark).sampleBy(sf.col("key") + sf.lit(1), {1: 1.0}, seed=0).orderBy("value")


@when(parsers.parse("DataFrame sampleBy with fraction {fraction} is collected with error {error}"))
def dataframe_sample_by_invalid_fraction(spark, fraction: str, error: str):
    value = math.nan if fraction == "NaN" else float(fraction)
    with pytest.raises(Exception, match=error):
        _sample_by_input(spark).sampleBy("key", {0: value}, seed=0).collect()


@then("DataFrame columns")
def dataframe_columns(datatable, dataframe):
    header, *rows = datatable
    assert not rows
    assert list(dataframe.columns) == header


@then(parsers.re("DataFrame result(?P<ordered>( ordered)?)"))
def dataframe_result(datatable, ordered, dataframe):
    header, *rows = datatable
    assert list(dataframe.columns) == header
    actual = [[str(row[column]) if row[column] is not None else "NULL" for column in header] for row in dataframe.collect()]
    if ordered:
        assert actual == rows
    else:
        assert sorted(actual) == sorted(rows)


@then(parsers.parse("DataFrame row count is {count:d}"))
def dataframe_row_count(dataframe, count: int):
    assert dataframe.count() == count
