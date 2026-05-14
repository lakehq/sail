from __future__ import annotations

import pandas as pd
import pytest
from jinja2 import Template
from pytest_bdd import given, parsers, then


@pytest.fixture
def dataframes():
    return {}


def _get_dataframe(dataframes, name):
    df = dataframes.get(name)
    assert df is not None, f"DataFrame {name!r} not found"
    return df


def _partition_count(df):
    def counter(_iterator):
        yield pd.DataFrame({"n": [1]})

    return df.mapInPandas(counter, schema="n: long").count()


@given(parsers.re(r"dataframe (?P<name>\w+) is query(?P<template>( template)?)"), target_fixture="dataframes")
def dataframe_from_query(name, template, docstring, spark, variables, dataframes):
    query = Template(docstring).render(**variables) if template else docstring
    dataframes[name] = spark.sql(query)
    return dataframes


@given(
    parsers.parse("dataframe {name} is repartition of {input_name} to {num_partitions:d} partitions"),
    target_fixture="dataframes",
)
def dataframe_repartition(name, input_name, num_partitions, dataframes):
    dataframes[name] = _get_dataframe(dataframes, input_name).repartition(num_partitions)
    return dataframes


@given(parsers.parse("dataframe {name} is registered as temp view {view_name}"))
def dataframe_registered_as_temp_view(name, view_name, dataframes):
    _get_dataframe(dataframes, name).createOrReplaceTempView(view_name)


@then(parsers.parse("dataframe {name} partition count is {expected:d}"))
def dataframe_partition_count(name, expected, dataframes):
    assert _partition_count(_get_dataframe(dataframes, name)) == expected


@then(parsers.parse("dataframe {name} distinct partition ids are {expected_ids}"))
def dataframe_distinct_partition_ids(name, expected_ids, dataframes):
    actual = {
        row["pid"]
        for row in _get_dataframe(dataframes, name).selectExpr("spark_partition_id() AS pid").distinct().collect()
    }
    expected = {int(part.strip()) for part in expected_ids.split(",") if part.strip()}
    assert actual == expected
