from __future__ import annotations

import pandas as pd
import pytest
from pytest_bdd import parsers, then, when

from pysail.testing.spark.utils.sql import parse_show_string


@when(
    parsers.parse('dataframe for table "{table_name}" with COALESCE hint {num_partitions:d}'),
    target_fixture="dataframe",
)
def dataframe_with_coalesce_hint(table_name, num_partitions, spark):
    return spark.table(table_name).hint("COALESCE", num_partitions)


def partition_count(df):
    def counter(_iterator):
        yield pd.DataFrame({"n": [1]})

    return df.mapInPandas(counter, schema="n: long").count()


@then(parsers.parse("dataframe has {num_partitions:d} partitions"))
def dataframe_has_partitions(dataframe, num_partitions):
    assert partition_count(dataframe) == num_partitions


@then(parsers.re("dataframe result(?P<ordered>( ordered)?)"))
def dataframe_result(datatable, ordered, dataframe):
    header, *rows = datatable
    [actual_header, *actual_rows] = parse_show_string(
        dataframe._show_string(n=0x7FFFFFFF, truncate=False)  # noqa: SLF001
    )
    assert header == actual_header
    if ordered:
        assert rows == actual_rows
    else:
        assert sorted(rows) == sorted(actual_rows)


@then(parsers.parse("dataframe error {error}"))
def dataframe_error(dataframe, error):
    with pytest.raises(Exception, match=error):
        _ = dataframe.count()