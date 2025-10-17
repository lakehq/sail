import pandas as pd
import pyarrow as pa
import pytest
from pandas.testing import assert_frame_equal
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

from pysail.tests.spark.utils import escape_sql_string_literal


@pytest.fixture
def iceberg_test_data():
    return [
        {"id": 10, "event": "A", "score": 0.98},
        {"id": 11, "event": "B", "score": 0.54},
        {"id": 12, "event": "A", "score": 0.76},
    ]


@pytest.fixture
def expected_pandas_df():
    return (
        pd.DataFrame({"id": [10, 11, 12], "event": ["A", "B", "A"], "score": [0.98, 0.54, 0.76]})
        .astype({"id": "int64", "score": "float64"})
        .assign(event=lambda df: df["event"].astype("object"))
    )


def test_iceberg_io_basic_read(spark, iceberg_test_data, expected_pandas_df, sql_catalog):
    table_name = "test_table"

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="event", field_type=StringType(), required=False),
        NestedField(field_id=3, name="score", field_type=DoubleType(), required=False),
    )

    table = sql_catalog.create_table(
        identifier=f"default.{table_name}",
        schema=schema,
    )

    try:
        df = pd.DataFrame(iceberg_test_data)
        arrow_table = pa.Table.from_pandas(df)
        table.append(arrow_table)

        table_path = table.location()

        result_df = spark.read.format("iceberg").load(table_path).sort("id")

        assert_frame_equal(
            result_df.toPandas(), expected_pandas_df.sort_values("id").reset_index(drop=True), check_dtype=True
        )
    finally:
        sql_catalog.drop_table(f"default.{table_name}")


def test_iceberg_io_read_with_sql(spark, iceberg_test_data, expected_pandas_df, sql_catalog):
    table_name = "test_table_sql"

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="event", field_type=StringType(), required=False),
        NestedField(field_id=3, name="score", field_type=DoubleType(), required=False),
    )

    table = sql_catalog.create_table(
        identifier=f"default.{table_name}",
        schema=schema,
    )

    try:
        df = pd.DataFrame(iceberg_test_data)
        arrow_table = pa.Table.from_pandas(df)
        table.append(arrow_table)

        table_path = table.location()

        spark.sql(f"CREATE TABLE my_iceberg USING iceberg LOCATION '{escape_sql_string_literal(table_path)}'")

        try:
            result_df = spark.sql("SELECT * FROM my_iceberg").sort("id")

            assert_frame_equal(
                result_df.toPandas(), expected_pandas_df.sort_values("id").reset_index(drop=True), check_dtype=True
            )
        finally:
            spark.sql("DROP TABLE IF EXISTS my_iceberg")
    finally:
        sql_catalog.drop_table(f"default.{table_name}")


def test_iceberg_io_multiple_files(spark, sql_catalog):
    table_name = "test_table_multiple"

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="value", field_type=StringType(), required=False),
    )

    table = sql_catalog.create_table(
        identifier=f"default.{table_name}",
        schema=schema,
    )

    try:
        df1 = pd.DataFrame([{"id": 1, "value": "a"}, {"id": 2, "value": "b"}])
        arrow_table1 = pa.Table.from_pandas(df1)
        table.append(arrow_table1)

        df2 = pd.DataFrame([{"id": 3, "value": "c"}, {"id": 4, "value": "d"}])
        arrow_table2 = pa.Table.from_pandas(df2)
        table.append(arrow_table2)

        table_path = table.location()

        result_df = spark.read.format("iceberg").load(table_path).sort("id")

        expected_data = (
            pd.DataFrame({"id": [1, 2, 3, 4], "value": ["a", "b", "c", "d"]})
            .astype({"id": "int64"})
            .assign(value=lambda df: df["value"].astype("object"))
        )

        assert_frame_equal(
            result_df.toPandas(), expected_data.sort_values("id").reset_index(drop=True), check_dtype=True
        )

        assert result_df.count() == 4  # noqa: PLR2004
    finally:
        sql_catalog.drop_table(f"default.{table_name}")
