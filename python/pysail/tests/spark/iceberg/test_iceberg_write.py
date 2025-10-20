import pandas as pd
import pyarrow as pa
import pytest
from pandas.testing import assert_frame_equal
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

from pysail.tests.spark.utils import escape_sql_string_literal


@pytest.mark.skip(reason="overwrite not supported yet")
def test_iceberg_write_overwrite_and_read(spark, sql_catalog):
    identifier = "default.write_overwrite"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="event", field_type=StringType(), required=False),
            NestedField(field_id=3, name="score", field_type=DoubleType(), required=False),
        ),
    )
    try:
        df = spark.createDataFrame(
            [(10, "A", 0.98), (11, "B", 0.54), (12, "A", 0.76)],
            schema="id LONG, event STRING, score DOUBLE",
        )
        df.write.format("iceberg").mode("overwrite").save(table.location())

        result_df = spark.read.format("iceberg").load(table.location()).sort("id")
        expected = pd.DataFrame(
            {
                "id": [10, 11, 12],
                "event": ["A", "B", "A"],
                "score": [0.98, 0.54, 0.76],
            }
        )
        assert_frame_equal(result_df.toPandas(), expected)
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_write_append_mode(spark, sql_catalog):
    identifier = "default.write_append"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="event", field_type=StringType(), required=False),
        ),
    )
    try:
        seed_df = pd.DataFrame({"id": [1, 2], "event": ["a", "b"]})
        table.append(pa.Table.from_pandas(seed_df))

        df2 = spark.createDataFrame([(3, "c"), (4, "d")], schema="id LONG, event STRING")
        df2.write.format("iceberg").mode("append").save(table.location())

        result_df = spark.read.format("iceberg").load(table.location()).sort("id")
        expected = pd.DataFrame({"id": [1, 2, 3, 4], "event": ["a", "b", "c", "d"]})
        assert_frame_equal(result_df.toPandas(), expected.astype(result_df.toPandas().dtypes))
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_sql_read_after_write(spark, sql_catalog):
    identifier = "default.write_sql_table"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        ),
    )
    try:
        seed_df = pd.DataFrame({"id": [1], "name": ["alice"]})
        table.append(pa.Table.from_pandas(seed_df))

        df = spark.createDataFrame([(2, "bob")], schema="id LONG, name STRING")
        df.write.format("iceberg").mode("append").save(table.location())

        table_path = table.location()
        spark.sql(f"CREATE TABLE tmp_ice USING iceberg LOCATION '{escape_sql_string_literal(table_path)}'")
        try:
            result_df = spark.sql("SELECT * FROM tmp_ice").sort("id")
            expected = pd.DataFrame({"id": [1, 2], "name": ["alice", "bob"]})
            assert_frame_equal(result_df.toPandas(), expected.astype(result_df.toPandas().dtypes))
        finally:
            spark.sql("DROP TABLE IF EXISTS tmp_ice")
    finally:
        sql_catalog.drop_table(identifier)
