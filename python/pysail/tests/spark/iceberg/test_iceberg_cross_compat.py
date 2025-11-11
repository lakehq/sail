import platform

import pandas as pd
import pyarrow as pa
import pytest
from pandas.testing import assert_frame_equal
from pyiceberg.schema import Schema
from pyiceberg.table import StaticTable
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

from .utils import create_sql_catalog, pyiceberg_to_pandas  # noqa: TID252


@pytest.mark.skipif(platform.system() == "Windows", reason="not working on Windows")
def test_pyiceberg_read_after_sail_overwrite(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.cross_overwrite"
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="event", field_type=StringType(), required=False),
        NestedField(field_id=3, name="score", field_type=DoubleType(), required=False),
    )
    table = catalog.create_table(identifier=identifier, schema=schema)
    try:
        df = spark.createDataFrame(
            [(10, "A", 0.98), (11, "B", 0.54), (12, "A", 0.76)],
            schema="id LONG, event STRING, score DOUBLE",
        )
        df.write.format("iceberg").mode("overwrite").save(table.location())

        py_tbl = catalog.load_table(identifier)
        expected = (
            pd.DataFrame({"id": [10, 11, 12], "event": ["A", "B", "A"], "score": [0.98, 0.54, 0.76]})
            .astype({"id": "int64", "score": "float64"})
            .assign(event=lambda d: d["event"].astype("object"))
        )
        spark_pdf = spark.read.format("iceberg").load(table.location()).sort("id").toPandas()
        assert_frame_equal(spark_pdf, expected)

        actual = pyiceberg_to_pandas(py_tbl, sort_by="id")
        assert_frame_equal(actual, expected)
    finally:
        catalog.drop_table(identifier)


@pytest.mark.skipif(platform.system() == "Windows", reason="not working on Windows")
def test_pyiceberg_read_after_sail_append(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.cross_append"
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="event", field_type=StringType(), required=False),
    )
    table = catalog.create_table(identifier=identifier, schema=schema)
    try:
        seed_df = pd.DataFrame({"id": [1, 2], "event": ["a", "b"]}).astype({"id": "int64"})
        table.append(pa.Table.from_pandas(seed_df))

        df2 = spark.createDataFrame([(3, "c"), (4, "d")], schema="id LONG, event STRING")
        df2.write.format("iceberg").mode("append").save(table.location())

        py_tbl = catalog.load_table(identifier)
        actual_py = pyiceberg_to_pandas(py_tbl, sort_by="id")

        expected = (
            pd.DataFrame({"id": [1, 2, 3, 4], "event": ["a", "b", "c", "d"]})
            .astype({"id": "int64"})
            .assign(event=lambda d: d["event"].astype("object"))
        )
        spark_df = spark.read.format("iceberg").load(table.location()).sort("id").toPandas()
        assert_frame_equal(spark_df, expected)
        # FIXME: Add support to update catalog
        expected_py = (
            pd.DataFrame({"id": [1, 2], "event": ["a", "b"]})
            .astype({"id": "int64"})
            .assign(event=lambda d: d["event"].astype("object"))
        )
        assert_frame_equal(actual_py, expected_py)
    finally:
        catalog.drop_table(identifier)


@pytest.mark.skipif(platform.system() == "Windows", reason="not working on Windows")
def test_static_table_read_after_sail_overwrite(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.static_overwrite"
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="event", field_type=StringType(), required=False),
        NestedField(field_id=3, name="score", field_type=DoubleType(), required=False),
    )
    table = catalog.create_table(identifier=identifier, schema=schema)
    try:
        df = spark.createDataFrame(
            [(10, "A", 0.98), (11, "B", 0.54), (12, "A", 0.76)],
            schema="id LONG, event STRING, score DOUBLE",
        )
        df.write.format("iceberg").mode("overwrite").save(table.location())

        static_table = StaticTable.from_metadata(table.location())

        expected = (
            pd.DataFrame({"id": [10, 11, 12], "event": ["A", "B", "A"], "score": [0.98, 0.54, 0.76]})
            .astype({"id": "int64", "score": "float64"})
            .assign(event=lambda d: d["event"].astype("object"))
        )

        spark_pdf = spark.read.format("iceberg").load(table.location()).sort("id").toPandas()
        assert_frame_equal(spark_pdf, expected)

        actual = pyiceberg_to_pandas(static_table, sort_by="id")
        assert_frame_equal(actual, expected)
    finally:
        catalog.drop_table(identifier)


@pytest.mark.skipif(platform.system() == "Windows", reason="not working on Windows")
def test_static_table_read_after_sail_append(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.static_append"
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="event", field_type=StringType(), required=False),
    )
    table = catalog.create_table(identifier=identifier, schema=schema)
    try:
        seed_df = pd.DataFrame({"id": [1, 2], "event": ["a", "b"]}).astype({"id": "int64"})
        table.append(pa.Table.from_pandas(seed_df))

        df2 = spark.createDataFrame([(3, "c"), (4, "d")], schema="id LONG, event STRING")
        df2.write.format("iceberg").mode("append").save(table.location())

        static_table = StaticTable.from_metadata(table.location())
        actual_static = pyiceberg_to_pandas(static_table, sort_by="id")

        expected = (
            pd.DataFrame({"id": [1, 2, 3, 4], "event": ["a", "b", "c", "d"]})
            .astype({"id": "int64"})
            .assign(event=lambda d: d["event"].astype("object"))
        )

        spark_df = spark.read.format("iceberg").load(table.location()).sort("id").toPandas()
        assert_frame_equal(spark_df, expected)

        assert_frame_equal(actual_static, expected)
    finally:
        catalog.drop_table(identifier)


@pytest.mark.skipif(platform.system() == "Windows", reason="not working on Windows")
def test_static_table_read_multiple_sail_writes(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.static_multiple"
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="value", field_type=StringType(), required=False),
    )
    table = catalog.create_table(identifier=identifier, schema=schema)
    try:
        operations = [
            ("overwrite", [(1, "first")]),
            ("append", [(2, "second"), (3, "third")]),
            ("append", [(4, "fourth"), (5, "fifth")]),
            ("overwrite", [(10, "overwrite")]),
            ("append", [(11, "eleven"), (12, "twelve")]),
        ]

        current_rows = []
        for mode, rows in operations:
            df = spark.createDataFrame(rows, schema="id LONG, value STRING")
            df.write.format("iceberg").mode(mode).save(table.location())

            current_rows = list(rows) if mode == "overwrite" else current_rows + list(rows)

            static_table = StaticTable.from_metadata(table.location())
            actual = pyiceberg_to_pandas(static_table, sort_by="id")

            expected = (
                pd.DataFrame(current_rows, columns=["id", "value"])
                .astype({"id": "int64"})
                .assign(value=lambda d: d["value"].astype("object"))
                .sort_values("id")
                .reset_index(drop=True)
            )

            assert_frame_equal(actual, expected)
    finally:
        catalog.drop_table(identifier)
