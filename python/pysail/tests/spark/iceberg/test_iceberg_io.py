import pandas as pd
import pyarrow as pa
import pytest
from pandas.testing import assert_frame_equal
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType


class TestIcebergIO:
    @pytest.fixture(scope="class")
    def iceberg_test_data(self):
        return [
            {"id": 10, "event": "A", "score": 0.98},
            {"id": 11, "event": "B", "score": 0.54},
            {"id": 12, "event": "A", "score": 0.76},
        ]

    @pytest.fixture(scope="class")
    def expected_pandas_df(self):
        return pd.DataFrame({"id": [10, 11, 12], "event": ["A", "B", "A"], "score": [0.98, 0.54, 0.76]}).astype(
            {"id": "int64", "event": "string", "score": "float64"}
        )

    def test_iceberg_io_basic_read(self, spark, iceberg_test_data, expected_pandas_df, tmp_path):
        warehouse_path = tmp_path / "warehouse"
        warehouse_path.mkdir()
        table_name = "test_table"

        catalog = load_catalog(
            "test_catalog",
            type="sql",
            uri=f"sqlite:///{tmp_path}/pyiceberg_catalog.db",
            warehouse=f"file://{warehouse_path}",
        )

        catalog.create_namespace("default")

        schema = Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="event", field_type=StringType(), required=False),
            NestedField(field_id=3, name="score", field_type=DoubleType(), required=False),
        )

        table = catalog.create_table(
            identifier=f"default.{table_name}",
            schema=schema,
        )

        try:
            df = pd.DataFrame(iceberg_test_data)
            arrow_table = pa.Table.from_pandas(df)
            table.append(arrow_table)

            table_location = table.metadata_location
            # TODO: Keep file:// prefix for Sail, just remove /metadata/... suffix
            table_path = table_location.rsplit("/metadata/", 1)[0]

            result_df = spark.read.format("iceberg").load(table_path).sort("id")

            assert_frame_equal(
                result_df.toPandas(), expected_pandas_df.sort_values("id").reset_index(drop=True), check_dtype=False
            )
        finally:
            catalog.drop_table(f"default.{table_name}")

    def test_iceberg_io_read_with_sql(self, spark, iceberg_test_data, expected_pandas_df, tmp_path):
        warehouse_path = tmp_path / "warehouse"
        warehouse_path.mkdir()
        table_name = "test_table_sql"

        catalog = load_catalog(
            "test_catalog",
            type="sql",
            uri=f"sqlite:///{tmp_path}/pyiceberg_catalog.db",
            warehouse=f"file://{warehouse_path}",
        )

        catalog.create_namespace("default")

        schema = Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="event", field_type=StringType(), required=False),
            NestedField(field_id=3, name="score", field_type=DoubleType(), required=False),
        )

        table = catalog.create_table(
            identifier=f"default.{table_name}",
            schema=schema,
        )

        try:
            df = pd.DataFrame(iceberg_test_data)
            arrow_table = pa.Table.from_pandas(df)
            table.append(arrow_table)

            table_location = table.metadata_location
            # TODO: Keep file:// prefix for Sail, just remove /metadata/... suffix
            table_path = table_location.rsplit("/metadata/", 1)[0]

            spark.sql(f"CREATE TABLE my_iceberg USING iceberg LOCATION '{table_path}'")

            try:
                result_df = spark.sql("SELECT * FROM my_iceberg").sort("id")

                assert_frame_equal(
                    result_df.toPandas(), expected_pandas_df.sort_values("id").reset_index(drop=True), check_dtype=False
                )
            finally:
                spark.sql("DROP TABLE IF EXISTS my_iceberg")
        finally:
            catalog.drop_table(f"default.{table_name}")

    def test_iceberg_io_multiple_files(self, spark, tmp_path):
        warehouse_path = tmp_path / "warehouse"
        warehouse_path.mkdir()
        table_name = "test_table_multiple"

        catalog = load_catalog(
            "test_catalog",
            type="sql",
            uri=f"sqlite:///{tmp_path}/pyiceberg_catalog.db",
            warehouse=f"file://{warehouse_path}",
        )

        catalog.create_namespace("default")

        schema = Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="value", field_type=StringType(), required=False),
        )

        table = catalog.create_table(
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

            table_location = table.metadata_location
            # TODO: Keep file:// prefix for Sail, just remove /metadata/... suffix
            table_path = table_location.rsplit("/metadata/", 1)[0]

            result_df = spark.read.format("iceberg").load(table_path).sort("id")

            expected_data = pd.DataFrame({"id": [1, 2, 3, 4], "value": ["a", "b", "c", "d"]}).astype(
                {"id": "int64", "value": "string"}
            )

            assert_frame_equal(
                result_df.toPandas(), expected_data.sort_values("id").reset_index(drop=True), check_dtype=False
            )

            assert result_df.count() == 4  # noqa: PLR2004
        finally:
            catalog.drop_table(f"default.{table_name}")
