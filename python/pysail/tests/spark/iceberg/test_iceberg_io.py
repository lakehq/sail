import json

import pandas as pd
import pyarrow as pa
import pytest
from pandas.testing import assert_frame_equal
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

from pysail.testing.spark.utils.sql import escape_sql_string_literal


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


def latest_iceberg_metadata(table_path):
    metadata_dir = table_path / "metadata"
    hint = (metadata_dir / "version-hint.text").read_text(encoding="utf-8").strip()
    if hint.isdigit():
        metadata_file = metadata_dir / f"v{hint}.metadata.json"
    else:
        file_name = hint if hint.endswith(".metadata.json") else f"{hint}.metadata.json"
        metadata_file = metadata_dir / file_name
    return json.loads(metadata_file.read_text(encoding="utf-8"))


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


def test_iceberg_io_create_table_materializes_empty_metadata(spark, tmp_path):
    table_path = tmp_path / "iceberg_empty_table"
    table_location = table_path.as_uri()
    table_name = "iceberg_empty_materialized_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (
              id BIGINT,
              name STRING
            )
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(table_location)}'
            """
        )

        metadata_dir = table_path / "metadata"
        assert metadata_dir.exists()
        assert (metadata_dir / "version-hint.text").read_text(encoding="utf-8").strip() == "1"
        metadata_v1 = metadata_dir / "v1.metadata.json"
        assert metadata_v1.exists()
        create_metadata = json.loads(metadata_v1.read_text(encoding="utf-8"))
        assert create_metadata["current-snapshot-id"] == -1
        assert create_metadata["snapshots"] == []
        assert spark.sql(f"SELECT id, name FROM {table_name} ORDER BY id").collect() == []  # noqa: S608

        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'one')")  # noqa: S608
        assert (metadata_dir / "version-hint.text").read_text(encoding="utf-8").strip() == "2"
        metadata_v2 = metadata_dir / "v2.metadata.json"
        assert metadata_v2.exists()
        insert_metadata = json.loads(metadata_v2.read_text(encoding="utf-8"))
        assert insert_metadata["current-snapshot-id"] != -1
        assert len(insert_metadata["snapshots"]) == 1
        assert len(insert_metadata["metadata-log"]) == 1
        assert insert_metadata["metadata-log"][0]["metadata-file"].endswith("/metadata/v1.metadata.json")
        assert insert_metadata["metadata-log"][0]["timestamp-ms"] == create_metadata["last-updated-ms"]
        rows = spark.sql(f"SELECT id, name FROM {table_name} ORDER BY id").collect()  # noqa: S608
        assert [(row.id, row.name) for row in rows] == [(1, "one")]

        spark.sql(f"INSERT INTO {table_name} VALUES (2, 'two')")  # noqa: S608
        append_metadata = latest_iceberg_metadata(table_path)
        assert append_metadata["metadata-log"][-1]["metadata-file"].endswith("/metadata/v2.metadata.json")
        assert append_metadata["metadata-log"][-1]["timestamp-ms"] == insert_metadata["last-updated-ms"]

        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('custom.key' = 'value')")
        altered_metadata = latest_iceberg_metadata(table_path)
        assert altered_metadata["metadata-log"][-1]["metadata-file"].endswith("/metadata/v3.metadata.json")
        assert altered_metadata["metadata-log"][-1]["timestamp-ms"] == append_metadata["last-updated-ms"]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_iceberg_io_rejects_external_write_data_path_without_side_effects(spark, tmp_path):
    table_path = tmp_path / "iceberg_external_write_path"
    external_data_path = tmp_path / "external_data"
    table_name = "iceberg_external_write_path_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id BIGINT)
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(table_path.as_uri())}'
            TBLPROPERTIES (
              'write.data.path' = '{escape_sql_string_literal(external_data_path.as_uri())}'
            )
            """
        )
        before_metadata = latest_iceberg_metadata(table_path)

        with pytest.raises(Exception, match=r"external Iceberg write paths are not supported"):
            spark.sql(f"INSERT INTO {table_name} VALUES (1)").collect()  # noqa: S608

        assert latest_iceberg_metadata(table_path) == before_metadata
        assert not list(table_path.rglob("*.parquet"))
        assert not list(external_data_path.rglob("*.parquet"))
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_iceberg_io_create_or_replace_existing_table_replaces_metadata_and_clears_rows(spark, tmp_path):
    table_path = tmp_path / "iceberg_replace_existing"
    table_location = table_path.as_uri()
    table_name = "iceberg_create_or_replace_existing_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (
              id BIGINT,
              name STRING
            )
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(table_location)}'
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'one')")  # noqa: S608
        insert_metadata = latest_iceberg_metadata(table_path)
        previous_sequence_number = insert_metadata["last-sequence-number"]

        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {table_name} (
              id BIGINT,
              name STRING
            )
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(table_location)}'
            """
        )

        rows = spark.sql(f"SELECT id, name FROM {table_name} ORDER BY id").collect()  # noqa: S608
        assert rows == []

        replacement_metadata = latest_iceberg_metadata(table_path)
        assert replacement_metadata["current-snapshot-id"] == -1
        assert replacement_metadata["snapshots"] == []
        assert [field["name"] for field in replacement_metadata["schemas"][-1]["fields"]] == ["id", "name"]
        assert replacement_metadata["metadata-log"][-1]["metadata-file"].endswith("/metadata/v2.metadata.json")
        assert replacement_metadata["metadata-log"][-1]["timestamp-ms"] == insert_metadata["last-updated-ms"]
        assert replacement_metadata["last-sequence-number"] == previous_sequence_number

        spark.sql(f"INSERT INTO {table_name} VALUES (2, 'two')")  # noqa: S608
        post_replace_insert_metadata = latest_iceberg_metadata(table_path)
        current_snapshot_id = post_replace_insert_metadata["current-snapshot-id"]
        current_snapshot = next(
            snapshot
            for snapshot in post_replace_insert_metadata["snapshots"]
            if snapshot["snapshot-id"] == current_snapshot_id
        )
        assert current_snapshot["sequence-number"] == previous_sequence_number + 1
        assert post_replace_insert_metadata["last-sequence-number"] == previous_sequence_number + 1
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_iceberg_io_create_or_replace_v3_table_preserves_next_row_id(spark, tmp_path):
    table_path = tmp_path / "iceberg_replace_v3_row_id"
    table_location = table_path.as_uri()
    table_name = "iceberg_create_or_replace_v3_row_id_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id BIGINT)
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(table_location)}'
            TBLPROPERTIES ('format-version' = '3')
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1), (2), (3)")  # noqa: S608
        insert_metadata = latest_iceberg_metadata(table_path)
        assert insert_metadata["next-row-id"] == 3  # noqa: PLR2004

        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {table_name} (id BIGINT)
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(table_location)}'
            TBLPROPERTIES ('format-version' = '3')
            """
        )

        replacement_metadata = latest_iceberg_metadata(table_path)
        assert replacement_metadata["format-version"] == 3  # noqa: PLR2004
        assert replacement_metadata["next-row-id"] == insert_metadata["next-row-id"]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_iceberg_io_create_or_replace_existing_table_changes_schema(spark, tmp_path):
    table_path = tmp_path / "iceberg_replace_schema_change"
    table_name = "iceberg_create_or_replace_schema_change_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (
              id BIGINT,
              name STRING
            )
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(str(table_path))}'
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'one')")  # noqa: S608

        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {table_name} (
              replacement STRING
            )
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(str(table_path))}'
            """
        )

        rows = spark.sql(f"SELECT replacement FROM {table_name}").collect()  # noqa: S608
        assert rows == []

        replacement_metadata = latest_iceberg_metadata(table_path)
        assert replacement_metadata["current-snapshot-id"] == -1
        assert replacement_metadata["snapshots"] == []
        assert [field["name"] for field in replacement_metadata["schemas"][-1]["fields"]] == ["replacement"]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_iceberg_io_create_or_replace_existing_table_without_schema_creates_empty_schema(spark, tmp_path):
    table_path = tmp_path / "iceberg_replace_no_schema"
    table_name = "iceberg_create_or_replace_no_schema_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (
              id BIGINT
            )
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(str(table_path))}'
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1)")  # noqa: S608

        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {table_name}
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(str(table_path))}'
            """
        )

        assert spark.table(table_name).schema.fields == []
        assert spark.sql(f"SELECT * FROM {table_name}").collect() == []  # noqa: S608

        replacement_metadata = latest_iceberg_metadata(table_path)
        assert replacement_metadata["current-snapshot-id"] == -1
        assert replacement_metadata["snapshots"] == []
        assert replacement_metadata["schemas"][-1]["fields"] == []
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_iceberg_io_replace_table_missing_errors_without_materializing_metadata(spark, tmp_path):
    table_path = tmp_path / "iceberg_replace_missing"
    table_name = "iceberg_replace_missing_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        with pytest.raises(Exception, match=r"(?i)(not found|does not exist|table_or_view_not_found)"):
            spark.sql(
                f"""
                REPLACE TABLE {table_name} (
                  id BIGINT
                )
                USING ICEBERG
                LOCATION '{escape_sql_string_literal(str(table_path))}'
                """
            )

        assert not (table_path / "metadata").exists()
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_iceberg_io_replace_table_as_select_missing_errors_without_materializing_metadata(spark, tmp_path):
    table_path = tmp_path / "iceberg_replace_as_select_missing"
    table_name = "iceberg_replace_as_select_missing_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        with pytest.raises(Exception, match=r"(?i)(not found|does not exist|table_or_view_not_found)"):
            spark.sql(
                f"""
                REPLACE TABLE {table_name}
                USING ICEBERG
                LOCATION '{escape_sql_string_literal(str(table_path))}'
                AS SELECT 1 AS id
                """
            )

        assert not (table_path / "metadata").exists()
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_iceberg_io_create_table_if_not_exists_does_not_materialize_new_location(spark, tmp_path):
    table_path = tmp_path / "iceberg_if_not_exists_table"
    alternate_path = tmp_path / "iceberg_if_not_exists_alternate"
    table_location = table_path.as_uri()
    alternate_location = alternate_path.as_uri()
    table_name = "iceberg_if_not_exists_materialized_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (
              id BIGINT,
              name STRING
            )
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(table_location)}'
            """
        )
        assert (table_path / "metadata" / "version-hint.text").exists()

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
              id BIGINT,
              name STRING
            )
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(alternate_location)}'
            """
        )

        assert not (alternate_path / "metadata").exists()
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_iceberg_io_create_table_rejects_existing_metadata_location(spark, tmp_path):
    table_path = tmp_path / "iceberg_existing_metadata"
    table_location = table_path.as_uri()
    first_table = "iceberg_existing_metadata_first_test"
    second_table = "iceberg_existing_metadata_second_test"

    spark.sql(f"DROP TABLE IF EXISTS {first_table}")
    spark.sql(f"DROP TABLE IF EXISTS {second_table}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {first_table} (
              id BIGINT,
              name STRING
            )
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(table_location)}'
            """
        )

        with pytest.raises(Exception, match="metadata already exists"):
            spark.sql(
                f"""
                CREATE TABLE {second_table} (
                  id BIGINT,
                  name STRING
                )
                USING ICEBERG
                LOCATION '{escape_sql_string_literal(table_location)}'
                """
            )
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {second_table}")
        spark.sql(f"DROP TABLE IF EXISTS {first_table}")


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
