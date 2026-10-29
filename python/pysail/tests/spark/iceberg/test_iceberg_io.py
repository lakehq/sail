import json
from nturl2path import url2pathname as windows_url2pathname
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import url2pathname

import pandas as pd
import pyarrow as pa
import pytest
from pandas.testing import assert_frame_equal
from pyiceberg.avro.file import AvroFile
from pyiceberg.expressions import EqualTo
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import StaticTable
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

from pysail.testing.spark.steps.iceberg import (
    _current_manifest_list,
    _current_snapshot,
    _find_latest_metadata,
    _pyarrow_input_file,
)
from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests.spark.iceberg.utils import pyiceberg_file_io_properties


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


def metadata_file_path(location, path_converter=url2pathname):
    uri = urlsplit(location)
    path = uri.path
    if uri.netloc and uri.netloc.lower() != "localhost":
        path = f"//{uri.netloc}{path}"
    # Convert drive/UNC syntax without decoding Hadoop's literal percent escapes.
    return path_converter(path.replace("%", "%25"))


@pytest.mark.parametrize(
    ("location", "expected"),
    [
        ("file:///C:/warehouse/space%20name/v1.metadata.json", r"C:\warehouse\space%20name\v1.metadata.json"),
        (
            "file://localhost/C:/warehouse/percent%25name/v1.metadata.json",
            r"C:\warehouse\percent%25name\v1.metadata.json",
        ),
        (
            "file://server/share/literal%252Fname/hash%23name/v1.metadata.json",
            r"\\server\share\literal%252Fname\hash%23name\v1.metadata.json",
        ),
    ],
)
def test_metadata_file_uri_windows_paths(location, expected):
    assert metadata_file_path(location, windows_url2pathname) == expected


def assert_previous_metadata(metadata, previous_file, previous_location):
    entry = metadata["metadata-log"][-1]
    assert entry["metadata-file"] == previous_location
    previous_bytes = Path(metadata_file_path(entry["metadata-file"])).read_bytes()
    assert previous_bytes == previous_file.read_bytes()
    previous = json.loads(previous_bytes)
    assert entry["timestamp-ms"] == previous["last-updated-ms"]
    assert metadata["table-uuid"] == previous["table-uuid"]


@pytest.mark.parametrize("format_version", [2, 3])
def test_append_after_upgrade_preserves_manifest_summaries(spark, sql_catalog, format_version):
    identifier = "default.manifest_summary_upgrade"
    name = "manifest_summary_upgrade"
    table = sql_catalog.create_table(
        identifier,
        Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "p", LongType(), required=False),
        ),
        partition_spec=PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "p")),
        properties={"format-version": "1"},
    )
    try:
        table.append(pa.table({"id": [1, 2, 3], "p": [10, None, 30]}))
        before = {
            manifest.manifest_path: manifest.partitions for manifest in table.current_snapshot().manifests(table.io)
        }
        location = escape_sql_string_literal(table.location())
        spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{location}'").collect()
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('format-version'='{format_version}')").collect()
        spark.createDataFrame([(4, 40)], "id BIGINT, p BIGINT").writeTo(name).append()

        external = StaticTable.from_metadata(table.location(), properties=pyiceberg_file_io_properties())
        after = {
            manifest.manifest_path: manifest.partitions
            for manifest in external.current_snapshot().manifests(external.io)
        }
        assert before.keys() <= after.keys()
        for path, summaries in before.items():
            assert after[path] == summaries
        assert external.scan(row_filter=EqualTo("p", 10)).to_arrow().to_pylist() == [{"id": 1, "p": 10}]
        assert external.scan().to_arrow().num_rows == 4  # noqa: PLR2004
        rows = spark.sql(f"SELECT id, p FROM {name} WHERE p=10").collect()  # noqa: S608
        assert [tuple(row) for row in rows] == [(1, 10)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        sql_catalog.drop_table(identifier)


@pytest.mark.parametrize("format_version", [1, 2, 3])
def test_manifest_wire_schema_and_snapshot_headers(spark, tmp_path, format_version):
    name = "manifest_wire_contract"
    path = tmp_path / name
    try:
        spark.sql(
            f"CREATE TABLE {name} (id INT, ts TIMESTAMP, ntz TIMESTAMP_NTZ) USING iceberg "
            f"PARTITIONED BY (ts, ntz) LOCATION '{path.as_uri()}' "
            f"TBLPROPERTIES ('format-version'='{format_version}')"
        ).collect()
        for value in [1, 2]:
            spark.sql(
                f"INSERT INTO {name} VALUES ({value}, TIMESTAMP '2026-10-01 10:00:00', "  # noqa: S608
                "TIMESTAMP_NTZ '2026-10-01 10:00:00')"
            ).collect()
        metadata = _find_latest_metadata(path)
        snapshot = _current_snapshot(metadata)
        io = PyArrowFileIO()
        with AvroFile(_pyarrow_input_file(io, snapshot["manifest-list"])) as reader:
            header = reader.header.meta
            schema = json.loads(header["avro.schema"])
            assert header["snapshot-id"] == str(snapshot["snapshot-id"])
            assert header["parent-snapshot-id"] == str(snapshot["parent-snapshot-id"])
            assert header["format-version"] == str(format_version)
            if format_version >= 2:  # noqa: PLR2004
                assert header["sequence-number"] == str(snapshot["sequence-number"])
            if format_version == 3:  # noqa: PLR2004
                assert header["first-row-id"] == str(snapshot["first-row-id"])
            partitions = next(field for field in schema["fields"] if field["name"] == "partitions")
            array = next(branch for branch in partitions["type"] if isinstance(branch, dict))
            assert array["items"]["name"] == "r508"
        for manifest in _current_manifest_list(metadata)["manifests"]:
            with AvroFile(_pyarrow_input_file(io, manifest["manifest-path"])) as reader:
                schema = json.loads(reader.header.meta["avro.schema"])
                list(reader)  # Independently decode the payload, not just its header.
            data_file = next(field for field in schema["fields"] if field["name"] == "data_file")["type"]
            fields = {field["name"]: field for field in data_file["fields"]}
            partition = fields["partition"]["type"]
            assert partition["type"] == "record"
            annotations = {
                field["name"]: next(branch for branch in field["type"] if isinstance(branch, dict))
                for field in partition["fields"]
            }
            assert annotations["ts"]["adjust-to-utc"] is True
            assert annotations["ntz"]["adjust-to-utc"] is False
            version_three_fields = {"first_row_id", "content_offset", "content_size_in_bytes"}
            if format_version < 3:  # noqa: PLR2004
                assert not version_three_fields.intersection(fields)
            else:
                assert version_three_fields <= fields.keys()
            assert ("referenced_data_file" in fields) == (format_version >= 2)  # noqa: PLR2004
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_iceberg_io_ignore_mode(spark, tmp_path):
    path = tmp_path / "iceberg_ignore"
    # Ignore still creates a table when no table exists.
    spark.createDataFrame([(1,)], "id INT").write.format("iceberg").mode("ignore").save(str(path))
    files_before = {file.relative_to(path): file.read_bytes() for file in path.rglob("*") if file.is_file()}
    spark.createDataFrame([(2,)], "id INT").write.format("iceberg").mode("ignore").save(str(path))
    # The early no-op must not execute the input or touch any data/metadata files.
    spark.range(1).selectExpr("raise_error('ignored input was executed') AS id").write.format("iceberg").mode(
        "ignore"
    ).option("path", str(path)).save()
    assert {file.relative_to(path): file.read_bytes() for file in path.rglob("*") if file.is_file()} == files_before
    assert spark.read.format("iceberg").load(str(path)).collect() == [(1,)]


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


@pytest.mark.parametrize(
    "directory", ["iceberg_empty_table", "percent%name", "literal%2Fname", "space name", "hash#name"]
)
def test_iceberg_io_create_table_materializes_empty_metadata(spark, tmp_path, directory):
    table_location = (tmp_path / directory).as_uri()
    table_path = tmp_path / urlsplit(table_location).path.rsplit("/", 1)[-1]
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
        assert_previous_metadata(insert_metadata, metadata_v1, f"{table_location}/metadata/v1.metadata.json")
        rows = spark.sql(f"SELECT id, name FROM {table_name} ORDER BY id").collect()  # noqa: S608
        assert [(row.id, row.name) for row in rows] == [(1, "one")]

        spark.sql(f"INSERT INTO {table_name} VALUES (2, 'two')")  # noqa: S608
        append_metadata = latest_iceberg_metadata(table_path)
        assert_previous_metadata(append_metadata, metadata_v2, f"{table_location}/metadata/v2.metadata.json")

        # Existing historical keys are not repaired when a new entry is appended.
        append_metadata["metadata-log"][0]["metadata-file"] = "legacy/metadata/v1.metadata.json"
        metadata_v3 = metadata_dir / "v3.metadata.json"
        metadata_v3.write_text(json.dumps(append_metadata), encoding="utf-8")
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('custom.key' = 'value')")
        altered_metadata = latest_iceberg_metadata(table_path)
        assert altered_metadata["metadata-log"][:-1] == append_metadata["metadata-log"]
        assert_previous_metadata(altered_metadata, metadata_v3, f"{table_location}/metadata/v3.metadata.json")
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_iceberg_merge_honors_external_write_data_path(spark, tmp_path):
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
              'format-version' = '2',
              'write.merge.mode' = 'merge-on-read',
              'write.data.path' = '{escape_sql_string_literal(external_data_path.as_uri())}'
            )
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1), (2)")  # noqa: S608
        spark.sql("CREATE OR REPLACE TEMP VIEW iceberg_external_write_source AS SELECT * FROM VALUES (1), (3) AS t(id)")
        spark.sql(
            """
            MERGE INTO iceberg_external_write_path_test AS target
            USING iceberg_external_write_source AS source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET id = source.id + 10
            WHEN NOT MATCHED THEN INSERT (id) VALUES (source.id)
            """
        )

        rows = spark.sql(f"SELECT id FROM {table_name} ORDER BY id").collect()  # noqa: S608
        assert [row.id for row in rows] == [2, 3, 11]
        assert not list(table_path.rglob("*.parquet"))
        external_files = list(external_data_path.rglob("*.parquet"))
        assert any(path.name.startswith("part-") for path in external_files)
        assert any(path.name.startswith("delete-") for path in external_files)
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
        assert replacement_metadata["snapshots"] == insert_metadata["snapshots"]
        assert replacement_metadata["snapshot-log"] == insert_metadata["snapshot-log"]
        assert "main" not in replacement_metadata.get("refs", {})
        assert replacement_metadata["sort-orders"] == insert_metadata["sort-orders"]
        assert replacement_metadata.get("statistics") == insert_metadata.get("statistics")
        assert replacement_metadata.get("partition-statistics") == insert_metadata.get("partition-statistics")
        assert [field["name"] for field in replacement_metadata["schemas"][-1]["fields"]] == ["id", "name"]
        assert_previous_metadata(
            replacement_metadata,
            table_path / "metadata" / "v2.metadata.json",
            f"{table_location}/metadata/v2.metadata.json",
        )
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
        assert len(post_replace_insert_metadata["snapshots"]) == len(insert_metadata["snapshots"]) + 1
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
        insert_metadata = latest_iceberg_metadata(table_path)

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
        assert replacement_metadata["snapshots"] == insert_metadata["snapshots"]
        assert replacement_metadata["snapshot-log"] == insert_metadata["snapshot-log"]
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
        insert_metadata = latest_iceberg_metadata(table_path)

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
        assert replacement_metadata["snapshots"] == insert_metadata["snapshots"]
        assert replacement_metadata["snapshot-log"] == insert_metadata["snapshot-log"]
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


def test_iceberg_read_ignores_stale_version_hint(spark, tmp_path):
    table_path = tmp_path / "iceberg_stale_version_hint"
    table_location = table_path.as_uri()
    table_name = "iceberg_stale_version_hint_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id INT, value STRING)
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(table_location)}'
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'first')")  # noqa: S608
        spark.sql(f"INSERT INTO {table_name} VALUES (2, 'second')")  # noqa: S608
        (table_path / "metadata" / "version-hint.text").write_text("1", encoding="utf-8")

        rows = spark.read.format("iceberg").load(table_location).orderBy("id").collect()
        assert [(row.id, row.value) for row in rows] == [(1, "first"), (2, "second")]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
