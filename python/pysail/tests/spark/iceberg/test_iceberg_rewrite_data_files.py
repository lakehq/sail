import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField, StringType, StructType


def test_rewrite_preserves_imported_name_mapping(spark, sql_catalog, tmp_path):
    name = "rewrite_imported_names"
    table = sql_catalog.create_table(
        f"default.{name}",
        Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "payload", StructType(NestedField(3, "old", LongType(), required=False)), required=False),
        ),
    )
    imported = tmp_path / "imported.parquet"
    pq.write_table(
        pa.table({"id": [1, 2], "payload": pa.array([{"old": 10}, None], pa.struct([("old", pa.int64())]))}),
        imported,
    )
    table.add_files([imported.as_uri()])
    with table.update_schema() as update:
        update.rename_column("id", "key")
        update.rename_column(("payload", "old"), "value")
    spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{table.location()}'")
    try:
        expected = [{"key": 1, "payload": {"value": 10}}, {"key": 2, "payload": None}]
        assert [row.asDict(recursive=True) for row in spark.table(name).orderBy("key").collect()] == expected

        result = spark.sql(f"CALL system.rewrite_data_files('{name}', options => map('rewrite-all', 'true'))").first()

        assert result.rewritten_data_files_count == 1
        assert [row.asDict(recursive=True) for row in spark.table(name).orderBy("key").collect()] == expected
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        sql_catalog.drop_table(f"default.{name}")


def test_rewrite_preserves_omitted_identity_partition_values(spark, sql_catalog, tmp_path):
    name = "rewrite_identity_values"
    table = sql_catalog.create_table(
        f"default.{name}",
        Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "p", StringType(), required=False),
        ),
        partition_spec=PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "p")),
    )
    imported = tmp_path / "partitioned.parquet"
    pq.write_table(
        pa.Table.from_arrays(
            [pa.array([1, 2])],
            schema=pa.schema(
                [
                    pa.field("id", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),
                ]
            ),
        ),
        imported,
    )
    data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=imported.as_uri(),
        file_format=FileFormat.PARQUET,
        partition=Record("x"),
        record_count=2,
        file_size_in_bytes=imported.stat().st_size,
        spec_id=table.spec().spec_id,
    )
    with table.transaction() as transaction, transaction.update_snapshot().fast_append() as append:
        append.append_data_file(data_file)
    spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{table.location()}'")
    try:
        expected = [(1, "x"), (2, "x")]
        assert [tuple(row) for row in spark.table(name).select("id", "p").orderBy("id").collect()] == expected

        result = spark.sql(f"CALL system.rewrite_data_files('{name}', options => map('rewrite-all', 'true'))").first()

        assert result.rewritten_data_files_count == 1
        assert [tuple(row) for row in spark.table(name).select("id", "p").orderBy("id").collect()] == expected
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        sql_catalog.drop_table(f"default.{name}")


@pytest.mark.parametrize("files_per_group", [1, 2])
def test_rewrite_thresholds_apply_after_group_size_limit(spark, tmp_path, files_per_group):
    name = "rewrite_group_thresholds"
    location = (tmp_path / name).as_uri()
    spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg LOCATION '{location}'")
    try:
        for value in range(6):
            spark.sql(f"INSERT INTO {name} VALUES ({value})")  # noqa: S608
        files = spark.table(f"{name}.files").select("file_path", "file_size_in_bytes").orderBy("file_path").collect()
        snapshots = spark.table(f"{name}.snapshots").orderBy("snapshot_id").collect()
        group_size = max(row.file_size_in_bytes for row in files) * files_per_group
        statement = (
            f"CALL system.rewrite_data_files('{name}', options => map("
            f"'max-file-group-size-bytes', '{group_size}', "
            "'target-file-size-bytes', '1048576', 'min-input-files', '2'))"
        )
        result = spark.sql(statement).first()
        if files_per_group == 1:
            assert tuple(result) == (0, 0, 0, 0, 0)
            assert tuple(spark.sql(statement).first()) == (0, 0, 0, 0, 0)
            assert spark.table(f"{name}.snapshots").orderBy("snapshot_id").collect() == snapshots
            assert (
                spark.table(f"{name}.files").select("file_path", "file_size_in_bytes").orderBy("file_path").collect()
                == files
            )
        else:
            assert result.rewritten_data_files_count == len(files)
            assert result.added_data_files_count == len(files) // files_per_group
        assert [row.id for row in spark.table(name).orderBy("id").collect()] == list(range(6))
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
