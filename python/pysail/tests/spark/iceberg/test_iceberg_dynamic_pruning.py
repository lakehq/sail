from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat, ManifestContent
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.puffin import PuffinFile
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, LongType, NestedField

from pysail.tests.spark.iceberg.test_iceberg_merge import _current_manifest_entries, _local_file_path
from pysail.tests.spark.iceberg.utils import create_sql_catalog


@pytest.mark.parametrize("partitioned", [False, True], ids=["manifest-statistics", "identity-partitions"])
@pytest.mark.parametrize("metadata_as_data", [False, True])
def test_dynamic_join_skips_unreadable_files(spark, tmp_path, partitioned, metadata_as_data):
    selected_key = 2
    catalog = create_sql_catalog(tmp_path)
    table = catalog.create_table(
        "default.dynamic_pruning",
        schema=Schema(
            NestedField(1, "payload", LongType(), required=False),
            NestedField(2, "key", IntegerType(), required=False),
        ),
        partition_spec=PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "key"))
        if partitioned
        else PartitionSpec(),
    )
    try:
        for key in range(4):
            payload = pa.array([key * 10, key * 10 + 1], pa.int64())
            if partitioned:
                file = tmp_path / f"part-{key}.parquet"
                pq.write_table(
                    pa.Table.from_arrays(
                        [payload],
                        schema=pa.schema(
                            [
                                pa.field("payload", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),
                            ]
                        ),
                    ),
                    file,
                )
                data_file = DataFile.from_args(
                    content=DataFileContent.DATA,
                    file_path=file.as_uri(),
                    file_format=FileFormat.PARQUET,
                    partition=Record(key),
                    record_count=2,
                    file_size_in_bytes=file.stat().st_size,
                    spec_id=table.spec().spec_id,
                )
                with table.transaction() as transaction, transaction.update_snapshot().fast_append() as append:
                    append.append_data_file(data_file)
            else:
                table.append(pa.table({"payload": payload, "key": pa.array([key, key], pa.int32())}))
        frame = (
            spark.read.format("iceberg")
            .option("metadataAsDataRead", str(metadata_as_data).lower())
            .load(table.location())
        )
        keys = spark.createDataFrame([(selected_key,), (selected_key,)], "key int")
        joined = frame.select("key", "payload").join(keys, "key").select("payload", "key")
        expected = [(20, 2), (20, 2), (21, 2), (21, 2)]
        assert sorted(tuple(row) for row in joined.collect()) == expected
        for task in table.scan().plan_files():
            file = Path(unquote(urlparse(task.file.file_path).path))
            key = (
                task.file.partition[0] if partitioned else pq.ParquetFile(file).read(columns=["key"])["key"][0].as_py()
            )
            if key != selected_key:
                file.unlink()
        assert sorted(tuple(row) for row in joined.collect()) == expected
        with pytest.raises(Exception, match=r"(?i)(not found|no such file)"):
            frame.collect()
    finally:
        catalog.drop_table("default.dynamic_pruning")


def test_streaming_partitioned_limit_spans_file_groups(spark, tmp_path):
    path = tmp_path / "partitioned_limit"
    spark.createDataFrame(
        [(key, key * 10 + offset) for key in range(4) for offset in range(2)], "key int, value long"
    ).write.format("iceberg").partitionBy("key").save(path.as_uri())
    frame = spark.read.format("iceberg").option("metadataAsDataRead", "true").load(path.as_uri())
    rows = frame.select("value", "key").limit(5).collect()
    assert len(rows) == 5  # noqa: PLR2004
    assert all(row.value in (row.key * 10, row.key * 10 + 1) for row in rows)


@pytest.mark.parametrize("metadata_as_data", [False, True])
@pytest.mark.parametrize("deletes", [False, True], ids=["clean", "dv"])
@pytest.mark.parametrize(
    ("partitioning", "key_type", "values"),
    [
        ("bucket(16, key)", "INT", [-5, -4, -3, -2]),
        ("truncate(10, key)", "INT", [-21, -11, 1, 11]),
        (
            "days(key)",
            "TIMESTAMP_NTZ",
            [datetime.fromisoformat(value) for value in ["1969-12-30", "1969-12-31", "1970-01-01", "1970-01-02"]],
        ),
    ],
    ids=["bucket", "truncate", "day"],
)
def test_dynamic_partition_transforms_without_metrics(
    spark, tmp_path, metadata_as_data, deletes, partitioning, key_type, values
):
    name = "dynamic_transforms"
    path = tmp_path / name
    spark.sql(f"""CREATE TABLE {name} (payload BIGINT, key {key_type}) USING iceberg
        PARTITIONED BY ({partitioning}) LOCATION '{path.as_uri()}'
        TBLPROPERTIES ('format-version'='3', 'write.merge.mode'='merge-on-read',
                      'write.metadata.metrics.default'='none')""")
    moved = []
    try:
        for index, key in enumerate(values):
            rows = spark.createDataFrame(
                [(index * 10 + offset, key) for offset in range(3)], f"payload long, key {key_type}"
            )
            rows.coalesce(1).writeTo(name).append()
            if deletes:
                rows.filter(f"payload = {index * 10}").createOrReplaceTempView("dynamic_delete_key")
                spark.sql(f"""MERGE INTO {name} t USING dynamic_delete_key s
                    ON t.key=s.key AND t.payload=s.payload WHEN MATCHED THEN DELETE""").collect()
        data_files = [entry.data_file for entry in _current_manifest_entries(path, ManifestContent.DATA)]
        assert len(data_files) == 4  # noqa: PLR2004
        assert len({file.partition[0] for file in data_files}) == 4  # noqa: PLR2004
        assert all(not file.lower_bounds and not file.upper_bounds for file in data_files)
        frame = (
            spark.read.format("iceberg").option("metadataAsDataRead", str(metadata_as_data).lower()).load(path.as_uri())
        )
        keys = spark.createDataFrame([(values[2],), (values[2],)], f"key {key_type}")
        joined = frame.select("key", "payload").join(keys, "key").select("payload")
        expected = [21, 21, 22, 22] if deletes else [20, 20, 21, 21, 22, 22]
        assert sorted(row.payload for row in joined.collect()) == expected
        excluded = set()
        for file in data_files:
            source = _local_file_path(file.file_path)
            if pq.ParquetFile(source).read(columns=["key"])["key"][0].as_py() != values[2]:
                excluded.add(file.file_path)
        skipped = [_local_file_path(file) for file in excluded]
        if deletes:
            for entry in _current_manifest_entries(path, ManifestContent.DELETES):
                source = _local_file_path(entry.data_file.file_path)
                references = {
                    blob.properties["referenced-data-file"] for blob in PuffinFile(source.read_bytes()).footer.blobs
                }
                if references <= excluded:
                    skipped.append(source)
        assert len(skipped) == (6 if deletes else 3)
        for index, source in enumerate(skipped):
            backup = tmp_path / f"excluded-{index}"
            source.rename(backup)
            moved.append((source, backup))
        assert sorted(row.payload for row in joined.collect()) == expected
        with pytest.raises(Exception, match=r"(?i)(not found|no such file)"):
            frame.collect()
    finally:
        for source, backup in moved:
            backup.rename(source)
        if deletes:
            spark.catalog.dropTempView("dynamic_delete_key")
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("metadata_as_data", [False, True])
def test_predicate_does_not_evaluate_deleted_values(spark, tmp_path, metadata_as_data):
    name = "deleted_predicate_values"
    spark.sql(f"""CREATE TABLE {name} (key INT) USING iceberg LOCATION '{(tmp_path / name).as_uri()}'
        TBLPROPERTIES ('format-version'='3', 'write.merge.mode'='merge-on-read')""")
    try:
        spark.sql(f"INSERT INTO {name} VALUES (0), (1), (2)")  # noqa: S608
        spark.sql(f"""MERGE INTO {name} t USING (SELECT 0 AS key) s
            ON t.key=s.key WHEN MATCHED THEN DELETE""").collect()
        frame = (
            spark.read.format("iceberg")
            .option("metadataAsDataRead", str(metadata_as_data).lower())
            .load((tmp_path / name).as_uri())
        )
        assert sorted(row.key for row in frame.where("100 DIV key > 0").collect()) == [1, 2]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("format_version", [2, 3], ids=["position-delete", "dv"])
@pytest.mark.parametrize("metadata_as_data", [False, True])
@pytest.mark.parametrize("partitioned", [False, True], ids=["manifest-statistics", "identity-no-metrics"])
def test_dynamic_join_prunes_files_with_deletes(spark, tmp_path, format_version, metadata_as_data, partitioned):
    path = tmp_path / "dynamic_deletes"
    name = "dynamic_deletes"
    selected_key = 2
    uses_vectors = format_version == 3  # noqa: PLR2004
    partitioning = "PARTITIONED BY (key)" if partitioned else ""
    metrics = "none" if partitioned else "full"
    spark.sql(f"""CREATE TABLE {name} (payload BIGINT, key INT) USING iceberg {partitioning}
        LOCATION '{path.as_uri()}' TBLPROPERTIES ('format-version'='{format_version}',
            'write.merge.mode'='merge-on-read', 'write.metadata.metrics.default'='{metrics}')""")
    moved = []
    try:
        for key in range(4):
            spark.createDataFrame([(key * 10 + i, key) for i in range(3)], "payload long, key int").coalesce(1).writeTo(
                name
            ).append()
            # Separate commits keep each file's deletion vector in its own Puffin file.
            spark.sql(f"""MERGE INTO {name} t
                USING (SELECT {key * 10}L AS payload, {key} AS key) s
                ON t.key=s.key AND t.payload=s.payload WHEN MATCHED THEN DELETE""").collect()
        data_files = [entry.data_file for entry in _current_manifest_entries(path, ManifestContent.DATA)]
        delete_files = [entry.data_file for entry in _current_manifest_entries(path, ManifestContent.DELETES)]
        assert len(data_files) == len(delete_files) == 4  # noqa: PLR2004
        assert all(file.content == DataFileContent.POSITION_DELETES for file in delete_files)
        assert all(
            file.file_format == (FileFormat.PUFFIN if uses_vectors else FileFormat.PARQUET) for file in delete_files
        )
        if partitioned:
            assert all(not file.lower_bounds and not file.upper_bounds for file in data_files)
        frame = (
            spark.read.format("iceberg").option("metadataAsDataRead", str(metadata_as_data).lower()).load(path.as_uri())
        )
        keys = spark.createDataFrame([(selected_key,), (selected_key,)], "key int")
        joined = frame.select("key", "payload").join(keys, "key").select("payload", "key")
        expected = [(21, 2), (21, 2), (22, 2), (22, 2)]
        assert sorted(tuple(row) for row in joined.collect()) == expected
        excluded = set()
        for file in data_files:
            source = _local_file_path(file.file_path)
            key = file.partition[0] if partitioned else pq.ParquetFile(source).read(columns=["key"])["key"][0].as_py()
            if key != selected_key:
                excluded.add(file.file_path)
        skipped = [_local_file_path(file) for file in excluded]
        for file in delete_files:
            referenced = (
                {
                    blob.properties["referenced-data-file"]
                    for blob in PuffinFile(_local_file_path(file.file_path).read_bytes()).footer.blobs
                }
                if uses_vectors
                else set(
                    pq.ParquetFile(_local_file_path(file.file_path))
                    .read(columns=["file_path"])["file_path"]
                    .to_pylist()
                )
            )
            # Unpartitioned v2 deletes can also be routed to the selected file.
            if referenced <= excluded and (uses_vectors or partitioned):
                skipped.append(_local_file_path(file.file_path))
        expected_skips = 6 if uses_vectors or partitioned else 3
        assert len(skipped) == expected_skips
        for index, source in enumerate(skipped):
            backup = tmp_path / f"unread-{index}"
            source.rename(backup)
            moved.append((source, backup))
        assert sorted(tuple(row) for row in joined.collect()) == expected
        with pytest.raises(Exception, match=r"(?i)(not found|no such file)"):
            frame.collect()
    finally:
        for source, backup in moved:
            backup.rename(source)
        spark.sql(f"DROP TABLE IF EXISTS {name}")
