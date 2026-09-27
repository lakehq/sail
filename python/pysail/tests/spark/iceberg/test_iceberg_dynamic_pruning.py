from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, LongType, NestedField

from pysail.tests.spark.iceberg.utils import create_sql_catalog


@pytest.mark.parametrize("partitioned", [False, True], ids=["manifest-statistics", "identity-partitions"])
def test_dynamic_join_skips_unreadable_files(spark, tmp_path, partitioned):
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
        frame = spark.read.format("iceberg").option("metadataAsDataRead", "true").load(table.location())
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
