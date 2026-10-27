import json
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, TimestampType

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.tests.spark.iceberg.utils import create_sql_catalog

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail worker Parquet configuration")


@pytest.fixture(scope="module", params=["local", "local-cluster"])
def remote(request):
    with spark_connect_server(
        envs={
            "SAIL_MODE": request.param,
            "SAIL_EXECUTION__DEFAULT_PARALLELISM": "2",
            "SAIL_CLUSTER__WORKER_MAX_COUNT": "2",
        }
    ) as server:
        yield server.remote


@pytest.mark.parametrize("table_format", ["delta", "iceberg"])
@pytest.mark.parametrize("metadata_as_data", [False, True])
def test_lake_int96_timestamps_use_microseconds(spark, tmp_path, table_format, metadata_as_data):
    path = tmp_path / "timestamps.parquet"
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), metadata={"PARQUET:field_id": "1"}),
            pa.field("ts", pa.timestamp("us"), metadata={"PARQUET:field_id": "2"}),
        ]
    )
    # The year 3000 fits in microseconds but overflows a nanosecond timestamp.
    timestamps = [
        datetime(2024, 1, 2, 3, 4, 5, 678901, tzinfo=UTC),
        datetime(3000, 1, 2, 3, 4, 5, 678901, tzinfo=UTC),
        None,
    ]
    data = pa.table({"id": [0, 1, 2], "ts": timestamps}, schema=schema)
    pq.write_table(data, path, use_deprecated_int96_timestamps=True, store_schema=False)
    catalog = None
    if table_format == "delta":
        log = tmp_path / "_delta_log"
        log.mkdir()
        fields = [
            {"name": name, "type": kind, "nullable": True, "metadata": {}}
            for name, kind in [("id", "long"), ("ts", "timestamp")]
        ]
        actions = [
            {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
            {
                "metaData": {
                    "id": "int96",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": json.dumps({"type": "struct", "fields": fields}),
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 0,
                }
            },
            {
                "add": {
                    "path": path.name,
                    "partitionValues": {},
                    "size": path.stat().st_size,
                    "modificationTime": 0,
                    "dataChange": True,
                    "stats": json.dumps({"numRecords": 3}),
                }
            },
        ]
        (log / "00000000000000000000.json").write_text("".join(json.dumps(action) + "\n" for action in actions))
        location = str(tmp_path)
    else:
        catalog = create_sql_catalog(tmp_path)
        table = catalog.create_table(
            "default.int96",
            schema=Schema(NestedField(1, "id", LongType()), NestedField(2, "ts", TimestampType())),
        )
        with table.transaction() as transaction, transaction.update_snapshot().fast_append() as append:
            append.append_data_file(
                DataFile.from_args(
                    content=DataFileContent.DATA,
                    file_path=path.as_uri(),
                    file_format=FileFormat.PARQUET,
                    partition={},
                    record_count=3,
                    file_size_in_bytes=path.stat().st_size,
                    spec_id=0,
                )
            )
        location = table.location()
    try:
        query = (
            spark.read.format(table_format).option("metadataAsDataRead", str(metadata_as_data).lower()).load(location)
        )
        if metadata_as_data:
            node = "DeltaScanByAddsExec" if table_format == "delta" else "IcebergScanByDataFilesExec"
            assert node in query._explain_string()  # noqa: SLF001
        rows = query.orderBy("id").selectExpr("CAST(ts AS STRING) AS ts").collect()
        assert [row.ts for row in rows] == ["2024-01-02 03:04:05.678901", "3000-01-02 03:04:05.678901", None]
    finally:
        if catalog is not None:
            catalog.drop_table("default.int96")
