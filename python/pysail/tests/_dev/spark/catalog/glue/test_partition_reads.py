# SPDX-License-Identifier: Apache-2.0

import json
from datetime import date
from urllib.parse import unquote

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


def test_registered_partition_reads(spark, moto_endpoint, tmp_path):
    glue = boto3.client("glue", region_name="us-east-1", endpoint_url=moto_endpoint)
    table = "test_db.registered_partition_reads"
    root = tmp_path / "root"
    stale = root / "p=99" / "day=2000-01-01" / "label=old"
    stale.mkdir(parents=True)
    pq.write_table(pa.table({"id": [99]}), stale / "data.parquet")
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, p INT, day DATE, label STRING) USING parquet "
        f"PARTITIONED BY (p, day, label) LOCATION '{root.as_uri()}'"
    )
    try:
        assert spark.table(table).collect() == []
        parquet_dir = tmp_path / "outside[1]"
        parquet_dir.mkdir()
        pq.write_table(pa.table({"id": [1], "p": [999]}), parquet_dir / "data.parquet")
        second_dir = root / "plain"
        second_dir.mkdir()
        pq.write_table(pa.table({"id": [3]}), second_dir / "data.parquet")
        json_dir = root / "p=wrong"
        json_dir.mkdir()
        (json_dir / "data.json").write_text(json.dumps({"id": 2}))
        for values, descriptor in [
            (["007", "2025-01-02", "x%2Fy"], {"Location": str(parquet_dir)}),
            (
                ["8", "2025-01-02", "local"],
                {
                    "Location": second_dir.as_uri(),
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                },
            ),
            (
                ["__HIVE_DEFAULT_PARTITION__", "2025-01-03", "other"],
                {
                    "Location": f"file:{json_dir}",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "SerdeInfo": {"SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"},
                },
            ),
        ]:
            glue.create_partition(
                DatabaseName="test_db",
                TableName="registered_partition_reads",
                PartitionInput={"Values": values, "StorageDescriptor": descriptor},
            )
        frame = spark.table(table)
        assert frame.orderBy("id").collect() == [
            (1, 7, date(2025, 1, 2), "x%2Fy"),
            (2, None, date(2025, 1, 3), "other"),
            (3, 8, date(2025, 1, 2), "local"),
        ]
        assert frame.where("p = 7").select("id").collect() == [(1,)]
        assert frame.where("p IS NULL").select("id").collect() == [(2,)]
        assert {unquote(uri) for uri in frame.inputFiles()} == {
            unquote((parquet_dir / "data.parquet").as_uri()),
            unquote((json_dir / "data.json").as_uri()),
            unquote((second_dir / "data.parquet").as_uri()),
        }
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
