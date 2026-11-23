# SPDX-License-Identifier: Apache-2.0

import json
from datetime import date
from decimal import Decimal
from urllib.parse import unquote

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


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
        empty = spark.table(table)
        assert empty.collect() == []
        assert empty.where("p = 7").inputFiles() == []
        with pytest.raises(Exception, match="MULTI_SOURCES_UNSUPPORTED_FOR_EXPRESSION"):
            empty.alias("a").join(empty.alias("b"), "id", "left").selectExpr("input_file_name()").collect()
        parquet_dir = tmp_path / "outside[1]"
        parquet_dir.mkdir()
        pq.write_table(pa.table({"id": [1], "p": [999]}), parquet_dir / "data.parquet")
        (parquet_dir / "empty.parquet").touch()
        nested_stale = parquet_dir / "p=stale"
        nested_stale.mkdir()
        pq.write_table(pa.table({"id": [100]}), nested_stale / "data.parquet")
        second_dir = parquet_dir / "another=partition"
        second_dir.mkdir()
        pq.write_table(pa.table({"id": [3]}), second_dir / "data.parquet")
        json_dir = root / "p=wrong"
        json_dir.mkdir()
        (json_dir / "data.json").write_text(json.dumps({"ID": 2}))
        for values, descriptor in [
            (["007", "2025-02", "x%2Fy"], {"Location": str(parquet_dir)}),
            (
                ["8", "2025-01-02", "local"],
                {
                    "Location": str(second_dir),
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
        assert frame.columns == ["id", "p", "day", "label"]
        assert frame.orderBy("id").collect() == [
            (1, 7, date(2025, 2, 1), "x%2Fy"),
            (2, None, date(2025, 1, 3), "other"),
            (3, 8, date(2025, 1, 2), "local"),
        ]
        assert frame.where("p = 7").select("id").collect() == [(1,)]
        assert frame.where("p IS NULL").select("id").collect() == [(2,)]
        assert {unquote(uri) for uri in frame.inputFiles()} == {
            unquote((parquet_dir / "data.parquet").as_uri()),
            unquote((parquet_dir / "empty.parquet").as_uri()),
            unquote((json_dir / "data.json").as_uri()),
            unquote((second_dir / "data.parquet").as_uri()),
        }
        # The filtered JSON scan must not sample a different partition in the same store.
        bad_json = root / "unreadable-json"
        bad_json.mkdir()
        (bad_json / "invalid.json.gz").write_bytes(b"invalid gzip")
        json_format = {"SerdeInfo": {"SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"}}
        for value, location in [("9", str(bad_json)), ("10", "unavailable://other-store/data")]:
            glue.create_partition(
                DatabaseName="test_db",
                TableName="registered_partition_reads",
                PartitionInput={
                    "Values": [value, "2025-03", "unreadable"],
                    "StorageDescriptor": {"Location": location, **json_format},
                },
            )
        glue.create_partition(
            DatabaseName="test_db",
            TableName="registered_partition_reads",
            PartitionInput={
                "Values": ["11", "2025-03", "unsupported"],
                "StorageDescriptor": {
                    "Location": str(root / "orc"),
                    "InputFormat": "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
                },
            },
        )
        selected = spark.table(table).where("p IS NULL")
        assert selected.select("id").collect() == [(2,)]
        assert [unquote(uri) for uri in selected.inputFiles()] == [unquote((json_dir / "data.json").as_uri())]
        with pytest.raises(Exception, match="unavailable"):
            spark.table(table).where("p = 10").collect()

        with pytest.raises(Exception, match="orc"):
            spark.table(table).where("p = 11").collect()

        glue.create_partition(
            DatabaseName="test_db",
            TableName="registered_partition_reads",
            PartitionInput={
                "Values": ["invalid", "bad-date", "legacy"],
                "StorageDescriptor": {"Location": str(parquet_dir)},
            },
        )
        ansi = "spark.sql.ansi.enabled"
        previous = spark.conf.get(ansi)
        try:
            spark.conf.set(ansi, "false")
            assert spark.table(table).where("label = 'legacy'").select("p", "day").collect() == [(None, None)]
        finally:
            spark.conf.set(ansi, previous)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_registered_decimal_with_nested_directory(spark, moto_endpoint, tmp_path):
    glue = boto3.client("glue", region_name="us-east-1", endpoint_url=moto_endpoint)
    root = tmp_path / "partition"
    nested = root / "child"
    nested.mkdir(parents=True)
    (root / "data.json").write_text('{"id":1}')
    (nested / "data.json").write_text('{"id":2}')
    table = "test_db.registered_decimal_partition"
    spark.sql(
        f"CREATE TABLE {table} (id INT, p DECIMAL(6,2)) USING json PARTITIONED BY (p) LOCATION '{tmp_path.as_uri()}'"
    )
    try:
        for value, path in [("1E+2", root), ("9.995E-1", nested)]:
            glue.create_partition(
                DatabaseName="test_db",
                TableName="registered_decimal_partition",
                PartitionInput={"Values": [value], "StorageDescriptor": {"Location": str(path)}},
            )
        assert spark.table(table).orderBy("id").collect() == [(1, Decimal("100.00")), (2, Decimal("1.00"))]
        mode = nested.stat().st_mode
        nested.chmod(0)
        try:
            parent = spark.table(table).where("p = 100")
            assert parent.collect() == [(1, Decimal("100.00"))]
            assert parent.inputFiles() == [(root / "data.json").as_uri()]
        finally:
            nested.chmod(mode)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_registered_csv_partition_options(spark, moto_endpoint, tmp_path):
    glue = boto3.client("glue", region_name="us-east-1", endpoint_url=moto_endpoint)
    table = "test_db.registered_csv_partitions"
    spark.sql(
        f"CREATE TABLE {table} (id INT, value STRING, p INT) USING parquet PARTITIONED BY (p) LOCATION '{tmp_path.as_uri()}'"
    )
    try:
        for p, delimiter in [(1, "|"), (2, ";")]:
            path = tmp_path / str(p)
            path.mkdir()
            (path / "part.csv").write_text(f"{p}{delimiter}'a{delimiter}b'\n")
            glue.create_partition(
                DatabaseName="test_db",
                TableName="registered_csv_partitions",
                PartitionInput={
                    "Values": [str(p)],
                    "StorageDescriptor": {
                        "Location": str(path),
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
                            "Parameters": {"separatorChar": delimiter, "quoteChar": "'"},
                        },
                    },
                },
            )
        assert spark.table(table).orderBy("p").collect() == [(1, "a|b", 1), (2, "a;b", 2)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_registered_csv_inherits_table_serde(spark, moto_endpoint, tmp_path):
    glue = boto3.client("glue", region_name="us-east-1", endpoint_url=moto_endpoint)
    name = "registered_csv_inherits_table_serde"
    table = f"test_db.{name}"
    glue.create_table(
        DatabaseName="test_db",
        TableInput={
            "Name": name,
            "TableType": "EXTERNAL_TABLE",
            "PartitionKeys": [{"Name": "p", "Type": "int"}],
            "StorageDescriptor": {
                "Columns": [{"Name": "id", "Type": "int"}, {"Name": "value", "Type": "string"}],
                "Location": str(tmp_path),
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
                    "Parameters": {"separatorChar": "|", "quoteChar": "'"},
                },
            },
        },
    )
    try:
        for p, separator in [(1, "|"), (2, ";")]:
            path = tmp_path / str(p)
            path.mkdir()
            (path / "data.csv").write_text(f"{p}{separator}'a{separator}b'\n")
            descriptor = {"Location": str(path)}
            if p == 2:
                descriptor["InputFormat"] = "org.apache.hadoop.mapred.TextInputFormat"
                descriptor["SerdeInfo"] = {"Parameters": {"separatorChar": separator}}
            glue.create_partition(
                DatabaseName="test_db",
                TableName=name,
                PartitionInput={"Values": [str(p)], "StorageDescriptor": descriptor},
            )
        assert spark.table(table).orderBy("p").collect() == [(1, "a|b", 1), (2, "a;b", 2)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.spark(version=">=4.1", reason="Uses the Python range data source with filter pushdown")
def test_partitioned_python_table_keeps_its_reader(spark, moto_endpoint):
    from pysail.testing._dev.spark.datasource import RangeDataSource

    spark.dataSource.register(RangeDataSource)
    glue = boto3.client("glue", region_name="us-east-1", endpoint_url=moto_endpoint)
    name = "registered_python_table"
    glue.create_table(
        DatabaseName="test_db",
        TableInput={
            "Name": name,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "spark.sql.sources.provider": RangeDataSource.name(),
                "option.start": "42",
                "option.end": "43",
            },
            "StorageDescriptor": {"Columns": []},
            "PartitionKeys": [{"Name": "id", "Type": "bigint"}],
        },
    )
    try:
        assert spark.table(f"test_db.{name}").collect() == [(42,)]
    finally:
        glue.delete_table(DatabaseName="test_db", Name=name)
