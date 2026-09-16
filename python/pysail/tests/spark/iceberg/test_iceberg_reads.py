import math
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import BooleanType, DoubleType, LongType, NestedField, StringType, TimestampType

from pysail.tests.spark.iceberg.utils import create_sql_catalog


@pytest.mark.parametrize("path_kind", ["absolute", "relative", "file_location"])
@pytest.mark.parametrize("metadata_as_data", [False, True])
def test_manifest_paths_preserve_percent_literals(spark, tmp_path, path_kind, metadata_as_data):
    catalog = create_sql_catalog(tmp_path)
    table_path = tmp_path / "manifest_paths"
    identifier = "default.manifest_paths"
    table = catalog.create_table(
        identifier=identifier,
        location=table_path.as_uri(),
        schema=Schema(NestedField(1, "id", LongType(), required=False)),
    )
    try:
        data_path = table_path / "data" / "part=a+%2B%252F+%E4%B8%AD" / "12%3A34%3A56.parquet"
        data_path.parent.mkdir(parents=True, exist_ok=True)
        schema = pa.schema([pa.field("id", pa.int64(), metadata={"PARQUET:field_id": "1"})])
        pq.write_table(pa.Table.from_pydict({"id": [1, 2]}, schema=schema), data_path)
        paths = {
            "absolute": data_path.as_posix(),
            "relative": data_path.relative_to(table_path).as_posix(),
            "file_location": "file:///" + data_path.as_posix().lstrip("/"),
        }
        data_file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=paths[path_kind],
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=2,
            file_size_in_bytes=data_path.stat().st_size,
        )
        data_file.spec_id = 0
        with table.transaction() as transaction, transaction.update_snapshot().fast_append() as append:
            append.append_data_file(data_file)
        result = (
            spark.read.format("iceberg")
            .option("metadataAsDataRead", str(metadata_as_data).lower())
            .load(table.location())
            .orderBy("id")
            .collect()
        )
        assert [row.id for row in result] == [1, 2]
    finally:
        catalog.drop_table(identifier)


def test_nan_reads(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.test_nan_reads"
    table = catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(1, "idx", DoubleType(), required=False),
            NestedField(2, "col_numeric", DoubleType(), required=False),
        ),
    )
    try:
        tbl = pa.table({"idx": [1.0, 2.0, 3.0], "col_numeric": [float("nan"), 2.0, 3.0]})
        table.append(tbl)
        path = table.location()
        df = spark.read.format("iceberg").load(path).select("idx", "col_numeric").filter("isnan(col_numeric)")
        rows = df.collect()
        assert len(rows) == 1
        assert int(rows[0][0]) == 1
        assert math.isnan(rows[0][1])
    finally:
        catalog.drop_table(identifier)


def test_datetime_filter_reads(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.test_datetime_filter_reads"
    table = catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(1, "str", StringType(), required=False),
            NestedField(2, "datetime", TimestampType(), required=False),
        ),
    )
    try:
        yesterday = datetime.now() - timedelta(days=1)  # noqa: DTZ005
        tbl = pa.table({"str": ["foo"], "datetime": [yesterday]})
        table.append(tbl)
        path = table.location()
        iso_ts = yesterday.isoformat()
        df_ge = spark.read.format("iceberg").load(path).filter(f"datetime >= '{iso_ts}'")
        assert df_ge.count() == 1
        df_lt = spark.read.format("iceberg").load(path).filter(f"datetime < '{iso_ts}'")
        assert df_lt.count() == 0
    finally:
        catalog.drop_table(identifier)


def test_struct_null_filters(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.test_struct_null_filters"
    struct_field = pa.struct([("test", pa.int32())])
    arrow_schema = pa.schema([pa.field("col_struct", struct_field)])
    table = catalog.create_table(identifier=identifier, schema=arrow_schema)
    try:
        t1 = pa.Table.from_arrays([pa.array([None], type=struct_field)], schema=arrow_schema)
        t2 = pa.Table.from_arrays([pa.array([{"test": 1}], type=struct_field)], schema=arrow_schema)
        table.append(t1)
        table.append(t2)
        path = table.location()
        df_all = spark.read.format("iceberg").load(path)
        assert df_all.count() == 2  # noqa: PLR2004
        df_not_null = df_all.filter("col_struct.test is not null")
        assert df_not_null.count() == 1
        df_null = df_all.filter("col_struct.test is null")
        assert df_null.count() == 1
    finally:
        catalog.drop_table(identifier)


def test_limit_with_multiple_files(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.test_limit_with_multiple_files"
    table = catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(1, "id", StringType(), required=False),
        ),
    )
    try:
        tbl1 = pa.table({"id": ["a", "b", "c", "d", "e"]})
        tbl2 = pa.table({"id": ["f", "g", "h", "i", "j"]})
        table.append(tbl1)
        table.append(tbl2)
        path = table.location()
        df = spark.read.format("iceberg").load(path).limit(3)
        assert df.count() == 3  # noqa: PLR2004
    finally:
        catalog.drop_table(identifier)


def test_limit_with_filter(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.test_limit_with_filter"
    table = catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(1, "id", StringType(), required=False),
            NestedField(2, "flag", BooleanType(), required=False),
        ),
    )
    try:
        tbl1 = pa.table({"id": ["a", "b", "c", "d", "e"], "flag": [True, False, True, True, False]})
        tbl2 = pa.table({"id": ["f", "g", "h", "i", "j"], "flag": [False, True, False, True, True]})
        table.append(tbl1)
        table.append(tbl2)
        path = table.location()
        df = spark.read.format("iceberg").load(path).filter("flag = true").limit(3)
        assert df.count() == 3  # noqa: PLR2004
    finally:
        catalog.drop_table(identifier)


def test_limit_with_offset(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.test_limit_with_offset"
    table = catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "flag", BooleanType(), required=False),
        ),
    )
    try:
        # Multiple data files to exercise distributed limit/offset correctness.
        tbl1 = pa.table({"id": [0, 1, 2, 3, 4], "flag": [True, False, True, True, False]})
        tbl2 = pa.table({"id": [5, 6, 7, 8, 9], "flag": [False, True, False, True, True]})
        table.append(tbl1)
        table.append(tbl2)

        path = table.location()
        df = spark.read.format("iceberg").load(path).where("flag = true").orderBy("id")
        df.createOrReplaceTempView("tmp_limit_offset")
        ids = [row.id for row in spark.sql("SELECT id FROM tmp_limit_offset LIMIT 3 OFFSET 2").collect()]
        assert ids == [3, 6, 8]
        # Nested subquery to ensure we handle `GlobalLimitExec` not at the root.
        ids2 = [
            row.id for row in spark.sql("SELECT id FROM (SELECT id FROM tmp_limit_offset) t LIMIT 3 OFFSET 2").collect()
        ]
        assert ids2 == [3, 6, 8]
    finally:
        catalog.drop_table(identifier)
