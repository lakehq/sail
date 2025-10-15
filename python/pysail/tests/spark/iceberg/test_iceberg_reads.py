import math

import pyarrow as pa
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, DoubleType, NestedField, StringType, TimestampType

from .utils import create_sql_catalog  # noqa: TID252


@pytest.mark.parametrize("use_rewritten", [False, True])
def test_nan_reads(spark, tmp_path, use_rewritten):
    catalog = create_sql_catalog(tmp_path)
    identifier = f"default.test_nan_reads_{'rewritten' if use_rewritten else 'orig'}"
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
    from datetime import datetime, timedelta

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
