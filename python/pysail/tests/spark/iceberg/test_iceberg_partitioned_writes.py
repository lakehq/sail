import datetime as dt
from typing import List, Tuple  # noqa: UP035

import pytest
from pyiceberg.partitioning import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    PartitionField,
    PartitionSpec,
    TruncateTransform,
    YearTransform,
)
from pyiceberg.schema import Schema
from pyiceberg.types import DateType, IntegerType, NestedField, StringType, TimestampType

from .utils import create_sql_catalog, pyiceberg_to_pandas  # noqa: TID252

# pytestmark = pytest.mark.xfail(reason="partitioned writes not supported yet", strict=False)


def _common_schema() -> Schema:
    return Schema(
        NestedField(1, "number", IntegerType(), required=True),
        NestedField(2, "letter", StringType(), required=False),
        NestedField(3, "ts", TimestampType(), required=False),
        NestedField(4, "dt", DateType(), required=False),
    )


def _build_sample_rows() -> List[Tuple[int, str, dt.datetime, dt.date]]:  # noqa: UP006
    start_date = dt.date(2023, 3, 1)
    start_ts = dt.datetime(2023, 3, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
    letters = [
        "a",
        "b",
        "c",
        "d",
        "e",
        "f",
        "g",
        "h",
        "i",
        "j",
        "k",
        "l",
    ]
    rows: List[Tuple[int, str, dt.datetime, dt.date]] = []  # noqa: UP006
    for i in range(12):
        rows.append((i + 1, letters[i], start_ts + dt.timedelta(days=i), start_date + dt.timedelta(days=i)))  # noqa: PERF401
    return rows


@pytest.mark.parametrize(
    ("table_name", "spec", "predicate_column", "predicate_value", "expected_numbers"),
    [
        (
            "default.write_partition_identity",
            PartitionSpec(PartitionField(3, 1001, IdentityTransform(), "ts")),
            "ts",
            "'2023-03-05T00:00:00+00:00'",
            {5, 6, 7, 8, 9, 10, 11, 12},
        ),
        (
            "default.write_partition_year",
            PartitionSpec(PartitionField(4, 1002, YearTransform(), "dt_year")),
            "dt",
            "'2023-03-05'",
            {5, 6, 7, 8, 9, 10, 11, 12},
        ),
        (
            "default.write_partition_month",
            PartitionSpec(PartitionField(4, 1003, MonthTransform(), "dt_month")),
            "dt",
            "'2023-03-05'",
            {5, 6, 7, 8, 9, 10, 11, 12},
        ),
        (
            "default.write_partition_day",
            PartitionSpec(PartitionField(3, 1004, DayTransform(), "ts_day")),
            "ts",
            "'2023-03-05T00:00:00+00:00'",
            {5, 6, 7, 8, 9, 10, 11, 12},
        ),
        (
            "default.write_partition_hour",
            PartitionSpec(PartitionField(3, 1005, HourTransform(), "ts_hour")),
            "ts",
            "'2023-03-05T00:00:00+00:00'",
            {5, 6, 7, 8, 9, 10, 11, 12},
        ),
        (
            "default.write_partition_trunc",
            PartitionSpec(PartitionField(2, 1006, TruncateTransform(1), "letter_trunc")),
            "letter",
            "'e'",
            {5, 6, 7, 8, 9, 10, 11, 12},
        ),
        (
            "default.write_partition_bucket",
            PartitionSpec(PartitionField(1, 1007, BucketTransform(8), "number_bucket")),
            "number",
            "5",
            {5, 6, 7, 8, 9, 10, 11, 12},
        ),
    ],
)
def test_partitioned_write_then_sail_read(
    spark, tmp_path, table_name, spec, predicate_column, predicate_value, expected_numbers
):
    catalog = create_sql_catalog(tmp_path)
    schema = _common_schema()
    table = catalog.create_table(identifier=table_name, schema=schema, partition_spec=spec)
    try:
        rows = _build_sample_rows()
        df = spark.createDataFrame(rows, schema="number INT, letter STRING, ts TIMESTAMP, dt DATE")
        df.write.format("iceberg").mode("overwrite").save(table.location())

        predicate = f"{predicate_column} >= {predicate_value}"
        out_df = spark.read.format("iceberg").load(table.location()).filter(predicate).select("number")
        result = {r[0] for r in out_df.collect()}
        assert result == expected_numbers
    finally:
        catalog.drop_table(table_name)


@pytest.mark.parametrize(
    ("table_name", "spec"),
    [
        (
            "default.write_partition_identity_all",
            PartitionSpec(PartitionField(3, 1001, IdentityTransform(), "ts")),
        ),
        (
            "default.write_partition_year_all",
            PartitionSpec(PartitionField(4, 1002, YearTransform(), "dt_year")),
        ),
        (
            "default.write_partition_month_all",
            PartitionSpec(PartitionField(4, 1003, MonthTransform(), "dt_month")),
        ),
        (
            "default.write_partition_day_all",
            PartitionSpec(PartitionField(3, 1004, DayTransform(), "ts_day")),
        ),
        (
            "default.write_partition_hour_all",
            PartitionSpec(PartitionField(3, 1005, HourTransform(), "ts_hour")),
        ),
        (
            "default.write_partition_trunc_all",
            PartitionSpec(PartitionField(2, 1006, TruncateTransform(1), "letter_trunc")),
        ),
        (
            "default.write_partition_bucket_all",
            PartitionSpec(PartitionField(1, 1007, BucketTransform(8), "number_bucket")),
        ),
    ],
)
def test_partitioned_write_then_pyiceberg_read_all(spark, tmp_path, table_name, spec):
    catalog = create_sql_catalog(tmp_path)
    schema = _common_schema()
    table = catalog.create_table(identifier=table_name, schema=schema, partition_spec=spec)
    try:
        rows = _build_sample_rows()
        df = spark.createDataFrame(rows, schema="number INT, letter STRING, ts TIMESTAMP, dt DATE")
        df.write.format("iceberg").mode("overwrite").save(table.location())

        py_tbl = catalog.load_table(table_name)
        actual = pyiceberg_to_pandas(py_tbl, sort_by="number")

        expected_letters = [
            "a",
            "b",
            "c",
            "d",
            "e",
            "f",
            "g",
            "h",
            "i",
            "j",
            "k",
            "l",
        ]
        assert len(actual) == 12  # noqa: PLR2004
        assert list(actual["number"]) == list(range(1, 13))
        assert list(actual["letter"]) == expected_letters
    finally:
        catalog.drop_table(table_name)
