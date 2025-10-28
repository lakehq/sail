import datetime

import pyarrow as pa
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

from .utils import create_sql_catalog  # noqa: TID252


def _make_common_schema():
    return Schema(
        NestedField(1, "number", IntegerType(), required=True),
        NestedField(2, "letter", StringType(), required=False),
        NestedField(3, "ts", TimestampType(), required=False),
        NestedField(4, "dt", DateType(), required=False),
    )


def _append_sample_data(table):
    start_date = datetime.date(2023, 3, 1)
    start_dt = datetime.datetime(2023, 3, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
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
    numbers = list(range(1, 13))
    dts = [start_date + datetime.timedelta(days=i) for i in range(12)]
    tss = [start_dt + datetime.timedelta(days=i) for i in range(12)]

    arrow_schema = pa.schema(
        [
            pa.field("number", pa.int32(), nullable=False),
            pa.field("letter", pa.string(), nullable=True),
            pa.field("ts", pa.timestamp("us"), nullable=True),
            pa.field("dt", pa.date32(), nullable=True),
        ]
    )

    for i in range(0, 12, 4):
        arrays = [
            pa.array(numbers[i : i + 4], type=pa.int32()),
            pa.array(letters[i : i + 4], type=pa.string()),
            pa.array(tss[i : i + 4], type=pa.timestamp("us")),
            pa.array(dts[i : i + 4], type=pa.date32()),
        ]
        batch = pa.Table.from_arrays(arrays, schema=arrow_schema)
        table.append(batch)


@pytest.mark.parametrize(
    ("table_name", "spec", "predicate_column", "predicate_value", "expected_numbers"),
    [
        pytest.param(
            # FIXME: this test is failing in `local-cluster` mode because of
            # the proto issue related to `PartitionedFile`.
            "default.test_partitioned_by_identity",
            PartitionSpec(PartitionField(3, 1001, IdentityTransform(), "ts")),
            "ts",
            "'2023-03-05T00:00:00+00:00'",
            {5, 6, 7, 8, 9, 10, 11, 12},
            id="partition-by-identity",
        ),
        pytest.param(
            "default.test_partitioned_by_years",
            PartitionSpec(PartitionField(4, 1002, YearTransform(), "dt_year")),
            "dt",
            "'2023-03-05'",
            {5, 6, 7, 8, 9, 10, 11, 12},
            id="partition-by-years",
        ),
        pytest.param(
            "default.test_partitioned_by_months",
            PartitionSpec(PartitionField(4, 1003, MonthTransform(), "dt_month")),
            "dt",
            "'2023-03-05'",
            {5, 6, 7, 8, 9, 10, 11, 12},
            id="partition-by-months",
        ),
        pytest.param(
            "default.test_partitioned_by_days",
            PartitionSpec(PartitionField(3, 1004, DayTransform(), "ts_day")),
            "ts",
            "'2023-03-05T00:00:00+00:00'",
            {5, 6, 7, 8, 9, 10, 11, 12},
            id="partition-by-days",
        ),
        pytest.param(
            "default.test_partitioned_by_hours",
            PartitionSpec(PartitionField(3, 1005, HourTransform(), "ts_hour")),
            "ts",
            "'2023-03-05T00:00:00+00:00'",
            {5, 6, 7, 8, 9, 10, 11, 12},
            id="partition-by-hours",
        ),
        pytest.param(
            "default.test_partitioned_by_truncate",
            PartitionSpec(PartitionField(2, 1006, TruncateTransform(1), "letter_trunc")),
            "letter",
            "'e'",
            {5, 6, 7, 8, 9, 10, 11, 12},
            id="partition-by-truncate",
        ),
        pytest.param(
            "default.test_partitioned_by_bucket",
            PartitionSpec(PartitionField(1, 1007, BucketTransform(8), "number_bucket")),
            "number",
            "5",
            {5, 6, 7, 8, 9, 10, 11, 12},
            id="partition-by-bucket",
        ),
    ],
)
def test_partition_transform_pruning(
    spark, tmp_path, table_name, spec, predicate_column, predicate_value, expected_numbers
):
    catalog = create_sql_catalog(tmp_path)
    schema = _make_common_schema()
    table = catalog.create_table(identifier=table_name, schema=schema, partition_spec=spec)
    try:
        _append_sample_data(table)
        path = table.location()

        predicate = f"{predicate_column} >= {predicate_value}"
        df = spark.read.format("iceberg").load(path).filter(predicate).select("number")
        result = {r[0] for r in df.collect()}
        assert result == expected_numbers
    finally:
        catalog.drop_table(table_name)
