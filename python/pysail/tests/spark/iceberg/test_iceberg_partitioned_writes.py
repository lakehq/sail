import datetime as dt
import platform
from pathlib import Path
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

from pysail.tests.spark.utils import escape_sql_string_literal

from .utils import create_sql_catalog, pyiceberg_to_pandas  # noqa: TID252


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
@pytest.mark.skipif(platform.system() == "Windows", reason="may not work on Windows")
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
@pytest.mark.skipif(platform.system() == "Windows", reason="may not work on Windows")
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


@pytest.mark.skipif(platform.system() == "Windows", reason="may not work on Windows")
def test_iceberg_partition_writes_sql(spark, tmp_path):
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    path_single = f"file://{warehouse / 'ice_single'}"
    spark.sql(
        f"""
        CREATE TABLE tmp_ice_single (
            id INT,
            event STRING,
            score INT
        ) USING iceberg
        PARTITIONED BY (id)
        LOCATION '{escape_sql_string_literal(path_single)}'
        """
    )
    try:
        df_single = spark.createDataFrame(
            [(10, "A", 1), (11, "B", 2), (12, "A", 3)], schema="id INT, event STRING, score INT"
        )
        df_single.write.format("iceberg").mode("append").save(path_single)
        out_single = spark.read.format("iceberg").load(path_single).sort("id")
        assert [r[0] for r in out_single.select("id").collect()] == [10, 11, 12]
    finally:
        spark.sql("DROP TABLE IF EXISTS tmp_ice_single")

    path_multi = f"file://{warehouse / 'ice_multi'}"
    spark.sql(
        f"""
        CREATE TABLE tmp_ice_multi (
            id INT,
            region INT,
            category INT,
            value INT
        ) USING iceberg
        PARTITIONED BY (region, category)
        LOCATION '{escape_sql_string_literal(path_multi)}'
        """
    )
    try:
        df_multi = spark.createDataFrame(
            [(1, 1, 1, 100), (2, 1, 2, 200), (3, 2, 1, 300), (4, 2, 2, 400)],
            schema="id INT, region INT, category INT, value INT",
        )
        df_multi.write.format("iceberg").mode("append").save(path_multi)
        parts = spark.read.format("iceberg").load(path_multi).select("region", "category").distinct()
        actual = {f"region={r[0]}/category={r[1]}" for r in parts.collect()}
        assert actual == {"region=1/category=1", "region=1/category=2", "region=2/category=1", "region=2/category=2"}
    finally:
        spark.sql("DROP TABLE IF EXISTS tmp_ice_multi")


@pytest.mark.skipif(platform.system() == "Windows", reason="may not work on Windows")
def test_partitioned_append_infers_spec_from_metadata(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "category", StringType(), required=False),
        NestedField(3, "value", IntegerType(), required=False),
    )
    spec = PartitionSpec(PartitionField(2, 2008, IdentityTransform(), "category"))
    table_name = "default.partitioned_append_metadata"
    table = catalog.create_table(identifier=table_name, schema=schema, partition_spec=spec)
    try:
        schema_str = "id INT, category STRING, value INT"
        initial_rows = [
            (1, "A", 10),
            (2, "B", 20),
        ]
        append_rows = [
            (3, "A", 30),
            (4, "C", 40),
        ]

        spark.createDataFrame(initial_rows, schema=schema_str).write.format("iceberg").mode("append").save(
            table.location()
        )
        spark.createDataFrame(append_rows, schema=schema_str).write.format("iceberg").mode("append").save(
            table.location()
        )

        result = {
            (row.id, row.category, row.value) for row in spark.read.format("iceberg").load(table.location()).collect()
        }
        assert result == {(1, "A", 10), (2, "B", 20), (3, "A", 30), (4, "C", 40)}

        table_path = Path(table.location().removeprefix("file://"))
        data_dir = table_path / "data"
        partition_dirs = {p.name for p in data_dir.iterdir() if p.is_dir()}
        assert partition_dirs == {"category=A", "category=B", "category=C"}

        def count_parquet_files(partition: str) -> int:
            return len(list((data_dir / partition).glob("*.parquet")))

        expected_file_counts = {"category=A": 2, "category=B": 1, "category=C": 1}
        for partition, expected_count in expected_file_counts.items():
            assert count_parquet_files(partition) == expected_count

        root_files = list(data_dir.glob("*.parquet"))
        assert not root_files, "Partitioned Iceberg tables should not place files directly under data/"
    finally:
        catalog.drop_table(table_name)
