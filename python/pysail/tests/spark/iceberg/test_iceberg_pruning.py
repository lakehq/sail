# ruff: noqa
import pandas as pd
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, DoubleType, IntegerType, LongType, NestedField, StringType
from .utils import create_sql_catalog


def _make_eq_in_table(catalog, ident):
    table = catalog.create_table(
        identifier=ident,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="year", field_type=IntegerType(), required=False),
            NestedField(field_id=3, name="month", field_type=IntegerType(), required=False),
            NestedField(field_id=4, name="value", field_type=StringType(), required=False),
        ),
    )
    batches = [
        pd.DataFrame({"id": [1, 2], "year": [2023, 2023], "month": [1, 1], "value": ["a", "b"]}),
        pd.DataFrame({"id": [3, 4], "year": [2023, 2023], "month": [2, 2], "value": ["c", "d"]}),
        pd.DataFrame({"id": [5, 6], "year": [2024, 2024], "month": [1, 1], "value": ["e", "f"]}),
        pd.DataFrame({"id": [7, 8], "year": [2024, 2024], "month": [2, 2], "value": ["g", "h"]}),
    ]
    for df in batches:
        df = df.astype({"id": "int64", "year": "int32", "month": "int32"})
        table.append(pa.Table.from_pandas(df))
    return table


def test_pruning_equality_filters(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    ident = "default.prune_eq_only"
    table = _make_eq_in_table(catalog, ident)
    try:
        tp = table.location()
        df = spark.read.format("iceberg").load(tp).filter("year = 2023")
        assert df.count() == 4
        df = spark.read.format("iceberg").load(tp).filter("year = 2023 AND month = 1")
        assert df.count() == 2
    finally:
        catalog.drop_table(ident)


def test_pruning_in_clause(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    ident = "default.prune_in_only"
    table = _make_eq_in_table(catalog, ident)
    try:
        tp = table.location()
        df = spark.read.format("iceberg").load(tp).filter("month IN (2)")
        assert df.count() == 4
    finally:
        catalog.drop_table(ident)


def test_comparison_and_between(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    table = catalog.create_table(
        identifier="default.prune_cmp",
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="year", field_type=IntegerType(), required=False),
            NestedField(field_id=3, name="month", field_type=IntegerType(), required=False),
        ),
    )
    try:
        data = []
        for year in [2021, 2022, 2023, 2024]:
            for month in [1, 6, 12]:
                data.append({"id": len(data) + 1, "year": year, "month": month})
        for i in range(0, len(data), 6):
            batch = pd.DataFrame(data[i : i + 6]).astype({"id": "int64", "year": "int32", "month": "int32"})
            table.append(pa.Table.from_pandas(batch))

        tp = table.location()

        df = spark.read.format("iceberg").load(tp).filter("year > 2022")
        assert df.count() == 6

        df = spark.read.format("iceberg").load(tp).filter("year BETWEEN 2022 AND 2023")
        assert df.count() == 6

        df = spark.read.format("iceberg").load(tp).filter("year >= 2023 AND month >= 6")
        assert df.count() == 4
    finally:
        catalog.drop_table("default.prune_cmp")


def test_null_and_boolean(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    table = catalog.create_table(
        identifier="default.prune_null_bool",
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="region", field_type=StringType(), required=False),
            NestedField(field_id=3, name="active", field_type=BooleanType(), required=False),
        ),
    )
    try:
        table.append(
            pa.Table.from_pandas(
                pd.DataFrame([{"id": 1, "region": None, "active": True}, {"id": 2, "region": None, "active": True}])
            )
        )
        table.append(
            pa.Table.from_pandas(
                pd.DataFrame([{"id": 3, "region": "US", "active": False}, {"id": 4, "region": "EU", "active": False}])
            )
        )

        tp = table.location()

        df = spark.read.format("iceberg").load(tp).filter("region IS NULL")
        assert df.count() == 2

        df = spark.read.format("iceberg").load(tp).filter("active = true")
        assert df.count() == 2
    finally:
        catalog.drop_table("default.prune_null_bool")


def test_correctness_small(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    table = catalog.create_table(
        identifier="default.prune_correct",
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="year", field_type=IntegerType(), required=False),
            NestedField(field_id=3, name="month", field_type=IntegerType(), required=False),
            NestedField(field_id=4, name="val", field_type=DoubleType(), required=False),
        ),
    )
    try:
        records = []
        for year in [2022, 2023]:
            for month in [1, 2, 3]:
                for i in range(5):
                    records.append({"id": len(records) + 1, "year": year, "month": month, "val": float(i)})
        for i in range(0, len(records), 10):
            batch = pd.DataFrame(records[i : i + 10]).astype(
                {"id": "int64", "year": "int32", "month": "int32", "val": "float64"}
            )
            table.append(pa.Table.from_pandas(batch))

        tp = table.location()

        df = spark.read.format("iceberg").load(tp).filter("year = 2023")
        assert df.count() == 15

        df = spark.read.format("iceberg").load(tp).filter("year = 2022 AND month = 2")
        assert df.count() == 5

        df = spark.read.format("iceberg").load(tp).filter("(year = 2022 AND month = 1) OR (year = 2023 AND month = 3)")
        assert df.count() == 10
    finally:
        catalog.drop_table("default.prune_correct")


def test_or_and_not_pruning(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    table = catalog.create_table(
        identifier="default.prune_or_and_not",
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="year", field_type=IntegerType(), required=False),
            NestedField(field_id=3, name="region", field_type=StringType(), required=False),
        ),
    )
    try:
        records = []
        for year in [2022, 2023, 2024]:
            for region in ["US", "EU", "ASIA"]:
                for i in range(5):
                    records.append({"id": len(records) + 1, "year": year, "region": region})
        for i in range(0, len(records), 15):
            batch = pd.DataFrame(records[i : i + 15]).astype({"id": "int64", "year": "int32"})
            table.append(pa.Table.from_pandas(batch))

        tp = table.location()

        df = spark.read.format("iceberg").load(tp).filter("year = 2022 OR year = 2024")
        assert df.count() == 30

        df = spark.read.format("iceberg").load(tp).filter("year = 2023 AND region != 'ASIA'")
        assert df.count() == 10

        df = spark.read.format("iceberg").load(tp).filter("NOT (year = 2023 AND region = 'US')")
        assert df.count() == 40
    finally:
        catalog.drop_table("default.prune_or_and_not")


def test_string_in_and_range_pruning(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    table = catalog.create_table(
        identifier="default.prune_string_in_range",
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="dept", field_type=StringType(), required=False),
            NestedField(field_id=3, name="team", field_type=StringType(), required=False),
        ),
    )
    try:
        rows = [
            {"id": 1, "dept": "engineering", "team": "backend"},
            {"id": 2, "dept": "engineering", "team": "frontend"},
            {"id": 3, "dept": "marketing", "team": "growth"},
            {"id": 4, "dept": "sales", "team": "enterprise"},
        ]
        table.append(pa.Table.from_pandas(pd.DataFrame(rows)))

        tp = table.location()

        df = spark.read.format("iceberg").load(tp).filter("team IN ('backend','frontend')")
        assert df.count() == 2

        df = spark.read.format("iceberg").load(tp).filter("dept > 'engineering'")
        assert df.count() == 2
    finally:
        catalog.drop_table("default.prune_string_in_range")


def test_metrics_based_pruning_numeric(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    table = catalog.create_table(
        identifier="default.prune_metrics_numeric",
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="val", field_type=DoubleType(), required=False),
        ),
    )
    try:
        data = []
        for chunk in range(4):
            for i in range(10):
                data.append({"id": chunk * 10 + i, "val": float(i)})
        for i in range(0, len(data), 10):
            table.append(pa.Table.from_pandas(pd.DataFrame(data[i : i + 10])))

        tp = table.location()

        df = spark.read.format("iceberg").load(tp).filter("val >= 8.0")
        assert df.count() == 8

        df = spark.read.format("iceberg").load(tp).filter("val BETWEEN 3.0 AND 4.0")
        assert df.count() == 8
    finally:
        catalog.drop_table("default.prune_metrics_numeric")


def test_limit_pushdown_behavior(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    table = catalog.create_table(
        identifier="default.prune_limit",
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="flag", field_type=BooleanType(), required=False),
        ),
    )
    try:
        rows = [{"id": i, "flag": i % 2 == 0} for i in range(100)]
        for i in range(0, len(rows), 20):
            table.append(pa.Table.from_pandas(pd.DataFrame(rows[i : i + 20]).astype({"id": "int64"})))

        tp = table.location()

        df = spark.read.format("iceberg").load(tp).filter("flag = true").limit(7)
        assert df.count() == 7
    finally:
        catalog.drop_table("default.prune_limit")
