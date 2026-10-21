"""Catalog resolution and DML over column-mapped Delta tables in Unity Catalog."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import pytest

from pysail.tests.spark.delta.test_delta_column_mapping_operations import (
    _QUERIES,
    _ROWS_FIRST,
    _ROWS_SECOND,
    _SCHEMA,
    _row,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_DATABASE = "unity_column_mapping_test"
# The complete expression matrix lives in the Delta tests. These cases exercise
# catalog resolution of scalar, nested, partition, and mapped fields.
_QUERY_NAMES = [
    "select_scalars",
    "select_special_names",
    "select_struct_fields",
    "select_array_elements",
    "select_map_values",
    "filter_partition_column",
    "filter_partition_null",
    "filter_struct_field",
    "group_by_struct",
    "self_join_struct",
    "explode_map",
    "window_struct",
    "coalesce_struct",
    "union_all",
]


def _create_table(spark: SparkSession, table: str, *, column_mapping_mode: str, partitioned: bool) -> None:
    columns = ", ".join(f"`{field.name}` {field.dataType.simpleString()}" for field in _SCHEMA.fields)
    partition_clause = "PARTITIONED BY (grp)" if partitioned else ""
    properties = (
        f"TBLPROPERTIES ('delta.columnMapping.mode' = '{column_mapping_mode}')" if column_mapping_mode != "none" else ""
    )
    spark.sql(f"CREATE TABLE {table} ({columns}) USING delta {partition_clause} {properties}")
    column_order = [field.name for field in spark.table(table).schema.fields]
    for rows in (_ROWS_FIRST, _ROWS_SECOND):
        df = spark.createDataFrame(rows, schema=_SCHEMA)
        df.select(*[df[f"`{name}`"] for name in column_order]).write.insertInto(table)


@pytest.fixture(scope="module", params=[False, True], ids=["unpartitioned", "partitioned"])
def unity_tables(request, spark: SparkSession) -> dict:
    partitioned = request.param
    suffix = f"{'part' if partitioned else 'flat'}_{uuid.uuid4().hex[:8]}"
    mapped = f"{_DATABASE}.mapped_{suffix}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_DATABASE}")
    _create_table(spark, mapped, column_mapping_mode="name", partitioned=partitioned)
    try:
        yield {"mapped": mapped, "partitioned": partitioned}
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {mapped}")


def test_unity_column_mapped_table_schema(spark: SparkSession, unity_tables: dict) -> None:
    mapped = spark.table(unity_tables["mapped"]).schema
    assert [(f.name, f.dataType) for f in mapped.fields] == [(f.name, f.dataType) for f in _SCHEMA.fields]


@pytest.mark.parametrize("name", _QUERY_NAMES)
def test_unity_column_mapped_query(spark: SparkSession, unity_tables: dict, name: str) -> None:
    query, expected = _QUERIES[name]
    actual = spark.sql(query.format(t=unity_tables["mapped"])).collect()
    assert [tuple(row) for row in actual] == expected


def test_unity_column_mapped_dml(spark: SparkSession, unity_tables: dict) -> None:
    updated = [
        *_ROWS_FIRST[:3],
        _row(4, None, a=5, b="w", c=40, name="updated"),
        _ROWS_SECOND[0],
        _row(6, "g1", a=7, b="v", c=60, name="updated", flag=None, dbl=None),
        _ROWS_SECOND[2],
        _row(8, None, a=9, b="t", c=80, name="updated", arr=[8, None], m={"k1": None}),
    ]
    for statement, expected in [
        (
            "UPDATE {t} SET name = 'updated', s = named_struct('a', s.a + 1, 'b', s.b, 'inner', s.inner) WHERE s.a > 3",
            updated,
        ),
        (
            "DELETE FROM {t} WHERE s = named_struct('a', 1, 'b', 'x', 'inner', named_struct('c', 10, 'd', 'inner1'))",
            updated[1:],
        ),
        ("DELETE FROM {t} WHERE m['k1'] = 2", updated[2:]),
    ]:
        spark.sql(statement.format(t=unity_tables["mapped"]))
        rows = (
            spark.table(unity_tables["mapped"]).select(*[f"`{f.name}`" for f in _SCHEMA.fields]).orderBy("id").collect()
        )
        assert [tuple(row) for row in rows] == expected, statement
