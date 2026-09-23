# ruff: noqa: S608
"""Queries over column-mapped Delta tables managed by Unity Catalog.

Each query runs against a column-mapped Delta table and a Delta table without column
mapping holding the same rows, both resolved through Unity Catalog, and the results
must match exactly.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import pytest

from pysail.tests.spark.delta.test_delta_column_mapping_operations import (
    _KNOWN_QUERY_FAILURES,
    _QUERIES,
    _ROWS_FIRST,
    _ROWS_SECOND,
    _SCHEMA,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_DATABASE = "unity_column_mapping_test"

# The tables here are created with SQL, so their nested fields carry no Spark field
# metadata, and higher-order functions over arrays of structs work on partitioned tables.
_UNITY_KNOWN_QUERY_FAILURES = {
    name: failure for name, failure in _KNOWN_QUERY_FAILURES.items() if name != "higher_order"
}


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
    reference = f"{_DATABASE}.reference_{suffix}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_DATABASE}")
    for table in (mapped, reference):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    _create_table(spark, mapped, column_mapping_mode="name", partitioned=partitioned)
    _create_table(spark, reference, column_mapping_mode="none", partitioned=partitioned)
    yield {"mapped": mapped, "reference": reference, "partitioned": partitioned}
    for table in (mapped, reference):
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_unity_column_mapped_table_schema_matches_reference(spark: SparkSession, unity_tables: dict) -> None:
    mapped = spark.table(unity_tables["mapped"]).schema
    reference = spark.table(unity_tables["reference"]).schema
    assert [(f.name, f.dataType) for f in mapped.fields] == [(f.name, f.dataType) for f in reference.fields]


@pytest.mark.parametrize(("name", "query"), list(_QUERIES.items()), ids=list(_QUERIES))
def test_unity_column_mapped_query_matches_reference(
    request, spark: SparkSession, unity_tables: dict, name: str, query: str
) -> None:
    if name in _UNITY_KNOWN_QUERY_FAILURES:
        reason, partitioned_only = _UNITY_KNOWN_QUERY_FAILURES[name]
        if unity_tables["partitioned"] or not partitioned_only:
            request.applymarker(pytest.mark.xfail(reason=reason, strict=True))
    actual = spark.sql(query.format(t=unity_tables["mapped"])).collect()
    expected = spark.sql(query.format(t=unity_tables["reference"])).collect()
    assert actual == expected


def test_unity_column_mapped_dml(spark: SparkSession, unity_tables: dict) -> None:
    statements = [
        "UPDATE {t} SET name = 'updated', s = named_struct('a', s.a + 1, 'b', s.b, 'inner', s.inner) WHERE s.a > 3",
        "DELETE FROM {t} WHERE s = named_struct('a', 1, 'b', 'x', 'inner', named_struct('c', 10, 'd', 'inner1'))",
        "DELETE FROM {t} WHERE m['k1'] = 2",
    ]
    for statement in statements:
        for key in ("mapped", "reference"):
            spark.sql(statement.format(t=unity_tables[key])).collect()
        mapped = spark.table(unity_tables["mapped"]).orderBy("id").collect()
        reference = spark.table(unity_tables["reference"]).orderBy("id").collect()
        assert mapped == reference, statement
