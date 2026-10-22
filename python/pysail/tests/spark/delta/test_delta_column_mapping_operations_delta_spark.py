"""Sail queries over Delta tables written by Delta Spark, compared with Delta Spark.

Delta Spark writes column-mapped tables, and both Sail and Delta Spark run the same
queries against them. The results must match exactly. Spark writes timestamps as INT96
by default, which also covers reading nested fields next to INT96 columns in ID mode.
"""

# ruff: noqa: S608

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pysail.tests.spark.delta.test_delta_column_mapping_operations import (
    _QUERIES,
    _ROWS_FIRST,
    _ROWS_SECOND,
    _SCHEMA,
)

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession

pytestmark = pytest.mark.integration

# Delta Spark rejects the special characters in the column names without column mapping,
# so only column-mapped tables are written here.
_VARIANTS = [
    pytest.param((mode, partitioned), id=f"{mode}-{'partitioned' if partitioned else 'unpartitioned'}")
    for mode in ("name", "id")
    for partitioned in (False, True)
]

# Queries that fail for reasons unrelated to column mapping. The value is the reason,
# and whether the failure only happens on partitioned tables.
_KNOWN_QUERY_FAILURES = {
    "nvl_array_struct": ("type coercion fails on nested fields carrying field metadata", False),
    "filter_array_struct": (
        "a higher-order function over an array of structs in a filter cannot resolve the column",
        False,
    ),
    "semi_join": ("a correlated reference to a nested field cannot be resolved", False),
    "intersect": ("INTERSECT on array of struct columns fails to compare list values", False),
    "select_casts": ("casting arrays and maps with null elements to STRING prints NULL instead of null", False),
}


@pytest.fixture(scope="module", params=_VARIANTS)
def delta_spark_table(request, delta_jvm_spark: SparkSession, tmp_path_factory) -> dict:
    """Write a Delta table with Delta Spark in two commits and return its location."""
    column_mapping_mode, partitioned = request.param
    path = tmp_path_factory.mktemp(f"delta_spark_{column_mapping_mode}") / "table"
    for rows, mode in ((_ROWS_FIRST, "overwrite"), (_ROWS_SECOND, "append")):
        writer = delta_jvm_spark.createDataFrame(rows, schema=_SCHEMA).write.format("delta").mode(mode)
        if column_mapping_mode != "none":
            writer = writer.option("delta.columnMapping.mode", column_mapping_mode)
        if partitioned:
            writer = writer.partitionBy("grp")
        writer.save(str(path))
    return {"path": str(path), "mode": column_mapping_mode, "partitioned": partitioned}


@pytest.fixture
def utc(spark: SparkSession, delta_jvm_spark: SparkSession):
    sessions = (spark, delta_jvm_spark)
    previous = [session.conf.get("spark.sql.session.timeZone") for session in sessions]
    for session in sessions:
        session.conf.set("spark.sql.session.timeZone", "UTC")
    yield
    for session, timezone in zip(sessions, previous, strict=True):
        session.conf.set("spark.sql.session.timeZone", timezone)


def test_delta_spark_table_schema_matches(
    spark: SparkSession, delta_jvm_spark: SparkSession, delta_spark_table: dict
) -> None:
    sail = spark.read.format("delta").load(delta_spark_table["path"]).schema
    reference = delta_jvm_spark.read.format("delta").load(delta_spark_table["path"]).schema
    assert [(f.name, f.dataType) for f in sail.fields] == [(f.name, f.dataType) for f in reference.fields]


@pytest.mark.usefixtures("utc")
@pytest.mark.parametrize(("name", "query"), list(_QUERIES.items()), ids=list(_QUERIES))
def test_delta_spark_table_query_matches(
    request,
    spark: SparkSession,
    delta_jvm_spark: SparkSession,
    delta_spark_table: dict,
    name: str,
    query: str,
) -> None:
    if name in _KNOWN_QUERY_FAILURES:
        reason, partitioned_only = _KNOWN_QUERY_FAILURES[name]
        if delta_spark_table["partitioned"] or not partitioned_only:
            request.applymarker(pytest.mark.xfail(reason=reason, strict=True))
    view = f"delta_spark_{delta_spark_table['mode']}_{int(delta_spark_table['partitioned'])}"
    spark.read.format("delta").load(delta_spark_table["path"]).createOrReplaceTempView(view)
    delta_jvm_spark.read.format("delta").load(delta_spark_table["path"]).createOrReplaceTempView(view)
    try:
        actual = spark.sql(query.format(t=view)).collect()
        expected = delta_jvm_spark.sql(query.format(t=view)).collect()
    finally:
        spark.catalog.dropTempView(view)
        delta_jvm_spark.catalog.dropTempView(view)
    assert actual == expected


@pytest.mark.parametrize("partitioned", [False, True], ids=["unpartitioned", "partitioned"])
def test_sail_reads_delta_spark_renamed_and_dropped_columns(
    spark: SparkSession,
    delta_jvm_spark: SparkSession,
    tmp_path: Path,
    partitioned: bool,  # noqa: FBT001
) -> None:
    path = tmp_path / "renamed"
    table = f"delta.`{path}`"
    writer = (
        delta_jvm_spark.createDataFrame(_ROWS_FIRST + _ROWS_SECOND, schema=_SCHEMA)
        .write.format("delta")
        .option("delta.columnMapping.mode", "name")
    )
    if partitioned:
        writer = writer.partitionBy("grp")
    writer.save(str(path))
    delta_jvm_spark.sql(f"ALTER TABLE {table} RENAME COLUMN name TO renamed")
    delta_jvm_spark.sql(f"ALTER TABLE {table} RENAME COLUMN s.inner.d TO d_renamed")
    delta_jvm_spark.sql(f"ALTER TABLE {table} RENAME COLUMN `dotted.col` TO `renamed.dotted`")
    delta_jvm_spark.sql(f"ALTER TABLE {table} DROP COLUMN big")
    delta_jvm_spark.sql(f"ALTER TABLE {table} DROP COLUMN s.b")
    delta_jvm_spark.sql(
        f"INSERT INTO {table} SELECT * FROM {table} WHERE id = 1"
        if not partitioned
        else f"INSERT INTO {table} SELECT * FROM {table} WHERE id = 2"
    )

    queries = [
        "SELECT * FROM {t} ORDER BY id, renamed",
        "SELECT id, renamed, `renamed.dotted`, s.a, s.inner.d_renamed FROM {t} ORDER BY id, renamed",
        "SELECT id FROM {t} WHERE s.inner.d_renamed = 'inner3' OR renamed = 'n4' ORDER BY id",
        "SELECT renamed, count(*) AS n FROM {t} GROUP BY renamed ORDER BY renamed",
        "SELECT grp, max(s.inner) AS mi FROM {t} GROUP BY grp ORDER BY grp",
    ]
    spark.read.format("delta").load(str(path)).createOrReplaceTempView("renamed_view")
    delta_jvm_spark.read.format("delta").load(str(path)).createOrReplaceTempView("renamed_view")
    try:
        schema = spark.table("renamed_view").schema
        assert "big" not in schema.fieldNames()
        assert [field.name for field in schema["s"].dataType.fields] == ["a", "inner"]
        for query in queries:
            sql = query.format(t="renamed_view")
            assert spark.sql(sql).collect() == delta_jvm_spark.sql(sql).collect(), sql
    finally:
        spark.catalog.dropTempView("renamed_view")
        delta_jvm_spark.catalog.dropTempView("renamed_view")


@pytest.mark.parametrize("partitioned", [False, True], ids=["unpartitioned", "partitioned"])
def test_sail_coalesces_struct_from_delta_spark_table(
    request,
    spark: SparkSession,
    delta_jvm_spark: SparkSession,
    tmp_path: Path,
    partitioned: bool,  # noqa: FBT001
) -> None:
    if partitioned:
        request.applymarker(
            pytest.mark.xfail(
                reason="a struct built by an expression lacks the column mapping metadata of the planned struct type",
                strict=True,
            )
        )
    path = tmp_path / "coalesce"
    table = f"delta.`{path}`"
    partition_clause = "PARTITIONED BY (name)" if partitioned else ""
    delta_jvm_spark.sql(
        f"CREATE TABLE {table} (id INT, name STRING, s STRUCT<a: INT, b: STRING>) USING DELTA {partition_clause}"
        " TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"
    )
    delta_jvm_spark.sql(
        f"INSERT INTO {table} VALUES (1, 'a', named_struct('a', 1, 'b', 'x')),"
        " (2, 'a', named_struct('a', 2, 'b', 'y')), (3, 'b', NULL)"
    )
    query = "SELECT id, coalesce(s, named_struct('a', 0, 'b', '')) AS s2 FROM {t} ORDER BY id"
    spark.read.format("delta").load(str(path)).createOrReplaceTempView("coalesce_view")
    try:
        actual = spark.sql(query.format(t="coalesce_view")).collect()
    finally:
        spark.catalog.dropTempView("coalesce_view")
    assert actual == delta_jvm_spark.sql(query.format(t=table)).collect()
