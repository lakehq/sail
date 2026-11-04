"""Tests for Spark Catalog API field names.

PySpark maps tableName from the server to name in the client Table namedtuple.
The camelCase fields (tableType, isTemporary) are passed through directly.
"""

import uuid
from pathlib import Path

import pytest
from pyspark.errors.exceptions.connect import IllegalArgumentException
from pyspark.sql.types import LongType, StructField, StructType

from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.testing.spark.utils.sql import escape_sql_string_literal


@pytest.fixture(scope="module", autouse=True)
def setup_view(spark):
    """Create a temporary view for testing."""
    spark.sql("SELECT 1 AS col").createOrReplaceTempView("test_view")
    yield
    spark.catalog.dropTempView("test_view")


def test_list_tables_returns_name(spark):
    """listTables returns table with 'name' field (mapped from tableName)."""
    tables = spark.catalog.listTables()
    table_names = [t.name for t in tables]
    assert "test_view" in table_names


def test_list_tables_returns_table_type(spark):
    """listTables returns tableType in camelCase."""
    tables = spark.catalog.listTables()
    test_table = next(t for t in tables if t.name == "test_view")
    assert test_table.tableType == "TEMPORARY"


def test_list_tables_returns_is_temporary(spark):
    """listTables returns isTemporary in camelCase."""
    tables = spark.catalog.listTables()
    test_table = next(t for t in tables if t.name == "test_view")
    assert test_table.isTemporary is True


def test_show_tables_returns_spark_sql_shape(spark):
    """SHOW TABLES returns the Spark SQL 3-column output shape."""
    tables = spark.sql("SHOW TABLES")
    assert tables.columns == ["database", "tableName", "isTemporary"]

    test_table = next(row for row in tables.collect() if row.tableName == "test_view")
    assert test_table.isTemporary is True


def test_show_table_extended_returns_spark_sql_shape(spark):
    """SHOW TABLE EXTENDED returns the Spark SQL 4-column output shape."""
    tables = spark.sql("SHOW TABLE EXTENDED LIKE 'test_view'")
    assert tables.columns == ["database", "tableName", "isTemporary", "information"]

    test_table = next(row for row in tables.collect() if row.tableName == "test_view")
    assert test_table.isTemporary is True
    assert "Type: TEMPORARY" in test_table.information
    assert "Schema: root" in test_table.information
    assert " |-- col: int (nullable = " in test_table.information


@pytest.mark.parametrize(
    "sql",
    [
        "DESCRIBE TABLE EXTENDED test_view",
        "DESCRIBE EXTENDED test_view",
    ],
)
def test_describe_extended_accepts_long_and_short_forms(spark, sql):
    """DESCRIBE EXTENDED accepts both Spark table forms."""
    describe = spark.sql(sql)
    assert describe.columns == ["col_name", "data_type", "comment"]

    rows = describe.collect()
    column_row = next(row for row in rows if row.col_name == "col")
    assert column_row.data_type == "int"

    metadata_marker = next(row for row in rows if row.col_name == "# Detailed Table Information")
    assert metadata_marker.data_type == ""


@pytest.mark.integration
def test_persistent_table_defaults_to_managed(spark):
    """Persistent tables without an explicit location are managed."""
    table_name = "test_external_default"
    try:
        spark.sql(f"CREATE TABLE {table_name} (id INT) USING PARQUET")

        table = spark.catalog.getTable(table_name)
        assert table.tableType == "MANAGED"

        show_rows = spark.sql(f"SHOW TABLE EXTENDED LIKE '{table_name}'").collect()
        show_row = next(row for row in show_rows if row.tableName == table_name)
        assert "Type: MANAGED" in show_row.information

        describe_rows = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()
        type_row = next(row for row in describe_rows if row.col_name == "Type")
        assert type_row.data_type == "MANAGED"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.integration
def test_persistent_table_with_location_is_external(spark, tmp_path):
    """Persistent table created with LOCATION surfaces EXTERNAL type."""
    table_name = "test_external_with_location"
    location = str(tmp_path / table_name)
    Path(location).mkdir(parents=True, exist_ok=True)
    try:
        spark.sql(f"CREATE TABLE {table_name} (id INT) USING PARQUET LOCATION '{escape_sql_string_literal(location)}'")

        table = spark.catalog.getTable(table_name)
        assert table.tableType == "EXTERNAL"

        show_rows = spark.sql(f"SHOW TABLE EXTENDED LIKE '{table_name}'").collect()
        show_row = next(row for row in show_rows if row.tableName == table_name)
        assert "Type: EXTERNAL" in show_row.information

        describe_rows = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()
        type_row = next(row for row in describe_rows if row.col_name == "Type")
        assert type_row.data_type == "EXTERNAL"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


@pytest.fixture(params=[False, True], ids=["managed", "external"])
def created_table(spark, tmp_path, request):
    name = f"create_table_{uuid.uuid4().hex}"
    schema = StructType([StructField("id", LongType())])
    external = request.param
    options = {"path": str(tmp_path / "data")} if external else {}
    try:
        result = spark.catalog.createTable(name, schema=schema, source="parquet", **options)
        yield name, schema, external, result
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
    assert not spark.catalog.tableExists(name)


# Ported from Spark 3 (3.5.9): pyspark/sql/catalog.py, Catalog.createTable doctest.
# Uses unique managed/external table names and deterministic cleanup.
def test_catalog_create_table(spark, created_table):
    name, schema, external, _ = created_table
    assert spark.table(name).schema == schema
    assert spark.table(name).collect() == []
    assert spark.catalog.getTable(name).tableType == ("EXTERNAL" if external else "MANAGED")


# Regression extending Spark 3 (3.5.9): pyspark/sql/catalog.py, Catalog.createTable doctest.
# Also verify that the returned DataFrame reads the created table.
@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="Catalog.createTable returns an unqueryable catalog command relation instead of the created table",
    raises=IllegalArgumentException,
    strict=True,
)
def test_create_table_returns_readable_dataframe(created_table):
    _, schema, _, result = created_table
    assert result.schema == schema
    assert result.collect() == []
