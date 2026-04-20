"""Tests for Spark Catalog API field names.

PySpark maps tableName from the server to name in the client Table namedtuple.
The camelCase fields (tableType, isTemporary) are passed through directly.
"""

import pytest


class TestListTables:
    """Test spark.catalog.listTables() returns correct field names and values."""

    @pytest.fixture(autouse=True)
    def setup_view(self, spark):
        """Create a temporary view for testing."""
        spark.sql("SELECT 1 AS col").createOrReplaceTempView("test_view")
        yield
        spark.catalog.dropTempView("test_view")

    def test_list_tables_returns_name(self, spark):
        """listTables returns table with 'name' field (mapped from tableName)."""
        tables = spark.catalog.listTables()
        table_names = [t.name for t in tables]
        assert "test_view" in table_names

    def test_list_tables_returns_table_type(self, spark):
        """listTables returns tableType in camelCase."""
        tables = spark.catalog.listTables()
        test_table = next(t for t in tables if t.name == "test_view")
        assert test_table.tableType == "TEMPORARY"

    def test_list_tables_returns_is_temporary(self, spark):
        """listTables returns isTemporary in camelCase."""
        tables = spark.catalog.listTables()
        test_table = next(t for t in tables if t.name == "test_view")
        assert test_table.isTemporary is True

    def test_show_tables_returns_spark_sql_shape(self, spark):
        """SHOW TABLES returns the Spark SQL 3-column output shape."""
        tables = spark.sql("SHOW TABLES")
        assert tables.columns == ["database", "tableName", "isTemporary"]

        test_table = next(row for row in tables.collect() if row.tableName == "test_view")
        assert test_table.isTemporary is True

    def test_show_table_extended_returns_spark_sql_shape(self, spark):
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
    def test_describe_extended_accepts_long_and_short_forms(self, spark, sql):
        """DESCRIBE EXTENDED accepts both Spark table forms."""
        describe = spark.sql(sql)
        assert describe.columns == ["col_name", "data_type", "comment"]

        rows = describe.collect()
        column_row = next(row for row in rows if row.col_name == "col")
        assert column_row.data_type == "int"

        metadata_marker = next(row for row in rows if row.col_name == "# Detailed Table Information")
        assert metadata_marker.data_type == ""


class TestListDatabases:
    """Test spark.catalog.listDatabases() returns correct field names."""

    def test_list_databases_returns_name(self, spark):
        """listDatabases returns database with 'name' field."""
        databases = spark.catalog.listDatabases()
        db_names = [db.name for db in databases]
        assert "default" in db_names
