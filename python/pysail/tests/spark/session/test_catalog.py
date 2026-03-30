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


class TestListDatabases:
    """Test spark.catalog.listDatabases() returns correct field names."""

    def test_list_databases_returns_name(self, spark):
        """listDatabases returns database with 'name' field."""
        databases = spark.catalog.listDatabases()
        db_names = [db.name for db in databases]
        assert "default" in db_names
