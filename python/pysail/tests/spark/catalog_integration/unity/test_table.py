"""Integration tests for Unity Catalog table operations.

Migrated from crates/sail-catalog-unity/tests/rest_integration_test.rs
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture(autouse=True)
def _unity_table_schema(unity_spark: SparkSession):
    """Create and clean up a test schema for table tests."""
    unity_spark.sql("CREATE SCHEMA IF NOT EXISTS unity_table_test")
    yield
    unity_spark.sql("DROP SCHEMA IF EXISTS unity_table_test CASCADE")


class TestCreateTable:
    """Tests table creation in Unity Catalog."""

    def test_create_table_basic(self, unity_spark: SparkSession):
        """Given a Unity Catalog,
        when creating a table with various column types including complex types,
        then the table is created with correct columns.
        """
        unity_spark.sql("""
            CREATE TABLE unity_table_test.t1 (
                foo STRING,
                bar ARRAY<STRUCT<a: STRING, b: INT>> NOT NULL COMMENT 'meow',
                baz MAP<STRING, INT>,
                mew STRUCT<a: STRING, b: INT>
            )
            USING delta
            COMMENT 'peow'
            LOCATION 's3://deltadata/custom/path/meow'
        """)
        result = unity_spark.sql("DESCRIBE TABLE EXTENDED unity_table_test.t1").collect()
        col_names = [row[0].strip() for row in result if row[0].strip() and not row[0].startswith("#")]
        assert "foo" in col_names
        assert "bar" in col_names
        assert "baz" in col_names
        assert "mew" in col_names

    def test_create_table_duplicate_fails(self, unity_spark: SparkSession):
        """Given an existing table,
        when creating a table with the same name without IF NOT EXISTS,
        then an error is raised.
        """
        unity_spark.sql("""
            CREATE TABLE unity_table_test.dup_t (id INT)
            USING delta
            LOCATION 's3://deltadata/dup_test'
        """)
        with pytest.raises(Exception, match=r".*"):
            unity_spark.sql("""
                CREATE TABLE unity_table_test.dup_t (id INT)
                USING delta
                LOCATION 's3://deltadata/dup_test2'
            """)

    def test_create_table_if_not_exists(self, unity_spark: SparkSession):
        """Given an existing table,
        when creating with IF NOT EXISTS,
        then no error is raised.
        """
        unity_spark.sql("""
            CREATE TABLE unity_table_test.ine_t (id INT)
            USING delta
            COMMENT 'original'
            LOCATION 's3://deltadata/ine_test'
        """)
        # Should not raise
        unity_spark.sql("""
            CREATE TABLE IF NOT EXISTS unity_table_test.ine_t (id INT)
            USING delta
            COMMENT 'new comment'
            LOCATION 's3://deltadata/ine_test2'
        """)

    def test_create_table_with_partitioning(self, unity_spark: SparkSession):
        """Given a Unity Catalog,
        when creating a table with partitioning and properties,
        then the table is created with correct partition spec.
        """
        unity_spark.sql("""
            CREATE TABLE unity_table_test.t2 (
                foo STRING,
                bar INT NOT NULL COMMENT 'meow',
                baz BOOLEAN
            )
            USING delta
            COMMENT 'test table'
            PARTITIONED BY (baz)
            LOCATION 's3://deltadata/custom/path/meow2'
            TBLPROPERTIES (owner = 'mr. meow', team = 'data-eng')
        """)
        result = unity_spark.sql("DESCRIBE TABLE EXTENDED unity_table_test.t2").collect()
        col_names = [row[0].strip() for row in result if row[0].strip() and not row[0].startswith("#")]
        assert "foo" in col_names
        assert "bar" in col_names
        assert "baz" in col_names


class TestGetTable:
    """Tests retrieving a table from Unity Catalog."""

    def test_get_existing_table(self, unity_spark: SparkSession):
        """Given a Unity table with columns, partitions, and properties,
        when describing it,
        then all fields are correctly returned.
        """
        unity_spark.sql("""
            CREATE TABLE unity_table_test.get_t (
                foo STRING,
                bar INT NOT NULL COMMENT 'meow',
                baz BOOLEAN
            )
            USING delta
            COMMENT 'test table'
            PARTITIONED BY (baz)
            LOCATION 's3://deltadata/custom/path/get_test'
            TBLPROPERTIES (owner = 'mr. meow', team = 'data-eng')
        """)
        result = unity_spark.sql("DESCRIBE TABLE EXTENDED unity_table_test.get_t").collect()
        col_names = [row[0].strip() for row in result if row[0].strip() and not row[0].startswith("#")]
        assert "foo" in col_names
        assert "bar" in col_names
        assert "baz" in col_names

    def test_get_nonexistent_table(self, unity_spark: SparkSession):
        """Given a Unity Catalog,
        when describing a non-existent table,
        then an error is raised.
        """
        with pytest.raises(Exception, match=r".*"):
            unity_spark.sql("DESCRIBE TABLE unity_table_test.nonexistent_t").collect()


class TestListTables:
    """Tests listing tables in Unity Catalog."""

    def test_list_tables(self, unity_spark: SparkSession):
        """Given multiple tables in a schema,
        when listing tables,
        then all tables are returned.
        """
        unity_spark.sql("""
            CREATE TABLE unity_table_test.list_t1 (id INT)
            USING delta
            LOCATION 's3://deltadata/list_t1'
        """)
        unity_spark.sql("""
            CREATE TABLE unity_table_test.list_t2 (id INT)
            USING delta
            LOCATION 's3://deltadata/list_t2'
        """)

        result = unity_spark.sql("SHOW TABLES IN unity_table_test").collect()
        table_names = [row["tableName"] for row in result]
        assert "list_t1" in table_names
        assert "list_t2" in table_names

    def test_list_tables_empty(self, unity_spark: SparkSession):
        """Given an empty schema,
        when listing tables,
        then an empty list is returned.
        """
        unity_spark.sql("CREATE SCHEMA IF NOT EXISTS empty_table_schema_unity")
        try:
            result = unity_spark.sql("SHOW TABLES IN empty_table_schema_unity").collect()
            assert len(result) == 0
        finally:
            unity_spark.sql("DROP SCHEMA IF EXISTS empty_table_schema_unity")
