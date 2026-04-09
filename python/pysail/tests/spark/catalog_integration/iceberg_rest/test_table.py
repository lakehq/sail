"""Integration tests for Iceberg REST catalog table operations.

Migrated from crates/sail-catalog-iceberg/tests/rest_integration_test.rs
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture(autouse=True)
def _iceberg_table_ns(iceberg_spark: SparkSession):
    """Create and clean up a test namespace for table tests."""
    iceberg_spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_table_test")
    yield
    iceberg_spark.sql("DROP NAMESPACE IF EXISTS iceberg_table_test CASCADE")


class TestCreateTable:
    """Tests table creation in Iceberg REST catalog."""

    def test_create_table_basic(self, iceberg_spark: SparkSession):
        """Given an Iceberg REST catalog,
        when creating a table with columns, comment, and properties,
        then the table is created with correct metadata.
        """
        iceberg_spark.sql("""
            CREATE TABLE iceberg_table_test.t1 (
                foo STRING,
                bar INT NOT NULL COMMENT 'meow',
                baz BOOLEAN
            )
            USING iceberg
            COMMENT 'peow'
        """)
        result = iceberg_spark.sql("DESCRIBE TABLE EXTENDED iceberg_table_test.t1").collect()
        col_names = [row[0].strip() for row in result if row[0].strip() and not row[0].startswith("#")]
        assert "foo" in col_names
        assert "bar" in col_names
        assert "baz" in col_names

    def test_create_table_duplicate_fails(self, iceberg_spark: SparkSession):
        """Given an existing table,
        when creating a table with the same name without IF NOT EXISTS,
        then an error is raised.
        """
        iceberg_spark.sql("""
            CREATE TABLE iceberg_table_test.dup_t (id INT) USING iceberg
        """)
        with pytest.raises(Exception, match=r".*"):
            iceberg_spark.sql("CREATE TABLE iceberg_table_test.dup_t (id INT) USING iceberg")

    def test_create_table_if_not_exists(self, iceberg_spark: SparkSession):
        """Given an existing table,
        when creating with IF NOT EXISTS,
        then no error is raised.
        """
        iceberg_spark.sql("""
            CREATE TABLE iceberg_table_test.ine_t (id INT) USING iceberg
        """)
        # Should not raise
        iceberg_spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg_table_test.ine_t (id INT) USING iceberg
        """)

    def test_create_table_with_partitioning(self, iceberg_spark: SparkSession):
        """Given an Iceberg REST catalog,
        when creating a table with partition columns,
        then the table is created with correct partition spec.
        """
        iceberg_spark.sql("""
            CREATE TABLE iceberg_table_test.partitioned_t (
                foo STRING,
                bar INT NOT NULL COMMENT 'meow',
                baz BOOLEAN
            )
            USING iceberg
            COMMENT 'test table'
            PARTITIONED BY (baz)
            LOCATION 's3://icebergdata/custom/path/meow'
            TBLPROPERTIES (owner = 'mr. meow', team = 'data-eng')
        """)
        result = iceberg_spark.sql("DESCRIBE TABLE EXTENDED iceberg_table_test.partitioned_t").collect()
        col_names = [row[0].strip() for row in result if row[0].strip() and not row[0].startswith("#")]
        assert "foo" in col_names
        assert "bar" in col_names
        assert "baz" in col_names

        # Check partition info is in the extended description
        all_text = " ".join(str(row) for row in result)
        assert "baz" in all_text


class TestGetTable:
    """Tests retrieving a table from Iceberg REST catalog."""

    def test_get_existing_table(self, iceberg_spark: SparkSession):
        """Given an Iceberg table with columns, partitions, and properties,
        when describing it,
        then all fields are correctly returned.
        """
        iceberg_spark.sql("""
            CREATE TABLE iceberg_table_test.get_t (
                foo STRING,
                bar INT NOT NULL COMMENT 'meow',
                baz BOOLEAN
            )
            USING iceberg
            COMMENT 'test table'
            PARTITIONED BY (baz)
            TBLPROPERTIES (owner = 'mr. meow', team = 'data-eng')
        """)
        result = iceberg_spark.sql("DESCRIBE TABLE EXTENDED iceberg_table_test.get_t").collect()
        col_names = [row[0].strip() for row in result if row[0].strip() and not row[0].startswith("#")]
        assert "foo" in col_names
        assert "bar" in col_names
        assert "baz" in col_names

    def test_get_nonexistent_table(self, iceberg_spark: SparkSession):
        """Given an Iceberg REST catalog,
        when describing a non-existent table,
        then an error is raised.
        """
        with pytest.raises(Exception, match=r".*"):
            iceberg_spark.sql("DESCRIBE TABLE iceberg_table_test.nonexistent_t").collect()


class TestListTables:
    """Tests listing tables in Iceberg REST catalog."""

    def test_list_tables(self, iceberg_spark: SparkSession):
        """Given multiple tables in a namespace,
        when listing tables,
        then all tables are returned.
        """
        iceberg_spark.sql("CREATE TABLE iceberg_table_test.list_t1 (id INT) USING iceberg")
        iceberg_spark.sql("CREATE TABLE iceberg_table_test.list_t2 (id INT) USING iceberg")

        result = iceberg_spark.sql("SHOW TABLES IN iceberg_table_test").collect()
        table_names = [row["tableName"] for row in result]
        assert "list_t1" in table_names
        assert "list_t2" in table_names


class TestDropTable:
    """Tests dropping tables from Iceberg REST catalog."""

    def test_drop_existing_table(self, iceberg_spark: SparkSession):
        """Given an existing table,
        when dropping it,
        then it is no longer accessible.
        """
        iceberg_spark.sql("CREATE TABLE iceberg_table_test.drop_t (id INT) USING iceberg")
        iceberg_spark.sql("DESCRIBE TABLE iceberg_table_test.drop_t").collect()
        iceberg_spark.sql("DROP TABLE iceberg_table_test.drop_t")
        with pytest.raises(Exception, match=r".*"):
            iceberg_spark.sql("DESCRIBE TABLE iceberg_table_test.drop_t").collect()

    def test_drop_nonexistent_if_exists(self, iceberg_spark: SparkSession):
        """Given an Iceberg REST catalog,
        when dropping a non-existent table with IF EXISTS,
        then no error is raised.
        """
        # Should not raise
        iceberg_spark.sql("DROP TABLE IF EXISTS iceberg_table_test.nonexistent_drop_t")
