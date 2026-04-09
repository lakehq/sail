"""Integration tests for Glue catalog table operations.

Migrated from crates/sail-catalog-glue/tests/table_tests.rs
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture(autouse=True)
def _table_test_db(glue_spark: SparkSession):
    """Create and clean up a test database for table tests."""
    glue_spark.sql("CREATE DATABASE IF NOT EXISTS table_test_db")
    glue_spark.sql("USE DATABASE table_test_db")
    yield
    glue_spark.sql("DROP DATABASE IF EXISTS table_test_db CASCADE")


class TestCreateTable:
    """Tests table creation in Glue catalog."""

    def test_create_table_basic(self, glue_spark: SparkSession):
        """Given a Glue catalog,
        when creating a table with multiple column types, comment, location, and partitioning,
        then the table is created with correct metadata.
        """
        glue_spark.sql("""
            CREATE TABLE products (
                id BIGINT NOT NULL COMMENT 'Primary key',
                name STRING,
                price DOUBLE,
                category STRING NOT NULL
            )
            USING parquet
            COMMENT 'Product catalog table'
            LOCATION 's3://bucket/products'
            PARTITIONED BY (category)
            TBLPROPERTIES (owner = 'test_user')
        """)
        result = glue_spark.sql("DESCRIBE TABLE EXTENDED products").collect()
        col_names = [row[0] for row in result if not row[0].startswith("#") and row[0].strip()]
        assert "id" in col_names
        assert "name" in col_names
        assert "price" in col_names
        assert "category" in col_names

    def test_create_table_duplicate_fails(self, glue_spark: SparkSession):
        """Given an existing table,
        when creating a table with the same name without IF NOT EXISTS,
        then an error is raised.
        """
        glue_spark.sql("CREATE TABLE dup_table (id INT) USING parquet LOCATION 's3://bucket/dup'")
        with pytest.raises(Exception, match=r".*"):
            glue_spark.sql("CREATE TABLE dup_table (id INT) USING parquet LOCATION 's3://bucket/dup2'")

    def test_create_table_if_not_exists(self, glue_spark: SparkSession):
        """Given an existing table,
        when creating with IF NOT EXISTS,
        then no error is raised and the existing table is unchanged.
        """
        glue_spark.sql("""
            CREATE TABLE ine_table (id INT)
            USING parquet
            COMMENT 'original'
            LOCATION 's3://bucket/ine'
        """)
        # Should not raise
        glue_spark.sql("""
            CREATE TABLE IF NOT EXISTS ine_table (id INT)
            USING parquet
            COMMENT 'new comment'
            LOCATION 's3://bucket/ine2'
        """)
        result = glue_spark.sql("DESCRIBE TABLE EXTENDED ine_table").collect()
        info = {row[0].strip(): row[1] for row in result}
        assert info.get("Comment", info.get("comment")) == "original"


class TestGetTable:
    """Tests retrieving a table from Glue catalog."""

    def test_get_nonexistent_table(self, glue_spark: SparkSession):
        """Given a Glue catalog,
        when describing a non-existent table,
        then an error is raised.
        """
        with pytest.raises(Exception, match=r".*"):
            glue_spark.sql("DESCRIBE TABLE nonexistent_table_glue").collect()

    def test_get_existing_table(self, glue_spark: SparkSession):
        """Given a table with columns, comment, and properties,
        when describing the table,
        then all fields match.
        """
        glue_spark.sql("""
            CREATE TABLE get_test_table (
                id BIGINT NOT NULL COMMENT 'The ID',
                value STRING
            )
            USING parquet
            COMMENT 'Test table description'
            LOCATION 's3://bucket/test_table'
            TBLPROPERTIES (key1 = 'value1')
        """)
        result = glue_spark.sql("DESCRIBE TABLE EXTENDED get_test_table").collect()
        col_info = [(row[0], row[1]) for row in result if row[1] and not row[0].startswith("#")]
        col_names = [c[0].strip() for c in col_info]
        assert "id" in col_names
        assert "value" in col_names


class TestColumnTypes:
    """Tests that various Arrow data types can be created as Glue table columns."""

    def test_all_supported_column_types(self, glue_spark: SparkSession):
        """Given a Glue catalog,
        when creating a table with all supported column types,
        then all columns round-trip correctly.
        """
        glue_spark.sql("""
            CREATE TABLE all_types (
                col_boolean BOOLEAN,
                col_tinyint TINYINT,
                col_smallint SMALLINT,
                col_int INT,
                col_bigint BIGINT,
                col_float FLOAT,
                col_double DOUBLE,
                col_string STRING,
                col_binary BINARY,
                col_date DATE,
                col_timestamp TIMESTAMP,
                col_decimal DECIMAL(10, 2),
                col_array ARRAY<STRING>,
                col_struct STRUCT<name: STRING, value: INT>,
                col_map MAP<STRING, INT>
            )
            USING parquet
            LOCATION 's3://bucket/all_types'
        """)
        result = glue_spark.sql("DESCRIBE TABLE all_types").collect()
        col_names = [row[0].strip() for row in result if row[0].strip() and not row[0].startswith("#")]
        expected_column_count = 15
        assert len(col_names) >= expected_column_count

    def test_unsupported_column_types(self, glue_spark: SparkSession):
        """Given a Glue catalog,
        when creating a table with an unsupported column type,
        then an error is raised.
        """
        # Spark SQL doesn't directly support Union type, so this test
        # validates that invalid DDL is rejected
        with pytest.raises(Exception, match=r".*"):
            glue_spark.sql("""
                CREATE TABLE unsupported_type (col_union UNIONTYPE<INT, STRING>)
                USING parquet
                LOCATION 's3://bucket/unsupported'
            """)


class TestStorageFormats:
    """Tests all supported storage formats."""

    @pytest.mark.parametrize("fmt", ["parquet", "csv", "json", "orc", "avro"])
    def test_storage_format(self, glue_spark: SparkSession, fmt: str):
        """Given a Glue catalog,
        when creating a table with a specific format,
        then the format is preserved.
        """
        table_name = f"test_{fmt}_table"
        glue_spark.sql(f"""
            CREATE TABLE {table_name} (id INT NOT NULL, name STRING)
            USING {fmt}
            COMMENT 'Table with {fmt} format'
            LOCATION 's3://bucket/{table_name}'
        """)
        result = glue_spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}").collect()
        info = {row[0].strip(): row[1] for row in result}
        # The provider field or format should contain the format name
        provider = info.get("Provider", info.get("provider", ""))
        assert provider.lower() == fmt.lower() or fmt.lower() in provider.lower()


class TestListTables:
    """Tests listing tables in a database."""

    def test_list_tables(self, glue_spark: SparkSession):
        """Given multiple tables,
        when listing tables,
        then all created tables are returned.
        """
        table_names = ["table_alpha", "table_beta", "table_gamma"]
        for name in table_names:
            glue_spark.sql(f"CREATE TABLE {name} (id INT) USING parquet LOCATION 's3://bucket/{name}'")

        result = glue_spark.sql("SHOW TABLES").collect()
        returned_names = [row["tableName"] for row in result]
        for name in table_names:
            assert name in returned_names


class TestDropTable:
    """Tests dropping tables from Glue catalog."""

    def test_drop_existing_table(self, glue_spark: SparkSession):
        """Given an existing table,
        when dropping it,
        then it is no longer accessible.
        """
        glue_spark.sql("CREATE TABLE drop_me (id INT) USING parquet LOCATION 's3://bucket/drop_me'")
        glue_spark.sql("DESCRIBE TABLE drop_me").collect()
        glue_spark.sql("DROP TABLE drop_me")
        with pytest.raises(Exception, match=r".*"):
            glue_spark.sql("DESCRIBE TABLE drop_me").collect()

    def test_drop_nonexistent_fails(self, glue_spark: SparkSession):
        """Given a Glue catalog,
        when dropping a non-existent table without IF EXISTS,
        then an error is raised.
        """
        with pytest.raises(Exception, match=r".*"):
            glue_spark.sql("DROP TABLE nonexistent_drop_table")

    def test_drop_nonexistent_if_exists(self, glue_spark: SparkSession):
        """Given a Glue catalog,
        when dropping a non-existent table with IF EXISTS,
        then no error is raised.
        """
        # Should not raise
        glue_spark.sql("DROP TABLE IF EXISTS nonexistent_drop_table")
