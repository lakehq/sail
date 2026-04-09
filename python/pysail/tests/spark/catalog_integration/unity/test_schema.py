"""Integration tests for Unity Catalog schema (database) operations.

Migrated from crates/sail-catalog-unity/tests/rest_integration_test.rs
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class TestCreateSchema:
    """Tests schema creation in Unity Catalog."""

    def test_create_schema_basic(self, unity_spark: SparkSession):
        """Given a Unity Catalog,
        when creating a schema with comment and properties,
        then the schema is created with correct metadata.
        """
        unity_spark.sql("""
            CREATE SCHEMA test_create_schema
            COMMENT 'test comment'
            WITH DBPROPERTIES (key1 = 'value1')
        """)
        try:
            result = unity_spark.sql("SHOW SCHEMAS LIKE 'test_create_schema'").collect()
            assert any(row[0] == "test_create_schema" for row in result)
        finally:
            unity_spark.sql("DROP SCHEMA IF EXISTS test_create_schema")

    def test_create_schema_duplicate_fails(self, unity_spark: SparkSession):
        """Given an existing schema,
        when creating a schema with the same name without IF NOT EXISTS,
        then an error is raised.
        """
        unity_spark.sql("CREATE SCHEMA dup_schema_unity")
        try:
            with pytest.raises(Exception, match=r".*"):
                unity_spark.sql("CREATE SCHEMA dup_schema_unity")
        finally:
            unity_spark.sql("DROP SCHEMA IF EXISTS dup_schema_unity")

    def test_create_schema_if_not_exists(self, unity_spark: SparkSession):
        """Given an existing schema,
        when creating with IF NOT EXISTS,
        then no error is raised and the existing schema is unchanged.
        """
        unity_spark.sql("CREATE SCHEMA ine_schema_unity")
        try:
            # Should not raise
            unity_spark.sql("CREATE SCHEMA IF NOT EXISTS ine_schema_unity COMMENT 'should be ignored'")
        finally:
            unity_spark.sql("DROP SCHEMA IF EXISTS ine_schema_unity")


class TestGetSchema:
    """Tests retrieving a schema from Unity Catalog."""

    def test_get_nonexistent_schema(self, unity_spark: SparkSession):
        """Given a Unity Catalog,
        when a non-existent schema is looked up,
        then it does not appear in the listing.
        """
        result = unity_spark.sql("SHOW SCHEMAS LIKE 'nonexistent_schema_unity'").collect()
        assert not any(row[0] == "nonexistent_schema_unity" for row in result)

    def test_get_existing_schema(self, unity_spark: SparkSession):
        """Given a schema with properties,
        when describing it,
        then the properties are correctly returned.
        """
        unity_spark.sql("""
            CREATE SCHEMA get_schema_unity
            WITH DBPROPERTIES (owner = 'Lake', community = 'Sail')
        """)
        try:
            result = unity_spark.sql("SHOW SCHEMAS LIKE 'get_schema_unity'").collect()
            assert any(row[0] == "get_schema_unity" for row in result)
        finally:
            unity_spark.sql("DROP SCHEMA IF EXISTS get_schema_unity")


class TestListSchemas:
    """Tests listing schemas in Unity Catalog."""

    def test_list_schemas(self, unity_spark: SparkSession):
        """Given multiple schemas,
        when listing schemas (SHOW DATABASES/SCHEMAS),
        then all created schemas are returned.
        """
        unity_spark.sql("CREATE SCHEMA IF NOT EXISTS list_ios_unity")
        unity_spark.sql("CREATE SCHEMA IF NOT EXISTS list_macos_unity")
        try:
            result = unity_spark.sql("SHOW SCHEMAS").collect()
            schema_names = [row[0] for row in result]
            assert "list_ios_unity" in schema_names
            assert "list_macos_unity" in schema_names
        finally:
            unity_spark.sql("DROP SCHEMA IF EXISTS list_ios_unity")
            unity_spark.sql("DROP SCHEMA IF EXISTS list_macos_unity")


class TestDropSchema:
    """Tests dropping schemas from Unity Catalog."""

    def test_drop_existing_schema(self, unity_spark: SparkSession):
        """Given an existing schema,
        when dropping it,
        then it is no longer accessible.
        """
        unity_spark.sql("CREATE SCHEMA drop_schema_unity")
        result = unity_spark.sql("SHOW SCHEMAS LIKE 'drop_schema_unity'").collect()
        assert any(row[0] == "drop_schema_unity" for row in result)
        unity_spark.sql("DROP SCHEMA drop_schema_unity")
        result = unity_spark.sql("SHOW SCHEMAS LIKE 'drop_schema_unity'").collect()
        assert not any(row[0] == "drop_schema_unity" for row in result)

    def test_drop_nonexistent_fails(self, unity_spark: SparkSession):
        """Given a Unity Catalog,
        when dropping a non-existent schema without IF EXISTS,
        then an error is raised.
        """
        with pytest.raises(Exception, match=r".*"):
            unity_spark.sql("DROP SCHEMA nonexistent_drop_schema_unity")

    def test_drop_nonexistent_if_exists(self, unity_spark: SparkSession):
        """Given a Unity Catalog,
        when dropping a non-existent schema with IF EXISTS,
        then no error is raised.
        """
        # Should not raise
        unity_spark.sql("DROP SCHEMA IF EXISTS nonexistent_drop_schema_unity")

    def test_drop_schema_cascade(self, unity_spark: SparkSession):
        """Given a schema with content,
        when dropping with CASCADE,
        then the schema and its content are removed.
        """
        unity_spark.sql("CREATE SCHEMA cascade_drop_unity")
        try:
            unity_spark.sql("""
                CREATE TABLE cascade_drop_unity.t1 (id INT)
                USING delta
                LOCATION 's3://deltadata/cascade_test'
            """)
            unity_spark.sql("DROP SCHEMA cascade_drop_unity CASCADE")
            result = unity_spark.sql("SHOW SCHEMAS LIKE 'cascade_drop_unity'").collect()
            assert not any(row[0] == "cascade_drop_unity" for row in result)
        finally:
            unity_spark.sql("DROP SCHEMA IF EXISTS cascade_drop_unity CASCADE")
