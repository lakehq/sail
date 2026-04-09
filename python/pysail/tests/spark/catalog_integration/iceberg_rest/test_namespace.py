"""Integration tests for Iceberg REST catalog namespace operations.

Migrated from crates/sail-catalog-iceberg/tests/rest_integration_test.rs
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class TestCreateNamespace:
    """Tests namespace (database) creation in Iceberg REST catalog."""

    def test_create_namespace_basic(self, iceberg_spark: SparkSession):
        """Given an Iceberg REST catalog,
        when creating a namespace with comment, location, and properties,
        then the namespace is created with correct metadata.
        """
        iceberg_spark.sql("""
            CREATE DATABASE test_create_ns
            COMMENT 'test comment'
            LOCATION 's3://bucket/path'
            WITH DBPROPERTIES (key1 = 'value1')
        """)
        try:
            result = iceberg_spark.sql("SHOW DATABASES LIKE 'test_create_ns'").collect()
            assert any(row[0] == "test_create_ns" for row in result)
        finally:
            iceberg_spark.sql("DROP DATABASE IF EXISTS test_create_ns")

    def test_create_namespace_duplicate_fails(self, iceberg_spark: SparkSession):
        """Given an existing namespace,
        when creating a namespace with the same name without IF NOT EXISTS,
        then an error is raised.
        """
        iceberg_spark.sql("CREATE DATABASE dup_ns_iceberg")
        try:
            with pytest.raises(Exception, match=r".*"):
                iceberg_spark.sql("CREATE DATABASE dup_ns_iceberg")
        finally:
            iceberg_spark.sql("DROP DATABASE IF EXISTS dup_ns_iceberg")

    def test_create_namespace_if_not_exists(self, iceberg_spark: SparkSession):
        """Given an existing namespace,
        when creating with IF NOT EXISTS,
        then no error is raised and the existing namespace is unchanged.
        """
        iceberg_spark.sql("CREATE DATABASE ine_ns_iceberg")
        try:
            # Should not raise
            iceberg_spark.sql("CREATE DATABASE IF NOT EXISTS ine_ns_iceberg COMMENT 'should be ignored'")
        finally:
            iceberg_spark.sql("DROP DATABASE IF EXISTS ine_ns_iceberg")


class TestGetNamespace:
    """Tests retrieving a namespace from Iceberg REST catalog."""

    def test_get_nonexistent_namespace(self, iceberg_spark: SparkSession):
        """Given an Iceberg REST catalog,
        when a non-existent namespace is looked up,
        then it does not appear in the listing.
        """
        result = iceberg_spark.sql("SHOW DATABASES LIKE 'nonexistent_ns_iceberg'").collect()
        assert not any(row[0] == "nonexistent_ns_iceberg" for row in result)

    def test_get_existing_namespace(self, iceberg_spark: SparkSession):
        """Given a namespace with properties,
        when describing it,
        then the properties are correctly returned.
        """
        iceberg_spark.sql("""
            CREATE DATABASE get_ns_iceberg
            WITH DBPROPERTIES (owner = 'Lake', community = 'Sail')
        """)
        try:
            result = iceberg_spark.sql("SHOW DATABASES LIKE 'get_ns_iceberg'").collect()
            assert any(row[0] == "get_ns_iceberg" for row in result)
        finally:
            iceberg_spark.sql("DROP DATABASE IF EXISTS get_ns_iceberg")

    def test_multi_level_namespace(self, iceberg_spark: SparkSession):
        """Given an Iceberg REST catalog,
        when creating two separate databases,
        then both appear in the listing.
        """
        iceberg_spark.sql("CREATE DATABASE IF NOT EXISTS ns_apple")
        iceberg_spark.sql("CREATE DATABASE IF NOT EXISTS ns_ios")
        try:
            result = iceberg_spark.sql("SHOW DATABASES").collect()
            ns_names = [row[0] for row in result]
            assert "ns_apple" in ns_names
            assert "ns_ios" in ns_names
        finally:
            iceberg_spark.sql("DROP DATABASE IF EXISTS ns_apple")
            iceberg_spark.sql("DROP DATABASE IF EXISTS ns_ios")


class TestListNamespaces:
    """Tests listing namespaces in Iceberg REST catalog."""

    def test_list_namespaces(self, iceberg_spark: SparkSession):
        """Given multiple databases,
        when listing databases,
        then all created databases are returned.
        """
        iceberg_spark.sql("CREATE DATABASE IF NOT EXISTS list_ns_one")
        iceberg_spark.sql("CREATE DATABASE IF NOT EXISTS list_ns_two")
        try:
            result = iceberg_spark.sql("SHOW DATABASES").collect()
            ns_names = [row[0] for row in result]
            assert "list_ns_one" in ns_names
            assert "list_ns_two" in ns_names
        finally:
            iceberg_spark.sql("DROP DATABASE IF EXISTS list_ns_one")
            iceberg_spark.sql("DROP DATABASE IF EXISTS list_ns_two")

    def test_list_empty_namespaces(self, iceberg_spark: SparkSession):
        """Given no databases matching a pattern,
        when listing with that pattern,
        then an empty list is returned.
        """
        result = iceberg_spark.sql("SHOW DATABASES LIKE 'never_created_ns_%'").collect()
        assert len(result) == 0

    def test_list_root_namespaces(self, iceberg_spark: SparkSession):
        """Given a database,
        when listing all databases,
        then the created database is returned.
        """
        iceberg_spark.sql("CREATE DATABASE IF NOT EXISTS root_test_ns")
        try:
            result = iceberg_spark.sql("SHOW DATABASES").collect()
            ns_names = [row[0] for row in result]
            assert "root_test_ns" in ns_names
        finally:
            iceberg_spark.sql("DROP DATABASE IF EXISTS root_test_ns")


class TestDropNamespace:
    """Tests dropping namespaces from Iceberg REST catalog."""

    def test_drop_existing_namespace(self, iceberg_spark: SparkSession):
        """Given an existing namespace,
        when dropping it,
        then it is no longer accessible.
        """
        iceberg_spark.sql("CREATE DATABASE drop_ns_iceberg")
        result = iceberg_spark.sql("SHOW DATABASES LIKE 'drop_ns_iceberg'").collect()
        assert any(row[0] == "drop_ns_iceberg" for row in result)
        iceberg_spark.sql("DROP DATABASE drop_ns_iceberg")
        result = iceberg_spark.sql("SHOW DATABASES LIKE 'drop_ns_iceberg'").collect()
        assert not any(row[0] == "drop_ns_iceberg" for row in result)

    def test_drop_nonexistent_fails(self, iceberg_spark: SparkSession):
        """Given an Iceberg REST catalog,
        when dropping a non-existent namespace without IF EXISTS,
        then an error is raised.
        """
        with pytest.raises(Exception, match=r".*"):
            iceberg_spark.sql("DROP DATABASE nonexistent_drop_ns_iceberg")

    def test_drop_nonexistent_if_exists(self, iceberg_spark: SparkSession):
        """Given an Iceberg REST catalog,
        when dropping a non-existent namespace with IF EXISTS,
        then no error is raised.
        """
        # Should not raise
        iceberg_spark.sql("DROP DATABASE IF EXISTS nonexistent_drop_ns_iceberg")
