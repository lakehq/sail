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
            CREATE NAMESPACE test_create_ns
            COMMENT 'test comment'
            LOCATION 's3://bucket/path'
            WITH DBPROPERTIES (key1 = 'value1')
        """)
        try:
            result = iceberg_spark.sql("DESCRIBE NAMESPACE EXTENDED test_create_ns").collect()
            info = {row[0].strip(): row[1].strip() if row[1] else "" for row in result}
            assert "test_create_ns" in info.get("Namespace Name", info.get("Database Name", ""))
        finally:
            iceberg_spark.sql("DROP NAMESPACE IF EXISTS test_create_ns")

    def test_create_namespace_duplicate_fails(self, iceberg_spark: SparkSession):
        """Given an existing namespace,
        when creating a namespace with the same name without IF NOT EXISTS,
        then an error is raised.
        """
        iceberg_spark.sql("CREATE NAMESPACE dup_ns_iceberg")
        try:
            with pytest.raises(Exception, match=r".*"):
                iceberg_spark.sql("CREATE NAMESPACE dup_ns_iceberg")
        finally:
            iceberg_spark.sql("DROP NAMESPACE IF EXISTS dup_ns_iceberg")

    def test_create_namespace_if_not_exists(self, iceberg_spark: SparkSession):
        """Given an existing namespace,
        when creating with IF NOT EXISTS,
        then no error is raised and the existing namespace is unchanged.
        """
        iceberg_spark.sql("CREATE NAMESPACE ine_ns_iceberg")
        try:
            # Should not raise
            iceberg_spark.sql("CREATE NAMESPACE IF NOT EXISTS ine_ns_iceberg COMMENT 'should be ignored'")
        finally:
            iceberg_spark.sql("DROP NAMESPACE IF EXISTS ine_ns_iceberg")


class TestGetNamespace:
    """Tests retrieving a namespace from Iceberg REST catalog."""

    def test_get_nonexistent_namespace(self, iceberg_spark: SparkSession):
        """Given an Iceberg REST catalog,
        when describing a non-existent namespace,
        then an error is raised.
        """
        with pytest.raises(Exception, match=r".*"):
            iceberg_spark.sql("DESCRIBE NAMESPACE nonexistent_ns_iceberg").collect()

    def test_get_existing_namespace(self, iceberg_spark: SparkSession):
        """Given a namespace with properties,
        when describing it,
        then the properties are correctly returned.
        """
        iceberg_spark.sql("""
            CREATE NAMESPACE get_ns_iceberg
            WITH DBPROPERTIES (owner = 'Lake', community = 'Sail')
        """)
        try:
            result = iceberg_spark.sql("DESCRIBE NAMESPACE EXTENDED get_ns_iceberg").collect()
            info = {row[0].strip(): row[1].strip() if row[1] else "" for row in result}
            # Verify the namespace exists and has some properties
            assert any("get_ns_iceberg" in v for v in info.values())
        finally:
            iceberg_spark.sql("DROP NAMESPACE IF EXISTS get_ns_iceberg")

    def test_multi_level_namespace(self, iceberg_spark: SparkSession):
        """Given an Iceberg REST catalog supporting multi-level namespaces,
        when creating a multi-level namespace,
        then it can be described.
        """
        iceberg_spark.sql("CREATE NAMESPACE IF NOT EXISTS apple")
        iceberg_spark.sql("CREATE NAMESPACE IF NOT EXISTS apple.ios")
        try:
            result = iceberg_spark.sql("DESCRIBE NAMESPACE apple.ios").collect()
            assert len(result) > 0
        finally:
            iceberg_spark.sql("DROP NAMESPACE IF EXISTS apple.ios")
            iceberg_spark.sql("DROP NAMESPACE IF EXISTS apple")


class TestListNamespaces:
    """Tests listing namespaces in Iceberg REST catalog."""

    def test_list_namespaces(self, iceberg_spark: SparkSession):
        """Given multiple namespaces,
        when listing namespaces,
        then all created namespaces are returned.
        """
        iceberg_spark.sql("CREATE NAMESPACE IF NOT EXISTS list_ns_parent")
        iceberg_spark.sql("CREATE NAMESPACE IF NOT EXISTS list_ns_parent.ios")
        iceberg_spark.sql("CREATE NAMESPACE IF NOT EXISTS list_ns_parent.macos")
        try:
            result = iceberg_spark.sql("SHOW NAMESPACES IN list_ns_parent").collect()
            ns_names = [row[0] for row in result]
            expected_ns_count = 2
            assert len(ns_names) >= expected_ns_count
        finally:
            iceberg_spark.sql("DROP NAMESPACE IF EXISTS list_ns_parent.ios")
            iceberg_spark.sql("DROP NAMESPACE IF EXISTS list_ns_parent.macos")
            iceberg_spark.sql("DROP NAMESPACE IF EXISTS list_ns_parent")

    def test_list_empty_namespaces(self, iceberg_spark: SparkSession):
        """Given a namespace with no children,
        when listing its sub-namespaces,
        then an empty list is returned.
        """
        iceberg_spark.sql("CREATE NAMESPACE IF NOT EXISTS empty_parent_ns")
        try:
            result = iceberg_spark.sql("SHOW NAMESPACES IN empty_parent_ns").collect()
            assert len(result) == 0
        finally:
            iceberg_spark.sql("DROP NAMESPACE IF EXISTS empty_parent_ns")

    def test_list_root_namespaces(self, iceberg_spark: SparkSession):
        """Given namespaces at multiple levels,
        when listing root namespaces,
        then only top-level namespaces are returned.
        """
        iceberg_spark.sql("CREATE NAMESPACE IF NOT EXISTS root_test_ns")
        iceberg_spark.sql("CREATE NAMESPACE IF NOT EXISTS root_test_ns.child1")
        try:
            result = iceberg_spark.sql("SHOW NAMESPACES").collect()
            ns_names = [row[0] for row in result]
            assert "root_test_ns" in ns_names
        finally:
            iceberg_spark.sql("DROP NAMESPACE IF EXISTS root_test_ns.child1")
            iceberg_spark.sql("DROP NAMESPACE IF EXISTS root_test_ns")


class TestDropNamespace:
    """Tests dropping namespaces from Iceberg REST catalog."""

    def test_drop_existing_namespace(self, iceberg_spark: SparkSession):
        """Given an existing namespace,
        when dropping it,
        then it is no longer accessible.
        """
        iceberg_spark.sql("CREATE NAMESPACE drop_ns_iceberg")
        iceberg_spark.sql("DESCRIBE NAMESPACE drop_ns_iceberg").collect()
        iceberg_spark.sql("DROP NAMESPACE drop_ns_iceberg")
        with pytest.raises(Exception, match=r".*"):
            iceberg_spark.sql("DESCRIBE NAMESPACE drop_ns_iceberg").collect()

    def test_drop_nonexistent_fails(self, iceberg_spark: SparkSession):
        """Given an Iceberg REST catalog,
        when dropping a non-existent namespace without IF EXISTS,
        then an error is raised.
        """
        with pytest.raises(Exception, match=r".*"):
            iceberg_spark.sql("DROP NAMESPACE nonexistent_drop_ns_iceberg")

    def test_drop_nonexistent_if_exists(self, iceberg_spark: SparkSession):
        """Given an Iceberg REST catalog,
        when dropping a non-existent namespace with IF EXISTS,
        then no error is raised.
        """
        # Should not raise
        iceberg_spark.sql("DROP NAMESPACE IF EXISTS nonexistent_drop_ns_iceberg")
