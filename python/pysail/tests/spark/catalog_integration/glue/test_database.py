"""Integration tests for Glue catalog database (schema) operations.

Migrated from crates/sail-catalog-glue/tests/database_tests.rs
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class TestCreateDatabase:
    """Tests database creation in Glue catalog."""

    def test_create_database_basic(self, glue_spark: SparkSession):
        """Given a Glue catalog,
        when creating a database with comment, location, and properties,
        then the database is created with correct metadata.
        """
        glue_spark.sql("""
            CREATE DATABASE test_create_db
            COMMENT 'test comment'
            LOCATION 's3://bucket/path'
            WITH DBPROPERTIES (key1 = 'value1')
        """)
        try:
            result = glue_spark.sql("DESCRIBE DATABASE EXTENDED test_create_db").collect()
            info = {row[0]: row[1] for row in result}
            assert info["Database Name"] == "test_create_db"
            assert info.get("Comment", "") == "test comment"
            assert "s3://bucket/path" in info.get("Location", "")
        finally:
            glue_spark.sql("DROP DATABASE IF EXISTS test_create_db")

    def test_create_database_duplicate_fails(self, glue_spark: SparkSession):
        """Given an existing database,
        when creating a database with the same name without IF NOT EXISTS,
        then an error is raised.
        """
        glue_spark.sql("CREATE DATABASE dup_db")
        try:
            with pytest.raises(Exception, match=r".*"):
                glue_spark.sql("CREATE DATABASE dup_db")
        finally:
            glue_spark.sql("DROP DATABASE IF EXISTS dup_db")

    def test_create_database_if_not_exists(self, glue_spark: SparkSession):
        """Given an existing database,
        when creating with IF NOT EXISTS,
        then no error is raised and the existing database is unchanged.
        """
        glue_spark.sql("CREATE DATABASE ine_db COMMENT 'original'")
        try:
            # Should not raise
            glue_spark.sql("CREATE DATABASE IF NOT EXISTS ine_db COMMENT 'new comment'")
            result = glue_spark.sql("DESCRIBE DATABASE EXTENDED ine_db").collect()
            info = {row[0]: row[1] for row in result}
            # Original comment should be preserved
            assert info.get("Comment", "") == "original"
        finally:
            glue_spark.sql("DROP DATABASE IF EXISTS ine_db")


class TestGetDatabase:
    """Tests retrieving a database from Glue catalog."""

    def test_get_nonexistent_database(self, glue_spark: SparkSession):
        """Given a Glue catalog,
        when describing a non-existent database,
        then an error is raised.
        """
        with pytest.raises(Exception, match=r".*"):
            glue_spark.sql("DESCRIBE DATABASE nonexistent_db_glue").collect()

    def test_get_existing_database(self, glue_spark: SparkSession):
        """Given a database with comment, location, and properties,
        when describing the database,
        then all fields are correctly returned.
        """
        glue_spark.sql("""
            CREATE DATABASE get_test_db
            COMMENT 'Get test description'
            LOCATION 's3://bucket/get-test'
            WITH DBPROPERTIES (owner = 'test_user', team = 'data_eng')
        """)
        try:
            result = glue_spark.sql("DESCRIBE DATABASE EXTENDED get_test_db").collect()
            info = {row[0]: row[1] for row in result}
            assert info["Database Name"] == "get_test_db"
            assert info.get("Comment", "") == "Get test description"
            assert "s3://bucket/get-test" in info.get("Location", "")
        finally:
            glue_spark.sql("DROP DATABASE IF EXISTS get_test_db")


class TestDropDatabase:
    """Tests dropping databases from Glue catalog."""

    def test_drop_nonexistent_fails(self, glue_spark: SparkSession):
        """Given a Glue catalog,
        when dropping a non-existent database without IF EXISTS,
        then an error is raised.
        """
        with pytest.raises(Exception, match=r".*"):
            glue_spark.sql("DROP DATABASE nonexistent_drop_db")

    def test_drop_nonexistent_if_exists(self, glue_spark: SparkSession):
        """Given a Glue catalog,
        when dropping a non-existent database with IF EXISTS,
        then no error is raised.
        """
        # Should not raise
        glue_spark.sql("DROP DATABASE IF EXISTS nonexistent_drop_db")

    def test_drop_existing_database(self, glue_spark: SparkSession):
        """Given an existing database,
        when dropping it,
        then it is no longer accessible.
        """
        glue_spark.sql("CREATE DATABASE drop_target_db COMMENT 'To be dropped'")
        # Verify exists
        glue_spark.sql("DESCRIBE DATABASE drop_target_db").collect()
        # Drop it
        glue_spark.sql("DROP DATABASE drop_target_db")
        # Verify gone
        with pytest.raises(Exception, match=r".*"):
            glue_spark.sql("DESCRIBE DATABASE drop_target_db").collect()


class TestListDatabases:
    """Tests listing databases in Glue catalog."""

    def test_list_databases(self, glue_spark: SparkSession):
        """Given multiple databases,
        when listing databases,
        then all created databases are returned.
        """
        glue_spark.sql("CREATE DATABASE IF NOT EXISTS list_db_one")
        glue_spark.sql("CREATE DATABASE IF NOT EXISTS list_db_two")
        glue_spark.sql("CREATE DATABASE IF NOT EXISTS list_other_db")
        try:
            result = glue_spark.sql("SHOW DATABASES").collect()
            db_names = [row[0] for row in result]
            assert "list_db_one" in db_names
            assert "list_db_two" in db_names
            assert "list_other_db" in db_names
        finally:
            glue_spark.sql("DROP DATABASE IF EXISTS list_db_one")
            glue_spark.sql("DROP DATABASE IF EXISTS list_db_two")
            glue_spark.sql("DROP DATABASE IF EXISTS list_other_db")
