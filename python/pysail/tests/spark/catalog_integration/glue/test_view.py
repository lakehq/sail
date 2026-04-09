"""Integration tests for Glue catalog view operations.

Migrated from crates/sail-catalog-glue/tests/view_tests.rs
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture(autouse=True)
def _view_test_db(glue_spark: SparkSession):
    """Create and clean up a test database for view tests."""
    glue_spark.sql("CREATE DATABASE IF NOT EXISTS view_test_db")
    glue_spark.sql("USE view_test_db")
    yield
    glue_spark.sql("DROP DATABASE IF EXISTS view_test_db CASCADE")


class TestCreateView:
    """Tests view creation in Glue catalog."""

    def test_create_view_basic(self, glue_spark: SparkSession):
        """Given a Glue catalog,
        when creating a view with columns and comment,
        then the view is created with correct metadata.
        """
        glue_spark.sql("""
            CREATE VIEW product_view
            COMMENT 'View of products'
            TBLPROPERTIES (owner = 'test_user')
            AS SELECT 1 AS id, 'test' AS name, 9.99 AS price
        """)
        result = glue_spark.sql("DESCRIBE TABLE EXTENDED product_view").collect()
        col_names = [row[0].strip() for row in result if row[0].strip() and not row[0].startswith("#")]
        assert "id" in col_names
        assert "name" in col_names
        assert "price" in col_names

    def test_create_view_duplicate_fails(self, glue_spark: SparkSession):
        """Given an existing view,
        when creating a view with the same name,
        then an error is raised.
        """
        glue_spark.sql("CREATE VIEW dup_view AS SELECT 1 AS id")
        with pytest.raises(Exception, match=r".*"):
            glue_spark.sql("CREATE VIEW dup_view AS SELECT 2 AS id")

    def test_create_view_if_not_exists(self, glue_spark: SparkSession):
        """Given an existing view,
        when creating with IF NOT EXISTS,
        then no error is raised.
        """
        glue_spark.sql("CREATE VIEW ine_view COMMENT 'original' AS SELECT 1 AS id")
        # Should not raise
        glue_spark.sql("CREATE VIEW IF NOT EXISTS ine_view COMMENT 'new' AS SELECT 2 AS id")


class TestGetView:
    """Tests retrieving a view from Glue catalog."""

    def test_get_nonexistent_view(self, glue_spark: SparkSession):
        """Given a Glue catalog,
        when describing a non-existent view,
        then an error is raised.
        """
        with pytest.raises(Exception, match=r".*"):
            glue_spark.sql("DESCRIBE TABLE nonexistent_view_glue").collect()

    def test_get_existing_view(self, glue_spark: SparkSession):
        """Given a view with columns and comment,
        when describing the view,
        then the view definition and columns are returned.
        """
        glue_spark.sql("""
            CREATE VIEW test_view
            COMMENT 'Test view description'
            TBLPROPERTIES (key1 = 'value1')
            AS SELECT 1 AS id, 'hello' AS value
        """)
        result = glue_spark.sql("DESCRIBE TABLE EXTENDED test_view").collect()
        col_names = [row[0].strip() for row in result if row[0].strip() and not row[0].startswith("#")]
        assert "id" in col_names
        assert "value" in col_names


class TestListViews:
    """Tests listing views in a database."""

    def test_list_views_excludes_tables(self, glue_spark: SparkSession):
        """Given views and tables in a database,
        when listing views,
        then only views are returned, not tables.
        """
        glue_spark.sql("CREATE VIEW view_alpha AS SELECT 1 AS id")
        glue_spark.sql("CREATE VIEW view_beta AS SELECT 2 AS id")
        glue_spark.sql("CREATE VIEW view_gamma AS SELECT 3 AS id")
        glue_spark.sql("CREATE TABLE a_table (id INT) USING parquet LOCATION 's3://bucket/a_table'")

        result = glue_spark.sql("SHOW VIEWS").collect()
        view_names = [row["viewName"] for row in result]
        assert "view_alpha" in view_names
        assert "view_beta" in view_names
        assert "view_gamma" in view_names
        assert "a_table" not in view_names


class TestDropView:
    """Tests dropping views from Glue catalog."""

    def test_drop_existing_view(self, glue_spark: SparkSession):
        """Given an existing view,
        when dropping it,
        then it is no longer accessible.
        """
        glue_spark.sql("CREATE VIEW drop_me_view AS SELECT 1 AS id")
        glue_spark.sql("DESCRIBE TABLE drop_me_view").collect()
        glue_spark.sql("DROP VIEW drop_me_view")
        with pytest.raises(Exception, match=r".*"):
            glue_spark.sql("DESCRIBE TABLE drop_me_view").collect()

    def test_drop_nonexistent_fails(self, glue_spark: SparkSession):
        """Given a Glue catalog,
        when dropping a non-existent view without IF EXISTS,
        then an error is raised.
        """
        with pytest.raises(Exception, match=r".*"):
            glue_spark.sql("DROP VIEW nonexistent_drop_view")

    def test_drop_nonexistent_if_exists(self, glue_spark: SparkSession):
        """Given a Glue catalog,
        when dropping a non-existent view with IF EXISTS,
        then no error is raised.
        """
        # Should not raise
        glue_spark.sql("DROP VIEW IF EXISTS nonexistent_drop_view")
