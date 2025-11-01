"""
DuckLake read tests for Sail.

This module tests reading DuckLake tables through Sail's DataSource API.
DuckLake is an open Lakehouse format extension for DuckDB that stores
metadata in SQLite/PostgreSQL and data files in object storage.
"""

import os
import tempfile

import duckdb
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.types import Row

from ..utils import escape_sql_string_literal  # noqa: TID252


@pytest.fixture(scope="module")
def duckdb_conn():
    """Create a DuckDB connection with DuckLake extension loaded."""
    conn = duckdb.connect(":memory:")
    try:
        # Try to install DuckLake extension
        conn.execute("INSTALL ducklake")
        conn.execute("LOAD ducklake")
    except Exception as e:
        pytest.skip(f"DuckLake extension not available: {e}")
    return conn


@pytest.fixture
def ducklake_setup(tmp_path, duckdb_conn):
    """
    Set up a DuckLake database with test data.
    
    Returns a tuple of (metadata_path, data_path, table_name).
    """
    metadata_path = tmp_path / "metadata.ducklake"
    data_path = tmp_path / "data"
    data_path.mkdir(exist_ok=True)
    
    # Attach DuckLake database with SQLite backend
    # Format: ducklake:sqlite:/path/to/metadata.db
    duckdb_conn.execute(f"""
        ATTACH 'ducklake:sqlite:{metadata_path}' AS my_ducklake 
        (DATA_PATH '{data_path}/')
    """)
    
    # Create a test table
    table_name = "my_ducklake.test_table"
    duckdb_conn.execute(f"""
        CREATE TABLE {table_name} (
            id INTEGER,
            name VARCHAR,
            score DOUBLE
        )
    """)
    
    # Insert test data
    duckdb_conn.execute(f"""
        INSERT INTO {table_name} 
        VALUES 
            (1, 'Alice', 95.5),
            (2, 'Bob', 87.3),
            (3, 'Charlie', 92.1)
    """)
    
    # IMPORTANT: Detach the database to release the SQLite lock
    # This allows Sail's Diesel connection to access the database
    duckdb_conn.execute("DETACH my_ducklake")
    
    yield str(metadata_path), str(data_path), table_name
    
    # Cleanup - database is already detached


class TestDuckLakeRead:
    """DuckLake read operation tests."""
    
    def test_ducklake_basic_read(self, spark, ducklake_setup):
        """Test basic DuckLake table read."""
        metadata_path, data_path, table_name = ducklake_setup
        
        # Read using Sail's DataSource API
        df = spark.read.format("ducklake").options(**{
            "url": f"sqlite:///{metadata_path}",
            "table": "test_table",
            "base_path": f"file://{data_path}/"
        }).load()
        
        result = df.sort("id").collect()
        
        assert len(result) == 3
        assert result[0].id == 1
        assert result[0].name == "Alice"
        assert abs(result[0].score - 95.5) < 0.01
        assert result[1].id == 2
        assert result[1].name == "Bob"
        assert abs(result[1].score - 87.3) < 0.01
        assert result[2].id == 3
        assert result[2].name == "Charlie"
        assert abs(result[2].score - 92.1) < 0.01
    
    def test_ducklake_read_with_filter(self, spark, ducklake_setup):
        """Test DuckLake read with filter pushdown."""
        metadata_path, data_path, table_name = ducklake_setup
        
        df = spark.read.format("ducklake").options(**{
            "url": f"sqlite:///{metadata_path}",
            "table": "test_table",
            "base_path": f"file://{data_path}/"
        }).load()
        
        # Filter for scores > 90
        filtered_df = df.filter("score > 90").sort("id")
        result = filtered_df.collect()
        
        assert len(result) == 2
        assert result[0].name == "Alice"
        assert result[1].name == "Charlie"
    
    def test_ducklake_read_with_projection(self, spark, ducklake_setup):
        """Test DuckLake read with column projection."""
        metadata_path, data_path, table_name = ducklake_setup
        
        df = spark.read.format("ducklake").options(**{
            "url": f"sqlite:///{metadata_path}",
            "table": "test_table",
            "base_path": f"file://{data_path}/"
        }).load()
        
        # Select only id and name columns
        projected_df = df.select("id", "name").sort("id")
        result = projected_df.collect()
        
        assert len(result) == 3
        assert hasattr(result[0], "id")
        assert hasattr(result[0], "name")
        assert not hasattr(result[0], "score")
    
    def test_ducklake_read_with_limit(self, spark, ducklake_setup):
        """Test DuckLake read with limit."""
        metadata_path, data_path, table_name = ducklake_setup
        
        df = spark.read.format("ducklake").options(**{
            "url": f"sqlite:///{metadata_path}",
            "table": "test_table",
            "base_path": f"file://{data_path}/"
        }).load()
        
        limited_df = df.limit(2)
        result = limited_df.collect()
        
        assert len(result) == 2
    
    def test_ducklake_read_to_pandas(self, spark, ducklake_setup):
        """Test DuckLake read and conversion to pandas."""
        metadata_path, data_path, table_name = ducklake_setup
        
        df = spark.read.format("ducklake").options(**{
            "url": f"sqlite:///{metadata_path}",
            "table": "test_table",
            "base_path": f"file://{data_path}/"
        }).load()
        
        pandas_df = df.sort("id").toPandas()
        
        expected_df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [95.5, 87.3, 92.1]
        }).astype({"id": "int32", "name": "string", "score": "float64"})
        
        assert_frame_equal(
            pandas_df.reset_index(drop=True),
            expected_df,
            check_dtype=False
        )
    
    def test_ducklake_read_with_sql(self, spark, ducklake_setup):
        """Test DuckLake read using SQL CREATE TABLE."""
        metadata_path, data_path, table_name = ducklake_setup
        
        # Create a table using SQL
        spark.sql(f"""
            CREATE TABLE ducklake_test 
            USING ducklake 
            OPTIONS (
                url 'sqlite:///{metadata_path}',
                `table` 'test_table',
                base_path 'file://{data_path}/'
            )
        """)
        
        try:
            result_df = spark.sql("SELECT * FROM ducklake_test").sort("id")
            result = result_df.collect()
            
            assert len(result) == 3
            assert result[0].name == "Alice"
            assert result[1].name == "Bob"
            assert result[2].name == "Charlie"
        finally:
            spark.sql("DROP TABLE IF EXISTS ducklake_test")
    
    def test_ducklake_read_with_schema_prefix(self, spark, tmp_path, duckdb_conn):
        """Test DuckLake read with schema.table format."""
        metadata_path = tmp_path / "metadata_schema.ducklake"
        data_path = tmp_path / "data_schema"
        data_path.mkdir(exist_ok=True)
        
        # Attach DuckLake database with SQLite backend
        duckdb_conn.execute(f"""
            ATTACH 'ducklake:sqlite:{metadata_path}' AS my_db 
            (DATA_PATH '{data_path}/')
        """)
        
        # Create a table in a specific schema
        duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS my_db.analytics")
        duckdb_conn.execute("""
            CREATE TABLE my_db.analytics.metrics (
                metric_id INTEGER,
                metric_name VARCHAR,
                value DOUBLE
            )
        """)
        
        duckdb_conn.execute("""
            INSERT INTO my_db.analytics.metrics 
            VALUES (1, 'cpu_usage', 75.5), (2, 'memory_usage', 82.3)
        """)
        
        # Detach to release the lock
        duckdb_conn.execute("DETACH my_db")
        
        # Read with schema.table format
        df = spark.read.format("ducklake").options(**{
            "url": f"sqlite:///{metadata_path}",
            "table": "analytics.metrics",
            "base_path": f"file://{data_path}/"
        }).load()
        
        result = df.sort("metric_id").collect()
        
        assert len(result) == 2
        assert result[0].metric_name == "cpu_usage"
        assert result[1].metric_name == "memory_usage"
    
    def test_ducklake_read_empty_table(self, spark, tmp_path, duckdb_conn):
        """Test reading an empty DuckLake table."""
        metadata_path = tmp_path / "metadata_empty.ducklake"
        data_path = tmp_path / "data_empty"
        data_path.mkdir(exist_ok=True)
        
        duckdb_conn.execute(f"""
            ATTACH 'ducklake:sqlite:{metadata_path}' AS empty_db 
            (DATA_PATH '{data_path}/')
        """)
        
        duckdb_conn.execute("""
            CREATE TABLE empty_db.empty_table (
                id INTEGER,
                value VARCHAR
            )
        """)
        
        # Detach to release the lock
        duckdb_conn.execute("DETACH empty_db")
        
        df = spark.read.format("ducklake").options(**{
            "url": f"sqlite:///{metadata_path}",
            "table": "empty_table",
            "base_path": f"file://{data_path}/"
        }).load()
        
        result = df.collect()
        assert len(result) == 0
    
    def test_ducklake_read_various_types(self, spark, tmp_path, duckdb_conn):
        """Test DuckLake read with various data types."""
        from datetime import date
        
        metadata_path = tmp_path / "metadata_types.ducklake"
        data_path = tmp_path / "data_types"
        data_path.mkdir(exist_ok=True)
        
        duckdb_conn.execute(f"""
            ATTACH 'ducklake:sqlite:{metadata_path}' AS types_db 
            (DATA_PATH '{data_path}/')
        """)
        
        duckdb_conn.execute("""
            CREATE TABLE types_db.type_test (
                int_col INTEGER,
                bigint_col BIGINT,
                double_col DOUBLE,
                varchar_col VARCHAR,
                bool_col BOOLEAN,
                date_col DATE
            )
        """)
        
        duckdb_conn.execute("""
            INSERT INTO types_db.type_test 
            VALUES (
                42,
                9223372036854775807,
                3.14159,
                'test string',
                true,
                '2024-01-15'
            )
        """)
        
        # Detach to release the lock
        duckdb_conn.execute("DETACH types_db")
        
        df = spark.read.format("ducklake").options(**{
            "url": f"sqlite:///{metadata_path}",
            "table": "type_test",
            "base_path": f"file://{data_path}/"
        }).load()
        
        result = df.collect()
        
        assert len(result) == 1
        row = result[0]
        assert row.int_col == 42
        assert row.bigint_col == 9223372036854775807
        assert abs(row.double_col - 3.14159) < 0.00001
        assert row.varchar_col == "test string"
        assert row.bool_col is True
    
    def test_ducklake_read_missing_table_error(self, spark, tmp_path, duckdb_conn):
        """Test error handling when reading non-existent table."""
        metadata_path = tmp_path / "metadata_missing.ducklake"
        data_path = tmp_path / "data_missing"
        data_path.mkdir(exist_ok=True)
        
        # Create empty DuckLake database using the shared connection
        duckdb_conn.execute(f"""
            ATTACH 'ducklake:sqlite:{metadata_path}' AS missing_db 
            (DATA_PATH '{data_path}/')
        """)
        
        try:
            # Detach to ensure metadata is persisted
            duckdb_conn.execute("DETACH missing_db")
            
            # Now try to read a non-existent table
            with pytest.raises(Exception):
                df = spark.read.format("ducklake").options(**{
                    "url": f"sqlite:///{metadata_path}",
                    "table": "nonexistent_table",
                    "base_path": f"file://{data_path}/"
                }).load()
                df.collect()
        finally:
            # Clean up if still attached
            try:
                duckdb_conn.execute("DETACH IF EXISTS missing_db")
            except:
                pass
    
    def test_ducklake_read_missing_options_error(self, spark):
        """Test error handling when required options are missing."""
        # Missing url option
        with pytest.raises(Exception):
            df = spark.read.format("ducklake").options(**{
                "table": "test_table",
                "base_path": "file:///tmp/data/"
            }).load()
            df.collect()
        
        # Missing table option
        with pytest.raises(Exception):
            df = spark.read.format("ducklake").options(**{
                "url": "sqlite:///tmp/metadata.db",
                "base_path": "file:///tmp/data/"
            }).load()
            df.collect()
        
        # Missing base_path option
        with pytest.raises(Exception):
            df = spark.read.format("ducklake").options(**{
                "url": "sqlite:///tmp/metadata.db",
                "table": "test_table"
            }).load()
            df.collect()


class TestDuckLakeReadAdvanced:
    """Advanced DuckLake read tests."""
    
    def test_ducklake_read_with_aggregation(self, spark, ducklake_setup):
        """Test DuckLake read with aggregation."""
        metadata_path, data_path, table_name = ducklake_setup
        
        df = spark.read.format("ducklake").options(**{
            "url": f"sqlite:///{metadata_path}",
            "table": "test_table",
            "base_path": f"file://{data_path}/"
        }).load()
        
        # Compute average score
        avg_score = df.agg({"score": "avg"}).collect()[0][0]
        
        expected_avg = (95.5 + 87.3 + 92.1) / 3
        assert abs(avg_score - expected_avg) < 0.01
    
    def test_ducklake_read_with_join(self, spark, ducklake_setup, tmp_path, duckdb_conn):
        """Test DuckLake read with join operations."""
        metadata_path, data_path, table_name = ducklake_setup
        
        # Re-attach the database to create a second table
        duckdb_conn.execute(f"""
            ATTACH 'ducklake:sqlite:{metadata_path}' AS my_ducklake 
            (DATA_PATH '{data_path}/')
        """)
        
        # Create a second table for join
        duckdb_conn.execute("""
            CREATE TABLE my_ducklake.departments (
                id INTEGER,
                dept_name VARCHAR
            )
        """)
        
        duckdb_conn.execute("""
            INSERT INTO my_ducklake.departments 
            VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'Marketing')
        """)
        
        # Detach to release the lock
        duckdb_conn.execute("DETACH my_ducklake")
        
        df1 = spark.read.format("ducklake").options(**{
            "url": f"sqlite:///{metadata_path}",
            "table": "test_table",
            "base_path": f"file://{data_path}/"
        }).load()
        
        df2 = spark.read.format("ducklake").options(**{
            "url": f"sqlite:///{metadata_path}",
            "table": "departments",
            "base_path": f"file://{data_path}/"
        }).load()
        
        joined_df = df1.join(df2, df1.id == df2.id, "inner").select(
            df1.name, df2.dept_name
        ).sort(df1.name)
        
        result = joined_df.collect()
        
        assert len(result) == 3
        assert result[0].name == "Alice"
        assert result[0].dept_name == "Engineering"
    
    def test_ducklake_read_count(self, spark, ducklake_setup):
        """Test DuckLake read with count operation."""
        metadata_path, data_path, table_name = ducklake_setup
        
        df = spark.read.format("ducklake").options(**{
            "url": f"sqlite:///{metadata_path}",
            "table": "test_table",
            "base_path": f"file://{data_path}/"
        }).load()
        
        count = df.count()
        assert count == 3

