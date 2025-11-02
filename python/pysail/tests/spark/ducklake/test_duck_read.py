"""
DuckLake read tests for Sail.

This module tests reading DuckLake tables through Sail's DataSource API.
DuckLake is an open Lakehouse format extension for DuckDB that stores
metadata in SQLite/PostgreSQL and data files in object storage.
"""

import duckdb
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.errors.exceptions.connect import AnalysisException

from pysail.tests.spark.utils import escape_sql_string_literal


@pytest.fixture(scope="module")
def duckdb_conn():
    """Create a DuckDB connection with DuckLake extension loaded."""
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("INSTALL ducklake")
        conn.execute("LOAD ducklake")
    except (RuntimeError, duckdb.Error) as e:
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

    table_identifier = table_name
    duckdb_conn.execute(f"""
        INSERT INTO {table_identifier}
        VALUES
            (1, 'Alice', 95.5),
            (2, 'Bob', 87.3),
            (3, 'Charlie', 92.1)
    """)

    # IMPORTANT: Detach the database to release the SQLite lock
    # This allows Sail's Diesel connection to access the database
    duckdb_conn.execute("DETACH my_ducklake")

    return str(metadata_path), str(data_path), table_name


class TestDuckLakeRead:
    """DuckLake read operation tests."""

    def test_ducklake_basic_read(self, spark, ducklake_setup):
        """Test basic DuckLake table read."""
        metadata_path, data_path, table_name = ducklake_setup

        # Read using Sail's DataSource API
        df = (
            spark.read.format("ducklake")
            .options(url=f"sqlite:///{metadata_path}", table="test_table", base_path=f"file://{data_path}/")
            .load()
        )

        result = df.sort("id").collect()

        expected_count = 3
        score_tolerance = 0.01
        expected_id_alice = 1
        expected_id_bob = 2
        expected_id_charlie = 3

        assert len(result) == expected_count
        assert result[0].id == expected_id_alice
        assert result[0].name == "Alice"
        assert abs(result[0].score - 95.5) < score_tolerance
        assert result[1].id == expected_id_bob
        assert result[1].name == "Bob"
        assert abs(result[1].score - 87.3) < score_tolerance
        assert result[2].id == expected_id_charlie
        assert result[2].name == "Charlie"
        assert abs(result[2].score - 92.1) < score_tolerance

    def test_ducklake_read_with_filter(self, spark, ducklake_setup):
        """Test DuckLake read with filter pushdown."""
        metadata_path, data_path, table_name = ducklake_setup

        df = (
            spark.read.format("ducklake")
            .options(url=f"sqlite:///{metadata_path}", table="test_table", base_path=f"file://{data_path}/")
            .load()
        )

        filtered_df = df.filter("score > 90").sort("id")
        result = filtered_df.collect()

        expected_filtered_count = 2
        assert len(result) == expected_filtered_count
        assert result[0].name == "Alice"
        assert result[1].name == "Charlie"

    def test_ducklake_read_with_projection(self, spark, ducklake_setup):
        """Test DuckLake read with column projection."""
        metadata_path, data_path, table_name = ducklake_setup

        df = (
            spark.read.format("ducklake")
            .options(url=f"sqlite:///{metadata_path}", table="test_table", base_path=f"file://{data_path}/")
            .load()
        )

        projected_df = df.select("id", "name").sort("id")
        result = projected_df.collect()

        expected_count = 3
        assert len(result) == expected_count
        assert hasattr(result[0], "id")
        assert hasattr(result[0], "name")
        assert not hasattr(result[0], "score")

    def test_ducklake_read_with_limit(self, spark, ducklake_setup):
        """Test DuckLake read with limit."""
        metadata_path, data_path, table_name = ducklake_setup

        df = (
            spark.read.format("ducklake")
            .options(url=f"sqlite:///{metadata_path}", table="test_table", base_path=f"file://{data_path}/")
            .load()
        )

        limit_count = 2
        limited_df = df.limit(limit_count)
        result = limited_df.collect()

        assert len(result) == limit_count

    def test_ducklake_read_to_pandas(self, spark, ducklake_setup):
        """Test DuckLake read and conversion to pandas."""
        metadata_path, data_path, table_name = ducklake_setup

        df = (
            spark.read.format("ducklake")
            .options(url=f"sqlite:///{metadata_path}", table="test_table", base_path=f"file://{data_path}/")
            .load()
        )

        pandas_df = df.sort("id").toPandas()

        expected_df = pd.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "score": [95.5, 87.3, 92.1]}
        ).astype({"id": "int32", "name": "string", "score": "float64"})

        assert_frame_equal(pandas_df.reset_index(drop=True), expected_df, check_dtype=False)

    def test_ducklake_read_with_sql(self, spark, ducklake_setup):
        """Test DuckLake read using SQL CREATE TABLE."""
        metadata_path, data_path, table_name = ducklake_setup

        spark.sql(
            f"""
            CREATE TABLE ducklake_test
            USING ducklake
            OPTIONS (
                url 'sqlite:///{metadata_path}',
                `table` 'test_table',
                base_path 'file://{data_path}/'
            )
        """
        )

        try:
            result_df = spark.sql("SELECT * FROM ducklake_test").sort("id")
            result = result_df.collect()

            expected_count = 3
            assert len(result) == expected_count
            assert result[0].name == "Alice"
            assert result[1].name == "Bob"
            assert result[2].name == "Charlie"
        finally:
            spark.sql("DROP TABLE IF EXISTS ducklake_test")

    def test_ducklake_read_with_sql_location(self, spark, ducklake_setup):
        """Test DuckLake read using SQL LOCATION (location-first)."""
        metadata_path, data_path, table_name = ducklake_setup

        # Use table-only path for location; metadata often stores default schema as 'main'
        tbl_only = table_name.split(".")[-1]

        loc = f"ducklake+sqlite:///{metadata_path}/{tbl_only}?base_path=file://{data_path}/"

        spark.sql(
            f"""
            CREATE TABLE ducklake_test_loc
            USING ducklake
            LOCATION '{escape_sql_string_literal(loc)}'
        """
        )

        try:
            result_df = spark.sql("SELECT * FROM ducklake_test_loc").sort("id")
            result = result_df.collect()

            expected_count = 3
            assert len(result) == expected_count
            assert result[0].name == "Alice"
            assert result[1].name == "Bob"
            assert result[2].name == "Charlie"
        finally:
            spark.sql("DROP TABLE IF EXISTS ducklake_test_loc")

    def test_ducklake_read_with_schema_prefix(self, spark, tmp_path, duckdb_conn):
        """Test DuckLake read with schema.table format."""
        metadata_path = tmp_path / "metadata_schema.ducklake"
        data_path = tmp_path / "data_schema"
        data_path.mkdir(exist_ok=True)

        duckdb_conn.execute(f"""
            ATTACH 'ducklake:sqlite:{metadata_path}' AS my_db
            (DATA_PATH '{data_path}/')
        """)

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

        duckdb_conn.execute("DETACH my_db")

        df = (
            spark.read.format("ducklake")
            .options(
                url=f"sqlite:///{metadata_path}",
                table="analytics.metrics",
                base_path=f"file://{data_path}/",
            )
            .load()
        )

        result = df.sort("metric_id").collect()

        expected_count = 2
        assert len(result) == expected_count
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

        duckdb_conn.execute("DETACH empty_db")

        df = (
            spark.read.format("ducklake")
            .options(url=f"sqlite:///{metadata_path}", table="empty_table", base_path=f"file://{data_path}/")
            .load()
        )

        result = df.collect()
        assert len(result) == 0

    def test_ducklake_read_various_types(self, spark, tmp_path, duckdb_conn):
        """Test DuckLake read with various data types."""

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

        duckdb_conn.execute("DETACH types_db")

        df = (
            spark.read.format("ducklake")
            .options(url=f"sqlite:///{metadata_path}", table="type_test", base_path=f"file://{data_path}/")
            .load()
        )

        result = df.collect()

        expected_row_count = 1
        expected_int_val = 42
        expected_bigint_val = 9223372036854775807
        double_tolerance = 0.00001

        assert len(result) == expected_row_count
        row = result[0]
        assert row.int_col == expected_int_val
        assert row.bigint_col == expected_bigint_val
        assert abs(row.double_col - 3.14159) < double_tolerance
        assert row.varchar_col == "test string"
        assert row.bool_col is True

    def test_ducklake_read_missing_table_error(self, spark, tmp_path, duckdb_conn):
        """Test error handling when reading non-existent table."""
        metadata_path = tmp_path / "metadata_missing.ducklake"
        data_path = tmp_path / "data_missing"
        data_path.mkdir(exist_ok=True)

        duckdb_conn.execute(f"""
            ATTACH 'ducklake:sqlite:{metadata_path}' AS missing_db
            (DATA_PATH '{data_path}/')
        """)

        duckdb_conn.execute("DETACH missing_db")

        with pytest.raises(AnalysisException):
            spark.read.format("ducklake").options(
                url=f"sqlite:///{metadata_path}", table="nonexistent_table", base_path=f"file://{data_path}/"
            ).load().collect()

    def test_ducklake_read_missing_options_error(self, spark):
        """Test error handling when required options are missing."""
        with pytest.raises(AnalysisException):
            spark.read.format("ducklake").options(table="test_table", base_path="file:///tmp/data/").load().collect()

        with pytest.raises(AnalysisException):
            spark.read.format("ducklake").options(
                url="sqlite:///tmp/metadata.db", base_path="file:///tmp/data/"
            ).load().collect()

        with pytest.raises(AnalysisException):
            spark.read.format("ducklake").options(url="sqlite:///tmp/metadata.db", table="test_table").load().collect()


class TestDuckLakeReadAdvanced:
    """Advanced DuckLake read tests."""

    def test_ducklake_read_with_aggregation(self, spark, ducklake_setup):
        """Test DuckLake read with aggregation."""
        metadata_path, data_path, table_name = ducklake_setup

        df = (
            spark.read.format("ducklake")
            .options(url=f"sqlite:///{metadata_path}", table="test_table", base_path=f"file://{data_path}/")
            .load()
        )

        avg_score = df.agg({"score": "avg"}).collect()[0][0]

        expected_avg = (95.5 + 87.3 + 92.1) / 3
        avg_tolerance = 0.01
        assert abs(avg_score - expected_avg) < avg_tolerance

    def test_ducklake_read_with_join(self, spark, ducklake_setup, duckdb_conn):
        """Test DuckLake read with join operations."""
        metadata_path, data_path, _table_name = ducklake_setup

        duckdb_conn.execute(f"""
            ATTACH 'ducklake:sqlite:{metadata_path}' AS my_ducklake
            (DATA_PATH '{data_path}/')
        """)

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

        duckdb_conn.execute("DETACH my_ducklake")

        df1 = (
            spark.read.format("ducklake")
            .options(url=f"sqlite:///{metadata_path}", table="test_table", base_path=f"file://{data_path}/")
            .load()
        )

        df2 = (
            spark.read.format("ducklake")
            .options(url=f"sqlite:///{metadata_path}", table="departments", base_path=f"file://{data_path}/")
            .load()
        )

        joined_df = df1.join(df2, df1.id == df2.id, "inner").select(df1.name, df2.dept_name).sort(df1.name)

        result = joined_df.collect()

        expected_join_count = 3
        assert len(result) == expected_join_count
        assert result[0].name == "Alice"
        assert result[0].dept_name == "Engineering"

    def test_ducklake_read_count(self, spark, ducklake_setup):
        """Test DuckLake read with count operation."""
        metadata_path, data_path, _table_name = ducklake_setup

        df = (
            spark.read.format("ducklake")
            .options(url=f"sqlite:///{metadata_path}", table="test_table", base_path=f"file://{data_path}/")
            .load()
        )

        count = df.count()
        expected_count = 3
        assert count == expected_count
