"""Tests for JDBC data source using Spark Connect."""

import os
import sqlite3
import tempfile
from collections import Counter
from collections.abc import Iterable

import pytest


def _compute_status_counts(rows: Iterable[tuple[int, str, float, str]]) -> Counter:
    return Counter(status for *_, status in rows)


@pytest.fixture(scope="module")
def sqlite_db():
    """Create a temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".db", delete=False) as f:
        db_path = f.name

    # Create database with test data
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_name TEXT,
            amount REAL,
            status TEXT
        )
        """
    )

    test_data = [
        (1, "Alice", 100.50, "completed"),
        (2, "Bob", 250.75, "pending"),
        (3, "Charlie", 75.25, "completed"),
        (4, "David", 150.00, "completed"),
        (5, "Eve", 300.00, "pending"),
    ]
    cursor.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", test_data)

    conn.commit()
    conn.close()

    yield db_path, test_data

    os.unlink(db_path)


def test_jdbc_format_basic_read(spark, sqlite_db):
    """Test basic JDBC read using format('jdbc')."""
    db_path, test_data = sqlite_db
    df = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:sqlite:{db_path}")
        .option("dbtable", "orders")
        .option("engine", "connectorx")
        .load()
    )

    result = df.collect()
    expected_row_count = len(test_data)

    assert len(result) == expected_row_count
    assert df.columns == ["order_id", "customer_name", "amount", "status"]


def test_jdbc_format_with_filter(spark, sqlite_db):
    """Test JDBC read with SQL filter."""
    db_path, test_data = sqlite_db
    df = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:sqlite:{db_path}")
        .option("dbtable", "(SELECT * FROM orders WHERE status = 'completed') AS filtered")
        .option("engine", "connectorx")
        .load()
    )

    result = df.collect()
    expected_completed = sum(status == "completed" for *_, status in test_data)

    assert len(result) == expected_completed
    assert all(row["status"] == "completed" for row in result)


def test_jdbc_python_format(spark, sqlite_db):
    """Test JDBC read using generic Python format (advanced usage)."""
    db_path, test_data = sqlite_db
    df = (
        spark.read.format("python")
        .option("python_module", "pysail.jdbc.datasource")
        .option("python_class", "JDBCArrowDataSource")
        .option("url", f"jdbc:sqlite:{db_path}")
        .option("dbtable", "orders")
        .option("engine", "connectorx")
        .load()
    )

    result = df.collect()

    assert len(result) == len(test_data)


def test_jdbc_with_spark_operations(spark, sqlite_db):
    """Test that JDBC DataFrames work with Spark operations."""
    db_path, test_data = sqlite_db
    df = spark.read.format("jdbc").option("url", f"jdbc:sqlite:{db_path}").option("dbtable", "orders").load()

    # Filter
    filtered = df.filter(df.status == "completed")
    expected_completed = sum(status == "completed" for *_, status in test_data)
    assert filtered.count() == expected_completed

    # Group by
    expected_counts = _compute_status_counts(test_data)
    grouped = df.groupBy("status").count()
    result = {row["status"]: row["count"] for row in grouped.collect()}
    assert result == expected_counts

    # Select
    names = df.select("customer_name").collect()
    assert len(names) == len(test_data)


def test_jdbc_schema_inference(spark, sqlite_db):
    """Test that schema is correctly inferred from database."""
    db_path, _ = sqlite_db
    df = spark.read.format("jdbc").option("url", f"jdbc:sqlite:{db_path}").option("dbtable", "orders").load()

    schema = df.schema
    field_names = [field.name for field in schema.fields]

    assert "order_id" in field_names
    assert "customer_name" in field_names
    assert "amount" in field_names
    assert "status" in field_names
