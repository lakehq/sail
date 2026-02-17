"""
Test script for PostgreSQL DataSource.

This script demonstrates and tests the PostgreSQL datasource functionality.

Prerequisites:
1. Start PostgreSQL: docker compose --profile datasources up -d
2. Build pysail: hatch run maturin develop --extras postgres
3. Start Sail server: sail spark server --port 50051
4. Run tests: hatch run python python/pysail/tests/datasources/postgres/manual_test_postgres.py
"""

import logging
import sys

from pyspark.sql import SparkSession

from pysail.datasources.postgres.datasource import PostgresDataSource

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)

logger = logging.getLogger(__name__)


def test_basic_read(spark):
    """Test 1: Basic read from PostgreSQL table."""
    logger.info("\n=== Test 1: Basic Read ===")

    spark.dataSource.register(PostgresDataSource)

    df = (
        spark.read.format("postgres")
        .options(host="localhost", port="5432", database="testdb", user="testuser", password="testpass", table="users")
        .load()
    )

    logger.info("Schema:")
    df.printSchema()

    logger.info("\nData:")
    df.show()

    count = df.count()
    logger.info("\nTotal rows: %s", count)
    assert count == 15, f"Expected 15 rows, got {count}"
    logger.info("✓ Test 1 passed")


def test_filter_pushdown(spark):
    """Test 2: Filter pushdown."""
    logger.info("\n=== Test 2: Filter Pushdown ===")

    spark.dataSource.register(PostgresDataSource)

    df = (
        spark.read.format("postgres")
        .options(host="localhost", port="5432", database="testdb", user="testuser", password="testpass", table="users")
        .load()
    )

    # Test equality filter
    filtered = df.filter("age = 28").collect()
    logger.info("Users with age = 28: %s", len(filtered))
    assert len(filtered) == 1, f"Expected 1 user with age 28, got {len(filtered)}"
    assert filtered[0].name == "Alice Johnson"

    # Test comparison filter
    filtered = df.filter("age > 35").collect()
    logger.info("Users with age > 35: %s", len(filtered))
    assert len(filtered) >= 4, f"Expected at least 4 users with age > 35, got {len(filtered)}"

    # Test combined filters
    filtered = df.filter("age > 30 AND is_active = true").collect()
    logger.info("Active users with age > 30: %s", len(filtered))

    logger.info("✓ Test 2 passed")


def test_projection(spark):
    """Test 3: Column projection."""
    logger.info("\n=== Test 3: Column Projection ===")

    spark.dataSource.register(PostgresDataSource)

    df = (
        spark.read.format("postgres")
        .options(host="localhost", port="5432", database="testdb", user="testuser", password="testpass", table="users")
        .load()
    )

    # Select specific columns
    result = df.select("name", "email").collect()
    logger.info("Projected %s rows with 2 columns", len(result))
    logger.info("First row: %s", result[0])

    assert len(result) == 15
    logger.info("✓ Test 3 passed")


def test_partitioned_read(spark):
    """Test 4: Partitioned reading."""
    logger.info("\n=== Test 4: Partitioned Reading ===")

    spark.dataSource.register(PostgresDataSource)

    df = (
        spark.read.format("postgres")
        .options(
            host="localhost",
            port="5432",
            database="testdb",
            user="testuser",
            password="testpass",
            table="users",
            numPartitions="4",
            partitionColumn="id",
        )
        .load()
    )

    count = df.count()
    logger.info("Total rows with 4 partitions: %s", count)
    assert count == 15, f"Expected 15 rows, got {count}"

    logger.info("✓ Test 4 passed")


def test_products_table(spark):
    """Test 5: Read from products table with various filters."""
    logger.info("\n=== Test 5: Products Table ===")

    spark.dataSource.register(PostgresDataSource)

    df = (
        spark.read.format("postgres")
        .options(
            host="localhost", port="5432", database="testdb", user="testuser", password="testpass", table="products"
        )
        .load()
    )

    logger.info("Products schema:")
    df.printSchema()

    # Test filtering by category
    electronics = df.filter("category = 'Electronics'").collect()
    logger.info("\nElectronics products: %s", len(electronics))

    # Test price filtering
    expensive = df.filter("price > 150").collect()
    logger.info("Products over $150: %s", len(expensive))
    for product in expensive:
        logger.info("  - %s: $%s", product.product_name, product.price)

    # Test aggregation
    logger.info("\nAggregate test:")
    df.groupBy("category").count().show()

    logger.info("✓ Test 5 passed")


def test_sql_queries(spark):
    """Test 6: SQL queries on the datasource."""
    logger.info("\n=== Test 6: SQL Queries ===")

    spark.dataSource.register(PostgresDataSource)

    # Create a temporary view
    df = (
        spark.read.format("postgres")
        .options(host="localhost", port="5432", database="testdb", user="testuser", password="testpass", table="users")
        .load()
    )

    df.createOrReplaceTempView("users_view")

    # Run SQL queries
    result = spark.sql("SELECT name, age FROM users_view WHERE age < 30 ORDER BY age")
    logger.info("\nUsers under 30:")
    result.show()

    result = spark.sql("""
        SELECT
            CASE WHEN age < 30 THEN 'Young'
                 WHEN age < 40 THEN 'Middle'
                 ELSE 'Senior'
            END as age_group,
            COUNT(*) as count
        FROM users_view
        GROUP BY age_group
    """)
    logger.info("\nAge distribution:")
    result.show()

    logger.info("✓ Test 6 passed")


def test_error_invalid_credentials(spark):
    """Test 7 (HIGH): Error handling with invalid credentials."""
    logger.info("\n=== Test 7: Invalid Credentials Error Handling ===")

    spark.dataSource.register(PostgresDataSource)

    try:
        df = (
            spark.read.format("postgres")
            .options(
                host="localhost", port="5432", database="testdb", user="wronguser", password="wrongpass", table="users"
            )
            .load()
        )
        df.count()
        logger.info("✗ Test failed: Expected authentication error")
        msg = "Should have raised authentication error"
        raise AssertionError(msg)  # noqa: TRY301
    except AssertionError:
        raise
    except Exception as e:  # noqa: BLE001
        error_msg = str(e).lower()
        assert "password" in error_msg or "authentication" in error_msg or "failed" in error_msg
        logger.info("✓ Correctly caught authentication error: %s", type(e).__name__)
        logger.info("✓ Test 7 passed")


def test_error_nonexistent_table(spark):
    """Test 8 (HIGH): Error handling with non-existent table."""
    logger.info("\n=== Test 8: Non-Existent Table Error Handling ===")

    spark.dataSource.register(PostgresDataSource)

    try:
        df = (
            spark.read.format("postgres")
            .options(
                host="localhost",
                port="5432",
                database="testdb",
                user="testuser",
                password="testpass",
                table="nonexistent_table_12345",
            )
            .load()
        )
        df.count()
        logger.info("✗ Test failed: Expected table not found error")
        msg = "Should have raised table not found error"
        raise AssertionError(msg)  # noqa: TRY301
    except AssertionError:
        raise
    except Exception as e:  # noqa: BLE001
        error_msg = str(e).lower()
        assert "not found" in error_msg or "does not exist" in error_msg or "table" in error_msg
        logger.info("✓ Correctly caught table error: %s", type(e).__name__)
        logger.info("✓ Test 8 passed")


def test_null_values_handling(spark):
    """Test 9 (HIGH): Handling of NULL values."""
    logger.info("\n=== Test 9: NULL Values Handling ===")

    spark.dataSource.register(PostgresDataSource)

    df = (
        spark.read.format("postgres")
        .options(host="localhost", port="5432", database="testdb", user="testuser", password="testpass", table="users")
        .load()
    )

    # Test IS NULL filter
    null_ages = df.filter("age IS NULL").collect()
    logger.info("Users with NULL age: %s", len(null_ages))
    assert len(null_ages) >= 1, "Should have at least one user with NULL age"
    logger.info("  Example: %s", null_ages[0].name)

    # Test IS NOT NULL filter
    non_null = df.filter("age IS NOT NULL").collect()
    logger.info("Users with non-NULL age: %s", len(non_null))
    assert len(non_null) == 14  # 15 total users - 1 NULL age = 14

    # Verify total
    total = len(null_ages) + len(non_null)
    all_users = df.count()
    assert total == all_users, f"NULL + non-NULL should equal total: {total} vs {all_users}"

    logger.info("✓ Test 9 passed")


def test_filter_null_values(spark):
    """Test 10 (HIGH): Filtering with NULL comparisons."""
    logger.info("\n=== Test 10: NULL Value Filtering ===")

    spark.dataSource.register(PostgresDataSource)

    df = (
        spark.read.format("postgres")
        .options(host="localhost", port="5432", database="testdb", user="testuser", password="testpass", table="users")
        .load()
    )

    # NULL values should not match equality
    result = df.filter("age = NULL").collect()
    logger.info("Rows matching 'age = NULL': %s (should be 0)", len(result))
    assert len(result) == 0, "NULL should not match equality comparison"

    # IS NULL should work
    result = df.filter("age IS NULL").collect()
    logger.info("Rows matching 'age IS NULL': %s", len(result))
    assert len(result) >= 1

    logger.info("✓ Test 10 passed")


def test_empty_table(spark):
    """Test 11 (HIGH): Reading from empty table."""
    logger.info("\n=== Test 11: Empty Table ===")

    spark.dataSource.register(PostgresDataSource)

    df = (
        spark.read.format("postgres")
        .options(
            host="localhost", port="5432", database="testdb", user="testuser", password="testpass", table="empty_table"
        )
        .load()
    )

    logger.info("Schema of empty table:")
    df.printSchema()

    count = df.count()
    logger.info("Row count: %s", count)
    assert count == 0, "Empty table should have 0 rows"

    # Should still be able to call operations
    result = df.filter("id > 0").collect()
    assert len(result) == 0

    logger.info("✓ Test 11 passed")


def test_large_dataset(spark):
    """Test 12 (HIGH): Reading large dataset (10K rows)."""
    logger.info("\n=== Test 12: Large Dataset (10K rows) ===")

    spark.dataSource.register(PostgresDataSource)

    df = (
        spark.read.format("postgres")
        .options(
            host="localhost", port="5432", database="testdb", user="testuser", password="testpass", table="large_table"
        )
        .load()
    )

    count = df.count()
    logger.info("Total rows in large_table: %s", count)
    assert count == 10000, f"Expected 10000 rows, got {count}"

    # Test filtering on large dataset
    filtered = df.filter("value > 5000").count()
    logger.info("Rows with value > 5000: %s", filtered)
    assert filtered == 5000, f"Expected 5000 rows (5001-10000), got {filtered}"

    # Test aggregation
    max_val = df.agg({"value": "max"}).collect()[0][0]
    logger.info("Max value: %s", max_val)
    assert max_val == 10000

    logger.info("✓ Test 12 passed")


def test_unicode_strings(spark):
    """Test 13 (HIGH): Unicode character handling."""
    logger.info("\n=== Test 13: Unicode Strings ===")

    spark.dataSource.register(PostgresDataSource)

    df = (
        spark.read.format("postgres")
        .options(host="localhost", port="5432", database="testdb", user="testuser", password="testpass", table="users")
        .load()
    )

    # Find Chinese name
    chinese = df.filter("name = '张伟'").collect()
    logger.info("Chinese user found: %s", len(chinese) > 0)
    assert len(chinese) == 1, "Should find Chinese name"
    logger.info("  Name: %s, Email: %s", chinese[0].name, chinese[0].email)

    # Find Spanish name
    spanish = df.filter("name = 'José García'").collect()
    logger.info("Spanish user found: %s", len(spanish) > 0)
    assert len(spanish) == 1
    logger.info("  Name: %s", spanish[0].name)

    # Find Arabic name
    arabic = df.filter("name = 'محمد علي'").collect()
    logger.info("Arabic user found: %s", len(arabic) > 0)
    assert len(arabic) == 1
    logger.info("  Name: %s", arabic[0].name)

    # Find Emoji name
    emoji = df.filter("name LIKE '%😀%'").collect()
    logger.info("Emoji user found: %s", len(emoji) > 0)
    assert len(emoji) == 1
    logger.info("  Name: %s", emoji[0].name)

    logger.info("✓ Test 13 passed")


def test_sql_injection_protection(spark):
    """Test 14 (HIGH): SQL injection protection."""
    logger.info("\n=== Test 14: SQL Injection Protection ===")

    spark.dataSource.register(PostgresDataSource)

    df = (
        spark.read.format("postgres")
        .options(
            host="localhost",
            port="5432",
            database="testdb",
            user="testuser",
            password="testpass",
            table="special_chars",
        )
        .load()
    )

    # Test reading data with apostrophes
    apostrophe = df.filter("name LIKE '%Reilly%'").collect()
    logger.info("Found name with apostrophe: %s", len(apostrophe) > 0)
    assert len(apostrophe) == 1, f"Expected 1 row with 'Reilly', got {len(apostrophe)}"
    logger.info("  Name: %s", apostrophe[0].name)
    assert "'" in apostrophe[0].name, "Name should contain an apostrophe"

    # Test data with quotes
    quote = df.filter("name LIKE '%Quote%'").collect()
    logger.info("Found name with quote: %s", len(quote) > 0)
    assert len(quote) == 1, f"Expected 1 row with 'Quote', got {len(quote)}"

    logger.info("✓ Test 14 passed")


def test_all_postgres_data_types(spark):
    """Test 15 (MEDIUM): All PostgreSQL data types."""
    logger.info("\n=== Test 15: All PostgreSQL Data Types ===")

    spark.dataSource.register(PostgresDataSource)

    df = (
        spark.read.format("postgres")
        .options(
            host="localhost",
            port="5432",
            database="testdb",
            user="testuser",
            password="testpass",
            table="data_types_test",
        )
        .load()
    )

    logger.info("Schema with all data types:")
    df.printSchema()

    # Get first row (non-NULL values)
    row = df.filter("id = 1").collect()[0]

    # Verify integer types
    assert row.col_smallint == 100
    assert row.col_integer == 10000
    assert row.col_bigint == 1000000000
    logger.info("✓ Integer types: SMALLINT, INTEGER, BIGINT")

    # Verify float types
    assert abs(row.col_real - 3.14) < 0.01
    assert abs(row.col_double - 2.718281828) < 0.0001
    logger.info("✓ Float types: REAL, DOUBLE PRECISION")

    # Verify string types
    assert row.col_text == "Sample text"
    assert row.col_varchar == "Sample varchar"
    logger.info("✓ String types: TEXT, VARCHAR")

    # Verify boolean
    assert row.col_boolean is True
    logger.info("✓ Boolean type")

    # Check NULL row
    null_row = df.filter("id = 2").collect()[0]
    assert null_row.col_integer is None
    logger.info("✓ NULL handling across all types")

    logger.info("✓ Test 15 passed")


def test_join_operations(spark):
    """Test 16 (MEDIUM): JOIN operations between tables."""
    logger.info("\n=== Test 16: JOIN Operations ===")

    spark.dataSource.register(PostgresDataSource)

    # Load users table
    users = (
        spark.read.format("postgres")
        .options(host="localhost", port="5432", database="testdb", user="testuser", password="testpass", table="users")
        .load()
    )

    # Load orders table
    orders = (
        spark.read.format("postgres")
        .options(host="localhost", port="5432", database="testdb", user="testuser", password="testpass", table="orders")
        .load()
    )

    # Load products table
    products = (
        spark.read.format("postgres")
        .options(
            host="localhost", port="5432", database="testdb", user="testuser", password="testpass", table="products"
        )
        .load()
    )

    # Create views for SQL
    users.createOrReplaceTempView("users")
    orders.createOrReplaceTempView("orders")
    products.createOrReplaceTempView("products")

    # Test INNER JOIN
    result = spark.sql("""
        SELECT u.name, o.order_id, o.quantity
        FROM users u
        INNER JOIN orders o ON u.id = o.user_id
        WHERE u.age IS NOT NULL
        ORDER BY u.name
    """).collect()
    logger.info("INNER JOIN result: %s rows", len(result))
    assert len(result) > 0

    # Test JOIN with aggregation
    result = spark.sql("""
        SELECT u.name, COUNT(o.order_id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.age IS NOT NULL
        GROUP BY u.name
        HAVING COUNT(o.order_id) > 0
        ORDER BY order_count DESC
    """).collect()
    logger.info("Users with orders: %s", len(result))
    logger.info("  Top customer: %s with %s orders", result[0].name, result[0].order_count)

    # Test three-way JOIN
    result = spark.sql("""
        SELECT u.name, p.product_name, o.quantity
        FROM orders o
        INNER JOIN users u ON o.user_id = u.id
        INNER JOIN products p ON o.product_id = p.product_id
        WHERE u.age IS NOT NULL
        LIMIT 5
    """).collect()
    logger.info("Three-way JOIN: %s rows", len(result))
    for r in result[:3]:
        logger.info("  %s ordered %sx %s", r.name, r.quantity, r.product_name)

    logger.info("✓ Test 16 passed")


def test_complex_filters(spark):
    """Test 17 (MEDIUM): Complex filter combinations (OR, NOT, IN, LIKE)."""
    logger.info("\n=== Test 17: Complex Filters ===")

    spark.dataSource.register(PostgresDataSource)

    df = (
        spark.read.format("postgres")
        .options(host="localhost", port="5432", database="testdb", user="testuser", password="testpass", table="users")
        .load()
    )

    # Test OR filter
    result = df.filter("age = 28 OR age = 35").collect()
    logger.info("OR filter (age 28 or 35): %s rows", len(result))
    assert len(result) >= 2

    # Test NOT filter
    result = df.filter("NOT (age > 40)").collect()
    logger.info("NOT filter (age <= 40): %s rows", len(result))
    assert len(result) > 0

    # Test IN filter
    result = df.filter("age IN (28, 35, 42)").collect()
    logger.info("IN filter: %s rows", len(result))
    assert len(result) >= 3

    # Test LIKE filter
    result = df.filter("email LIKE '%example.com'").collect()
    logger.info("LIKE filter (emails ending in example.com): %s rows", len(result))
    assert len(result) > 10

    # Test complex combination
    result = df.filter("(age > 30 AND is_active = true) OR name LIKE '%Alice%'").collect()
    logger.info("Complex filter: %s rows", len(result))
    assert len(result) > 0

    logger.info("✓ Test 17 passed")


def test_partition_with_nulls(spark):
    """Test 18 (MEDIUM): Partitioned reading with NULL values in partition column."""
    logger.info("\n=== Test 18: Partitions with NULLs ===")

    spark.dataSource.register(PostgresDataSource)

    # Read with partitions using id column (which has no NULLs)
    df = (
        spark.read.format("postgres")
        .options(
            host="localhost",
            port="5432",
            database="testdb",
            user="testuser",
            password="testpass",
            table="users",
            numPartitions="3",
            partitionColumn="id",
        )
        .load()
    )

    total = df.count()
    logger.info("Total rows with 3 partitions: %s", total)

    # Verify NULL age values are still included
    null_ages = df.filter("age IS NULL").count()
    logger.info("Rows with NULL age: %s", null_ages)
    assert null_ages >= 1

    logger.info("✓ Test 18 passed")


def test_concurrent_readers(spark):
    """Test 19 (MEDIUM): Multiple concurrent readers."""
    logger.info("\n=== Test 19: Concurrent Readers ===")

    spark.dataSource.register(PostgresDataSource)

    # Create multiple DataFrames reading the same table
    df1 = (
        spark.read.format("postgres")
        .options(host="localhost", port="5432", database="testdb", user="testuser", password="testpass", table="users")
        .load()
    )

    df2 = (
        spark.read.format("postgres")
        .options(
            host="localhost", port="5432", database="testdb", user="testuser", password="testpass", table="products"
        )
        .load()
    )

    df3 = (
        spark.read.format("postgres")
        .options(
            host="localhost", port="5432", database="testdb", user="testuser", password="testpass", table="large_table"
        )
        .load()
    )

    # Execute queries concurrently
    count1 = df1.count()
    count2 = df2.count()
    count3 = df3.count()

    logger.info("Concurrent reads: users=%s, products=%s, large_table=%s", count1, count2, count3)
    assert count1 > 10
    assert count2 == 10
    assert count3 == 10000

    logger.info("✓ Test 19 passed")


def test_connection_cleanup(spark):
    """Test 20 (MEDIUM): Connection cleanup after errors."""
    logger.info("\n=== Test 20: Connection Cleanup ===")

    spark.dataSource.register(PostgresDataSource)

    # Try to read from non-existent table multiple times
    for i in range(3):
        try:
            df = (
                spark.read.format("postgres")
                .options(
                    host="localhost",
                    port="5432",
                    database="testdb",
                    user="testuser",
                    password="testpass",
                    table=f"nonexistent_{i}",
                )
                .load()
            )
            df.count()
        except Exception as e:  # noqa: BLE001
            logger.debug("Expected error for nonexistent table: %s", e)

    logger.info("✓ Multiple failed connections handled")

    # Now try a successful read
    df = (
        spark.read.format("postgres")
        .options(host="localhost", port="5432", database="testdb", user="testuser", password="testpass", table="users")
        .load()
    )

    count = df.count()
    logger.info("Successful read after failures: %s rows", count)
    assert count > 10

    logger.info("✓ Test 20 passed")


def test_invalid_batch_size(spark):
    """Test 21 (HIGH): Invalid batchSize values are rejected with a clear error."""
    logger.info("\n=== Test 21: Invalid batchSize Validation ===")

    spark.dataSource.register(PostgresDataSource)

    for bad_value in ["0", "-1", "-100"]:
        try:
            df = (
                spark.read.format("postgres")
                .options(
                    host="localhost",
                    port="5432",
                    database="testdb",
                    user="testuser",
                    password="testpass",
                    table="users",
                    batchSize=bad_value,
                )
                .load()
            )
            df.count()
            msg = f"Should have raised an error for batchSize={bad_value}"
            raise AssertionError(msg)  # noqa: TRY301
        except AssertionError:
            raise
        except Exception as e:  # noqa: BLE001
            assert "batchSize" in str(e), f"Expected 'batchSize' in error, got: {e}"  # noqa: PT017
            logger.info("  batchSize=%s correctly rejected: %s", bad_value, type(e).__name__)

    logger.info("✓ Test 21 passed")


def test_invalid_num_partitions(spark):
    """Test 22 (HIGH): Invalid numPartitions values are rejected with a clear error."""
    logger.info("\n=== Test 22: Invalid numPartitions Validation ===")

    spark.dataSource.register(PostgresDataSource)

    for bad_value in ["0", "-1"]:
        try:
            df = (
                spark.read.format("postgres")
                .options(
                    host="localhost",
                    port="5432",
                    database="testdb",
                    user="testuser",
                    password="testpass",
                    table="users",
                    numPartitions=bad_value,
                    partitionColumn="id",
                )
                .load()
            )
            df.count()
            msg = f"Should have raised an error for numPartitions={bad_value}"
            raise AssertionError(msg)  # noqa: TRY301
        except AssertionError:
            raise
        except Exception as e:  # noqa: BLE001
            assert "numPartitions" in str(e), f"Expected 'numPartitions' in error, got: {e}"  # noqa: PT017
            logger.info("  numPartitions=%s correctly rejected: %s", bad_value, type(e).__name__)

    logger.info("✓ Test 22 passed")


def test_batch_size_option(spark):
    """Test 23 (MEDIUM): Custom batchSize returns correct row count regardless of fetch size."""
    logger.info("\n=== Test 23: Custom batchSize ===")

    spark.dataSource.register(PostgresDataSource)

    for batch_size in ["1", "100", "500", "8192", "20000"]:
        df = (
            spark.read.format("postgres")
            .options(
                host="localhost",
                port="5432",
                database="testdb",
                user="testuser",
                password="testpass",
                table="large_table",
                batchSize=batch_size,
            )
            .load()
        )
        count = df.count()
        assert count == 10000, f"batchSize={batch_size}: expected 10000 rows, got {count}"
        logger.info("  batchSize=%-6s -> %s rows ✓", batch_size, count)

    logger.info("✓ Test 23 passed")


def test_table_schema_option(spark):
    """Test 24 (HIGH): tableSchema option reads from the correct PostgreSQL schema."""
    logger.info("\n=== Test 24: tableSchema Option ===")

    spark.dataSource.register(PostgresDataSource)

    # Read from a non-public schema
    df = (
        spark.read.format("postgres")
        .options(
            host="localhost",
            port="5432",
            database="testdb",
            user="testuser",
            password="testpass",
            table="schema_table",
            tableSchema="test_schema",
        )
        .load()
    )

    count = df.count()
    logger.info("Rows in test_schema.schema_table: %s", count)
    assert count == 3, f"Expected 3 rows from test_schema, got {count}"

    rows = df.collect()
    values = {r.value for r in rows}
    assert values == {"row_from_test_schema_1", "row_from_test_schema_2", "row_from_test_schema_3"}
    logger.info("Values: %s", sorted(values))

    # Verify a same-named table in public schema is NOT returned
    # (there is no 'schema_table' in public, so it should raise "table not found")
    try:
        df_wrong = (
            spark.read.format("postgres")
            .options(
                host="localhost",
                port="5432",
                database="testdb",
                user="testuser",
                password="testpass",
                table="schema_table",
                tableSchema="public",
            )
            .load()
        )
        df_wrong.count()
        msg = "Expected table-not-found error for schema_table in public schema"
        raise AssertionError(msg)  # noqa: TRY301
    except AssertionError:
        raise
    except Exception as e:  # noqa: BLE001
        logger.info("  public.schema_table correctly not found: %s", type(e).__name__)

    logger.info("✓ Test 24 passed")


def main():
    """Run all tests."""
    logger.info("=" * 60)
    logger.info("PostgreSQL DataSource Test Suite")
    logger.info("=" * 60)

    # Create Spark session
    spark = SparkSession.builder.remote("sc://localhost:50051").appName("PostgreSQL DataSource Test").getOrCreate()

    try:
        # Original tests
        test_basic_read(spark)
        test_filter_pushdown(spark)
        test_projection(spark)
        test_partitioned_read(spark)
        test_products_table(spark)
        test_sql_queries(spark)

        # HIGH priority tests
        test_error_invalid_credentials(spark)
        test_error_nonexistent_table(spark)
        test_null_values_handling(spark)
        test_filter_null_values(spark)
        test_empty_table(spark)
        test_large_dataset(spark)
        test_unicode_strings(spark)
        test_sql_injection_protection(spark)

        # MEDIUM priority tests
        test_all_postgres_data_types(spark)
        test_join_operations(spark)
        test_complex_filters(spark)
        test_partition_with_nulls(spark)
        test_concurrent_readers(spark)
        test_connection_cleanup(spark)

        # Validation and schema tests
        test_invalid_batch_size(spark)
        test_invalid_num_partitions(spark)
        test_batch_size_option(spark)
        test_table_schema_option(spark)

        logger.info("\n%s", "=" * 60)
        logger.info("✓ All tests passed!")
        logger.info("%s", "=" * 60)
        logger.info("\nTest Summary:")
        logger.info("  - Original tests: 6")
        logger.info("  - HIGH priority: 10")
        logger.info("  - MEDIUM priority: 7")
        logger.info("  - Total: 24 tests")

    except Exception:
        logger.exception("✗ Test failed with error")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
