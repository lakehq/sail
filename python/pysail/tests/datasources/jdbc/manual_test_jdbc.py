"""
Manual integration tests for the JDBC datasource.

Run with a PostgreSQL instance and a running Sail server.
See README.md for setup instructions.

Usage::

    hatch run python python/pysail/tests/datasources/jdbc/manual_test_jdbc.py
"""

from __future__ import annotations

import os
import sys
import traceback

# ---------------------------------------------------------------------------
# Connection parameters — override via environment variables
# ---------------------------------------------------------------------------
PG_URL = os.environ.get("JDBC_URL", "jdbc:postgresql://localhost:5432/testdb")
PG_USER = os.environ.get("JDBC_USER", "testuser")
PG_PASSWORD = os.environ.get("JDBC_PASSWORD", "testpass")
SPARK_REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:50051")

COMMON_PROPS = {"user": PG_USER, "password": PG_PASSWORD}
COMMON_OPTS = {"user": PG_USER, "password": PG_PASSWORD}


# ---------------------------------------------------------------------------
# Test harness
# ---------------------------------------------------------------------------
_results: list[tuple[str, bool, str]] = []


def _run(name: str, fn) -> None:
    try:
        fn()
        _results.append((name, True, ""))
    except Exception:  # noqa: BLE001
        tb = traceback.format_exc()
        _results.append((name, False, tb))


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------


def _make_spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()


def _register(spark):
    from pysail.datasources.jdbc.datasource import JdbcDataSource

    spark.dataSource.register(JdbcDataSource)


# ---------------------------------------------------------------------------
# Tests — basic format("jdbc") read
# ---------------------------------------------------------------------------


def test_basic_format_read(spark):
    df = spark.read.format("jdbc").option("url", PG_URL).option("dbtable", "users").options(**COMMON_OPTS).load()
    rows = df.collect()
    assert len(rows) == 15, f"Expected 15 rows, got {len(rows)}"
    col_names = {f.name for f in df.schema.fields}
    assert "id" in col_names
    assert "name" in col_names


# ---------------------------------------------------------------------------
# Tests — spark.read.jdbc() shorthand
# ---------------------------------------------------------------------------


def test_jdbc_shorthand(spark):
    df = spark.read.jdbc(PG_URL, "users", properties=COMMON_PROPS)
    rows = df.collect()
    assert len(rows) == 15, f"Expected 15 rows, got {len(rows)}"


# ---------------------------------------------------------------------------
# Tests — query option with custom SQL
# ---------------------------------------------------------------------------


def test_query_option(spark):
    df = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("query", "SELECT id, name FROM users WHERE active = TRUE")
        .options(**COMMON_OPTS)
        .load()
    )
    rows = df.collect()
    assert len(rows) > 0, "Expected at least one active user"
    col_names = {f.name for f in df.schema.fields}
    assert col_names == {"id", "name"}, f"Unexpected columns: {col_names}"


# ---------------------------------------------------------------------------
# Tests — schema-qualified dbtable
# ---------------------------------------------------------------------------


def test_schema_qualified_dbtable(spark):
    df = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "analytics.events")
        .options(**COMMON_OPTS)
        .load()
    )
    rows = df.collect()
    assert len(rows) == 4, f"Expected 4 events, got {len(rows)}"


# ---------------------------------------------------------------------------
# Tests — range-stride partitioned read
# ---------------------------------------------------------------------------


def test_partitioned_read(spark):
    # Read with 4 partitions across id 1..10000
    df_partitioned = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "large_table")
        .option("partitionColumn", "id")
        .option("lowerBound", "1")
        .option("upperBound", "10000")
        .option("numPartitions", "4")
        .options(**COMMON_OPTS)
        .load()
    )
    df_single = (
        spark.read.format("jdbc").option("url", PG_URL).option("dbtable", "large_table").options(**COMMON_OPTS).load()
    )
    assert df_partitioned.count() == df_single.count(), (
        f"Partitioned ({df_partitioned.count()}) != single ({df_single.count()})"
    )


# ---------------------------------------------------------------------------
# Tests — customSchema partial override
# ---------------------------------------------------------------------------


def test_custom_schema(spark):
    df = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "products")
        .option("customSchema", "price DOUBLE, quantity INTEGER")
        .options(**COMMON_OPTS)
        .load()
    )
    schema_map = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert "double" in schema_map.get("price", ""), f"price type: {schema_map.get('price')}"
    assert "int" in schema_map.get("quantity", ""), f"quantity type: {schema_map.get('quantity')}"


# ---------------------------------------------------------------------------
# Tests — pushDownPredicate=false
# ---------------------------------------------------------------------------


def test_push_down_predicate_false(spark):
    # With pushDownPredicate=false, all rows are fetched and filtered by Sail
    df = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "users")
        .option("pushDownPredicate", "false")
        .options(**COMMON_OPTS)
        .load()
        .filter("active = true")
    )
    rows = df.collect()
    assert len(rows) > 0, "Expected active users"
    assert all(r.active for r in rows), "All returned rows should be active"


# ---------------------------------------------------------------------------
# Tests — dbtable + query raises ValueError
# ---------------------------------------------------------------------------


def test_dbtable_and_query_raises(spark):
    raised = False
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", PG_URL)
            .option("dbtable", "users")
            .option("query", "SELECT 1")
            .options(**COMMON_OPTS)
            .load()
        )
        df.collect()  # trigger schema inference which raises ValueError
    except Exception as e:  # noqa: BLE001
        if "mutually exclusive" in str(e).lower() or "dbtable" in str(e).lower():
            raised = True
    assert raised, "Expected ValueError for dbtable+query, but no error was raised"


# ---------------------------------------------------------------------------
# Tests — predicates in spark.read.jdbc() raises error
# ---------------------------------------------------------------------------


def test_predicates_raises(spark):
    """predicates= is not supported; should raise an error from PySpark or Sail."""
    raised = False
    try:
        spark.read.jdbc(
            PG_URL,
            "users",
            predicates=["id < 5", "id >= 5"],
            properties=COMMON_PROPS,
        ).collect()
    except Exception:  # noqa: BLE001
        raised = True
    assert raised, "Expected an error when using predicates=, but none was raised"


# ---------------------------------------------------------------------------
# Tests — ImportError without connectorx
# (Structural test — verified by reading the guard in datasource.py)
# ---------------------------------------------------------------------------


def test_import_guard_message():
    """Verify that the ImportError message mentions pysail[jdbc]."""
    import importlib
    import sys

    # Temporarily hide connectorx from the import system
    original = sys.modules.pop("connectorx", None)
    # Also remove already-loaded datasource module so it re-executes
    ds_mod = sys.modules.pop("pysail.datasources.jdbc.datasource", None)
    jdbc_mod = sys.modules.pop("pysail.datasources.jdbc", None)

    try:
        import connectorx as _fake  # noqa: F401
        # If connectorx is truly not installed, we just skip this test
    except ImportError:
        try:
            importlib.import_module("pysail.datasources.jdbc.datasource")
            msg = "Expected ImportError"
            raise AssertionError(msg)
        except ImportError as e:
            assert "pysail[jdbc]" in str(e), f"Unexpected error message: {e}"  # noqa: PT017
        return
    finally:
        # Restore original state
        if original is not None:
            sys.modules["connectorx"] = original
        if ds_mod is not None:
            sys.modules["pysail.datasources.jdbc.datasource"] = ds_mod
        if jdbc_mod is not None:
            sys.modules["pysail.datasources.jdbc"] = jdbc_mod

    # connectorx IS installed in this environment — skip


# ---------------------------------------------------------------------------
# Tests — non-existent table raises error
# ---------------------------------------------------------------------------


def test_error_nonexistent_table(spark):
    raised = False
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", PG_URL)
            .option("dbtable", "nonexistent_table_12345")
            .options(**COMMON_OPTS)
            .load()
        )
        df.collect()
    except Exception as e:  # noqa: BLE001
        msg = str(e).lower()
        if "not found" in msg or "does not exist" in msg or "table" in msg or "error" in msg:
            raised = True
    assert raised, "Expected table-not-found error"


# ---------------------------------------------------------------------------
# Tests — NULL value handling
# ---------------------------------------------------------------------------


def test_null_values_handling(spark):
    df = spark.read.format("jdbc").option("url", PG_URL).option("dbtable", "users").options(**COMMON_OPTS).load()
    # IS NULL filter (applied by Sail post-read since JDBC doesn't push IS NULL)
    null_ages = df.filter("age IS NULL").collect()
    assert len(null_ages) >= 1, "Should have at least one user with NULL age"

    non_null = df.filter("age IS NOT NULL").collect()
    assert len(non_null) == 14, f"Expected 14 non-NULL age rows, got {len(non_null)}"

    assert len(null_ages) + len(non_null) == 15


# ---------------------------------------------------------------------------
# Tests — empty table
# ---------------------------------------------------------------------------


def test_empty_table(spark):
    df = spark.read.format("jdbc").option("url", PG_URL).option("dbtable", "empty_table").options(**COMMON_OPTS).load()
    count = df.count()
    assert count == 0, f"Empty table should have 0 rows, got {count}"
    result = df.filter("id > 0").collect()
    assert len(result) == 0


# ---------------------------------------------------------------------------
# Tests — large dataset (10K rows)
# ---------------------------------------------------------------------------


def test_large_dataset(spark):
    df = spark.read.format("jdbc").option("url", PG_URL).option("dbtable", "large_table").options(**COMMON_OPTS).load()
    count = df.count()
    assert count == 10000, f"Expected 10000 rows, got {count}"

    # Filter by id (integer column)
    filtered = df.filter("id > 5000").count()
    assert filtered == 5000, f"Expected 5000 rows (id > 5000), got {filtered}"


# ---------------------------------------------------------------------------
# Tests — unicode strings
# ---------------------------------------------------------------------------


def test_unicode_strings(spark):
    df = spark.read.format("jdbc").option("url", PG_URL).option("dbtable", "users").options(**COMMON_OPTS).load()
    chinese = df.filter("name = '张伟'").collect()
    assert len(chinese) == 1, "Should find Chinese name"

    spanish = df.filter("name = 'José García'").collect()
    assert len(spanish) == 1, "Should find Spanish name"

    arabic = df.filter("name = 'محمد علي'").collect()
    assert len(arabic) == 1, "Should find Arabic name"

    emoji = df.filter("name LIKE '%😀%'").collect()
    assert len(emoji) == 1, "Should find Emoji name"


# ---------------------------------------------------------------------------
# Tests — data types coverage
# ---------------------------------------------------------------------------


def test_data_types(spark):
    df = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "data_types_test")
        .options(**COMMON_OPTS)
        .load()
    )
    row = df.filter("id = 1").collect()[0]

    assert row.col_smallint == 100
    assert row.col_integer == 10000
    assert row.col_bigint == 1000000000
    assert abs(row.col_real - 3.14) < 0.01
    assert abs(row.col_double - 2.718281828) < 0.0001
    assert row.col_text == "Sample text"
    assert row.col_varchar == "Sample varchar"
    assert row.col_boolean is True

    # NULL row
    null_row = df.filter("id = 2").collect()[0]
    assert null_row.col_integer is None


# ---------------------------------------------------------------------------
# Tests — JOIN operations
# ---------------------------------------------------------------------------


def test_join_operations(spark):
    users = spark.read.format("jdbc").option("url", PG_URL).option("dbtable", "users").options(**COMMON_OPTS).load()
    orders = spark.read.format("jdbc").option("url", PG_URL).option("dbtable", "orders").options(**COMMON_OPTS).load()
    products = (
        spark.read.format("jdbc").option("url", PG_URL).option("dbtable", "products").options(**COMMON_OPTS).load()
    )

    users.createOrReplaceTempView("jdbc_users")
    orders.createOrReplaceTempView("jdbc_orders")
    products.createOrReplaceTempView("jdbc_products")

    # INNER JOIN
    result = spark.sql("""
        SELECT u.name, o.order_id, o.quantity
        FROM jdbc_users u
        INNER JOIN jdbc_orders o ON u.id = o.user_id
        WHERE u.age IS NOT NULL
        ORDER BY u.name
    """).collect()
    assert len(result) > 0, "Expected at least one JOIN result"

    # Three-way JOIN
    result3 = spark.sql("""
        SELECT u.name, p.name AS product_name, o.quantity
        FROM jdbc_orders o
        INNER JOIN jdbc_users u ON o.user_id = u.id
        INNER JOIN jdbc_products p ON o.product_id = p.id
        WHERE u.age IS NOT NULL
        LIMIT 5
    """).collect()
    assert len(result3) > 0, "Expected three-way JOIN results"


# ---------------------------------------------------------------------------
# Tests — complex filters (OR, IN, LIKE)
# ---------------------------------------------------------------------------


def test_complex_filters(spark):
    df = spark.read.format("jdbc").option("url", PG_URL).option("dbtable", "users").options(**COMMON_OPTS).load()
    # OR filter
    result = df.filter("age = 30 OR age = 35").collect()
    assert len(result) >= 2, f"Expected >= 2 rows for OR filter, got {len(result)}"

    # IN filter
    result = df.filter("age IN (25, 30, 35)").collect()
    assert len(result) >= 3, f"Expected >= 3 rows for IN filter, got {len(result)}"

    # LIKE filter
    result = df.filter("email LIKE '%example.com'").collect()
    assert len(result) > 10, f"Expected >10 example.com emails, got {len(result)}"

    # NOT filter
    result = df.filter("NOT (age > 35)").collect()
    assert len(result) > 0, "Expected rows where age <= 35"


# ---------------------------------------------------------------------------
# Tests — partition with NULLs in non-partition column
# ---------------------------------------------------------------------------


def test_partition_with_nulls(spark):
    # Partition by id (no NULLs), but age has NULLs — they should still appear
    df = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "users")
        .option("partitionColumn", "id")
        .option("lowerBound", "1")
        .option("upperBound", "15")
        .option("numPartitions", "3")
        .options(**COMMON_OPTS)
        .load()
    )
    total = df.count()
    assert total == 15, f"Expected 15 rows with 3 partitions, got {total}"

    null_ages = df.filter("age IS NULL").count()
    assert null_ages >= 1, "NULL age rows should survive partitioned read"


# ---------------------------------------------------------------------------
# Tests — concurrent readers
# ---------------------------------------------------------------------------


def test_concurrent_readers(spark):
    df_users = spark.read.format("jdbc").option("url", PG_URL).option("dbtable", "users").options(**COMMON_OPTS).load()
    df_products = (
        spark.read.format("jdbc").option("url", PG_URL).option("dbtable", "products").options(**COMMON_OPTS).load()
    )
    df_large = (
        spark.read.format("jdbc").option("url", PG_URL).option("dbtable", "large_table").options(**COMMON_OPTS).load()
    )

    count_users = df_users.count()
    count_products = df_products.count()
    count_large = df_large.count()

    assert count_users == 15, f"Expected 15 users, got {count_users}"
    assert count_products == 5, f"Expected 5 products, got {count_products}"
    assert count_large == 10000, f"Expected 10000 large rows, got {count_large}"


# ---------------------------------------------------------------------------
# Tests — filter value SQL injection protection
# ---------------------------------------------------------------------------


def test_sql_injection_filter_value(spark):
    """Single-quote escaping in _filter_to_sql prevents injection via filter values."""
    # Confirm orders baseline
    orders_before = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "orders")
        .options(**COMMON_OPTS)
        .load()
        .count()
    )
    assert orders_before == 8, f"Precondition failed: expected 8 orders, got {orders_before}"

    from pyspark.sql.functions import col

    injection_payload = "'; DROP TABLE orders; --"
    users_df = spark.read.format("jdbc").option("url", PG_URL).option("dbtable", "users").options(**COMMON_OPTS).load()
    result = users_df.filter(col("name") == injection_payload).collect()
    assert len(result) == 0, f"Injection payload matched {len(result)} row(s), expected 0"

    # orders must still exist
    orders_after = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "orders")
        .options(**COMMON_OPTS)
        .load()
        .count()
    )
    assert orders_after == 8, f"Injection may have succeeded: orders now has {orders_after} rows"


# ---------------------------------------------------------------------------
# Unit tests — no Spark required
# ---------------------------------------------------------------------------


def test_filter_to_sql_unit():
    """_filter_to_sql converts PySpark filters to SQL fragments correctly."""
    # We need real filter objects — import them
    from pyspark.sql.datasource import (
        EqualTo,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual,
    )

    from pysail.datasources.jdbc.datasource import _filter_to_sql

    cases = [
        # Column names are double-quoted
        (EqualTo(("age",), 28), '"age" = 28'),
        (GreaterThan(("score",), 9.0), '"score" > 9.0'),
        (GreaterThanOrEqual(("id",), 1), '"id" >= 1'),
        (LessThan(("age",), 40), '"age" < 40'),
        (LessThanOrEqual(("age",), 35), '"age" <= 35'),
        # String value — single-quoted
        (EqualTo(("name",), "Alice"), "\"name\" = 'Alice'"),
        # SQL injection via string value — single quote escaped
        (EqualTo(("name",), "O'Reilly"), "\"name\" = 'O''Reilly'"),
        # Boolean values
        (EqualTo(("active",), True), '"active" = TRUE'),
        (EqualTo(("active",), False), '"active" = FALSE'),
    ]

    for f, expected in cases:
        result = _filter_to_sql(f)
        assert result == expected, f"_filter_to_sql({f!r}) = {result!r}, expected {expected!r}"


def test_jdbc_url_to_dsn_unit():
    """_jdbc_url_to_dsn strips jdbc: prefix and embeds credentials."""
    from pysail.datasources.jdbc.datasource import _jdbc_url_to_dsn

    # No credentials
    assert _jdbc_url_to_dsn("jdbc:postgresql://localhost:5432/db", None, None) == "postgresql://localhost:5432/db"

    # User only
    assert (
        _jdbc_url_to_dsn("jdbc:postgresql://localhost:5432/db", "alice", None) == "postgresql://alice@localhost:5432/db"
    )

    # User + password
    assert (
        _jdbc_url_to_dsn("jdbc:postgresql://localhost:5432/db", "alice", "secret")
        == "postgresql://alice:secret@localhost:5432/db"
    )

    # Password with special chars — should be URL-encoded
    result = _jdbc_url_to_dsn("jdbc:postgresql://h:5432/db", "u", "p@ss/w0rd")
    assert "p%40ss%2Fw0rd" in result, f"Special chars not encoded: {result}"

    # Invalid URL
    try:
        _jdbc_url_to_dsn("postgresql://localhost/db", None, None)
        msg = "Expected ValueError"
        raise AssertionError(msg)
    except ValueError:
        pass


def test_parse_custom_schema_unit():
    """_parse_custom_schema parses Spark DDL strings into Arrow types."""
    import pyarrow as pa

    from pysail.datasources.jdbc.datasource import _parse_custom_schema

    result = _parse_custom_schema("id BIGINT, name STRING, score DOUBLE, active BOOLEAN")
    assert result["id"] == pa.int64()
    assert result["name"] == pa.large_utf8()
    assert result["score"] == pa.float64()
    assert result["active"] == pa.bool_()

    # DECIMAL with precision/scale
    result = _parse_custom_schema("price DECIMAL(10,2)")
    assert result["price"] == pa.decimal128(10, 2)

    # Case-insensitive column names
    result = _parse_custom_schema("MyCol INTEGER")
    assert "mycol" in result
    assert result["mycol"] == pa.int32()

    # Empty string
    assert _parse_custom_schema("") == {}


# ---------------------------------------------------------------------------
# Unit tests — _lit handles bool, None, datetime
# ---------------------------------------------------------------------------


def test_lit_unit():
    """_lit produces correct SQL literals for all supported Python types."""
    import datetime as dt

    from pyspark.sql.datasource import EqualTo

    from pysail.datasources.jdbc.datasource import _filter_to_sql

    # Use _filter_to_sql as a thin wrapper to exercise _lit
    def lit(v):
        return _filter_to_sql(EqualTo(("x",), v)).split(" = ", 1)[1]

    assert lit(True) == "TRUE"
    assert lit(False) == "FALSE"
    assert lit(None) == "NULL"
    assert lit(42) == "42"
    assert lit(3.14) == "3.14"
    assert lit("hello") == "'hello'"
    assert lit("O'Reilly") == "'O''Reilly'"
    assert lit(dt.date(2024, 1, 15)) == "'2024-01-15'"
    assert lit(dt.datetime(2024, 1, 15, 10, 30)) == "'2024-01-15T10:30:00'"  # noqa: DTZ001


# ---------------------------------------------------------------------------
# Unit tests — _quote_identifier
# ---------------------------------------------------------------------------


def test_quote_identifier_unit():
    """_quote_identifier wraps names in double quotes and escapes embedded quotes."""
    from pysail.datasources.jdbc.datasource import _quote_identifier

    assert _quote_identifier("age") == '"age"'
    assert _quote_identifier("my col") == '"my col"'
    assert _quote_identifier('col"name') == '"col""name"'
    assert _quote_identifier("") == '""'


# ---------------------------------------------------------------------------
# Integration tests — validation edge cases (need Spark to trigger _resolve_options)
# ---------------------------------------------------------------------------


def _expects_error(spark, opts: dict, keyword: str, msg: str) -> None:
    """Helper: assert that .collect() raises an exception whose str contains keyword."""
    raised = False
    try:
        (spark.read.format("jdbc").options(**opts).load()).collect()
    except Exception as e:  # noqa: BLE001
        if keyword.lower() in str(e).lower():
            raised = True
    assert raised, msg


# ---------------------------------------------------------------------------
# Tests — lowerBound > upperBound raises ValueError
# ---------------------------------------------------------------------------


def test_lower_bound_gt_upper_bound(spark):
    _expects_error(
        spark,
        {
            "url": PG_URL,
            "dbtable": "users",
            "partitionColumn": "id",
            "lowerBound": "100",
            "upperBound": "10",
            "numPartitions": "2",
            **COMMON_OPTS,
        },
        keyword="lowerBound",
        msg="Expected error when lowerBound > upperBound",
    )


# ---------------------------------------------------------------------------
# Tests — lowerBound == upperBound raises ValueError
# ---------------------------------------------------------------------------


def test_lower_bound_eq_upper_bound(spark):
    _expects_error(
        spark,
        {
            "url": PG_URL,
            "dbtable": "users",
            "partitionColumn": "id",
            "lowerBound": "10",
            "upperBound": "10",
            "numPartitions": "2",
            **COMMON_OPTS,
        },
        keyword="lowerBound",
        msg="Expected error when lowerBound == upperBound",
    )


# ---------------------------------------------------------------------------
# Tests — non-integer lowerBound/upperBound raises ValueError
# ---------------------------------------------------------------------------


def test_non_integer_bounds(spark):
    _expects_error(
        spark,
        {
            "url": PG_URL,
            "dbtable": "users",
            "partitionColumn": "id",
            "lowerBound": "1.5",
            "upperBound": "10",
            "numPartitions": "2",
            **COMMON_OPTS,
        },
        keyword="integer",
        msg="Expected error for non-integer lowerBound",
    )


# ---------------------------------------------------------------------------
# Tests — empty dbtable raises ValueError
# ---------------------------------------------------------------------------


def test_empty_dbtable(spark):
    _expects_error(
        spark,
        {"url": PG_URL, "dbtable": "", **COMMON_OPTS},
        keyword="dbtable",
        msg="Expected error for empty dbtable",
    )


# ---------------------------------------------------------------------------
# Tests — boolean filter pushdown (_lit fix)
# ---------------------------------------------------------------------------


def test_boolean_filter_pushdown(spark):
    # Filter is pushed down to the database as: "active" = TRUE
    df = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "users")
        .options(**COMMON_OPTS)
        .load()
        .filter("active = true")
    )
    rows = df.collect()
    # 12 of 15 users have active=TRUE in init.sql
    assert len(rows) == 12, f"Expected 12 active users, got {len(rows)}"
    assert all(r.active for r in rows), "All rows should have active=TRUE"


# ---------------------------------------------------------------------------
# Tests — rows outside [lowerBound, upperBound] are included
# ---------------------------------------------------------------------------


def test_rows_outside_bounds_included(spark):
    # Use narrow bounds (1000..9000) on large_table (id 1..10000).
    # Open-ended first/last partitions must capture the outlier rows.
    df_partitioned = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "large_table")
        .option("partitionColumn", "id")
        .option("lowerBound", "1000")
        .option("upperBound", "9000")
        .option("numPartitions", "4")
        .options(**COMMON_OPTS)
        .load()
    )
    df_full = (
        spark.read.format("jdbc").option("url", PG_URL).option("dbtable", "large_table").options(**COMMON_OPTS).load()
    )
    assert df_partitioned.count() == df_full.count(), (
        "Partitioned read with narrow bounds should include all rows, "
        f"got {df_partitioned.count()} vs {df_full.count()}"
    )


# ---------------------------------------------------------------------------
# Tests — customSchema column not in table is silently ignored
# ---------------------------------------------------------------------------


def test_custom_schema_unknown_column(spark):
    df = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "users")
        .option("customSchema", "nonexistent_col BIGINT, age BIGINT")
        .options(**COMMON_OPTS)
        .load()
    )
    # Should not raise; 'nonexistent_col' is ignored, 'age' is overridden
    schema_map = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert "bigint" in schema_map.get("age", ""), f"age type: {schema_map.get('age')}"
    assert "nonexistent_col" not in schema_map, "Unknown column should not appear in schema"


# ---------------------------------------------------------------------------
# Tests — customSchema column name matching is case-insensitive
# ---------------------------------------------------------------------------


def test_custom_schema_case_insensitive(spark):
    df = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "users")
        .option("customSchema", "AGE BIGINT, NAME STRING")
        .options(**COMMON_OPTS)
        .load()
    )
    schema_map = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert "bigint" in schema_map.get("age", ""), f"age type: {schema_map.get('age')}"
    assert "string" in schema_map.get("name", "") or "utf8" in schema_map.get("name", ""), (
        f"name type: {schema_map.get('name')}"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    # Unit tests first (no Spark/DB required)
    _run("import guard message", test_import_guard_message)
    _run("unit: _filter_to_sql", test_filter_to_sql_unit)
    _run("unit: _jdbc_url_to_dsn", test_jdbc_url_to_dsn_unit)
    _run("unit: _parse_custom_schema", test_parse_custom_schema_unit)
    _run("unit: _lit type handling", test_lit_unit)
    _run("unit: _quote_identifier", test_quote_identifier_unit)

    # Spark integration tests
    try:
        spark = _make_spark()
        _register(spark)
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        sys.exit(1)

    _run("basic format('jdbc') read", lambda: test_basic_format_read(spark))
    _run("spark.read.jdbc() shorthand", lambda: test_jdbc_shorthand(spark))
    _run("query option with custom SQL", lambda: test_query_option(spark))
    _run("schema-qualified dbtable", lambda: test_schema_qualified_dbtable(spark))
    _run("range-stride partitioned read", lambda: test_partitioned_read(spark))
    _run("customSchema partial override", lambda: test_custom_schema(spark))
    _run("pushDownPredicate=false", lambda: test_push_down_predicate_false(spark))
    _run("dbtable+query raises ValueError", lambda: test_dbtable_and_query_raises(spark))
    _run("predicates raises error", lambda: test_predicates_raises(spark))
    _run("non-existent table error", lambda: test_error_nonexistent_table(spark))
    _run("NULL value handling", lambda: test_null_values_handling(spark))
    _run("empty table", lambda: test_empty_table(spark))
    _run("large dataset (10K rows)", lambda: test_large_dataset(spark))
    _run("unicode strings", lambda: test_unicode_strings(spark))
    _run("data types coverage", lambda: test_data_types(spark))
    _run("JOIN operations", lambda: test_join_operations(spark))
    _run("complex filters", lambda: test_complex_filters(spark))
    _run("partition with NULLs", lambda: test_partition_with_nulls(spark))
    _run("concurrent readers", lambda: test_concurrent_readers(spark))
    _run("filter value injection protection", lambda: test_sql_injection_filter_value(spark))
    _run("lowerBound > upperBound raises error", lambda: test_lower_bound_gt_upper_bound(spark))
    _run("lowerBound == upperBound raises error", lambda: test_lower_bound_eq_upper_bound(spark))
    _run("non-integer bounds raises error", lambda: test_non_integer_bounds(spark))
    _run("empty dbtable raises error", lambda: test_empty_dbtable(spark))
    _run("boolean filter pushdown", lambda: test_boolean_filter_pushdown(spark))
    _run("rows outside bounds included", lambda: test_rows_outside_bounds_included(spark))
    _run("customSchema unknown column ignored", lambda: test_custom_schema_unknown_column(spark))
    _run("customSchema case-insensitive match", lambda: test_custom_schema_case_insensitive(spark))

    spark.stop()

    # Summary
    passed = sum(1 for _, ok, _ in _results if ok)
    total = len(_results)
    if passed < total:
        sys.exit(1)


if __name__ == "__main__":
    main()
