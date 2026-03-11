"""Integration tests for the JDBC data source using testcontainers.

A PostgreSQL container is started once per session and torn down at the end.
All tests run against the container using the Sail Spark Connect server.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from testcontainers.postgres import PostgresContainer

from pysail.testing.spark.utils.common import pyspark_version

# We skip all the tests in this module for now since testcontainers have some issues
# on macOS and Windows.
pytest.skip("not working", allow_module_level=True)

if pyspark_version() < (4, 1):
    pytest.skip("Python data source requires Spark 4.1+", allow_module_level=True)

_PG_IMAGE = "postgres:16-alpine"
_PG_USER = "testuser"
_PG_PASSWORD = "testpass"  # noqa: S105
_PG_DB = "testdb"


@pytest.fixture(scope="module")
def pg_container():
    """Start a PostgreSQL container and initialise the test schema."""
    init_sql = (Path(__file__).parent / "init.sql").read_text(encoding="utf-8")
    with PostgresContainer(
        image=_PG_IMAGE,
        username=_PG_USER,
        password=_PG_PASSWORD,
        dbname=_PG_DB,
        driver=None,
    ) as container:
        result = container.exec(
            [
                "psql",
                "-v",
                "ON_ERROR_STOP=1",
                "--single-transaction",
                "-U",
                _PG_USER,
                "-d",
                _PG_DB,
                "-c",
                init_sql,
            ]
        )
        assert result.exit_code == 0, f"Failed to initialise DB: {result.output}"
        yield container


@pytest.fixture(scope="module")
def jdbc_url(pg_container):
    """Return the JDBC URL for the test PostgreSQL container."""
    host = pg_container.get_container_host_ip()
    port = pg_container.get_exposed_port(5432)
    return f"jdbc:postgresql://{host}:{port}/{_PG_DB}"


@pytest.fixture(scope="module")
def jdbc_opts(jdbc_url):
    """Return common JDBC options for the test PostgreSQL container."""
    return {"url": jdbc_url, "user": _PG_USER, "password": _PG_PASSWORD}


@pytest.fixture(scope="module", autouse=True)
def register_jdbc(spark):
    """Register the JDBC data source with the Spark session."""
    from pysail.spark.datasource.jdbc import JdbcDataSource

    spark.dataSource.register(JdbcDataSource)


# ---------------------------------------------------------------------------
# Basic format("jdbc") read
# ---------------------------------------------------------------------------


def test_basic_format_read(spark, jdbc_opts):
    df = spark.read.format("jdbc").option("dbtable", "users").options(**jdbc_opts).load()
    rows = df.collect()
    assert len(rows) == 15  # noqa: PLR2004
    col_names = {f.name for f in df.schema.fields}
    assert "id" in col_names
    assert "name" in col_names


# ---------------------------------------------------------------------------
# spark.read.jdbc() shorthand
# ---------------------------------------------------------------------------


def test_jdbc_shorthand(spark, jdbc_url):
    df = spark.read.jdbc(jdbc_url, "users", properties={"user": _PG_USER, "password": _PG_PASSWORD})
    rows = df.collect()
    assert len(rows) == 15  # noqa: PLR2004


# ---------------------------------------------------------------------------
# query option with custom SQL
# ---------------------------------------------------------------------------


def test_query_option(spark, jdbc_opts):
    df = (
        spark.read.format("jdbc")
        .option("query", "SELECT id, name FROM users WHERE active = TRUE")
        .options(**jdbc_opts)
        .load()
    )
    rows = df.collect()
    assert len(rows) > 0
    col_names = {f.name for f in df.schema.fields}
    assert col_names == {"id", "name"}


# ---------------------------------------------------------------------------
# schema-qualified dbtable
# ---------------------------------------------------------------------------


def test_schema_qualified_dbtable(spark, jdbc_opts):
    df = spark.read.format("jdbc").option("dbtable", "analytics.events").options(**jdbc_opts).load()
    rows = df.collect()
    assert len(rows) == 4  # noqa: PLR2004


# ---------------------------------------------------------------------------
# range-stride partitioned read
# ---------------------------------------------------------------------------


def test_partitioned_read(spark, jdbc_opts):
    df_partitioned = (
        spark.read.format("jdbc")
        .option("dbtable", "large_table")
        .option("partitionColumn", "id")
        .option("lowerBound", "1")
        .option("upperBound", "10000")
        .option("numPartitions", "4")
        .options(**jdbc_opts)
        .load()
    )
    df_single = spark.read.format("jdbc").option("dbtable", "large_table").options(**jdbc_opts).load()
    assert df_partitioned.count() == df_single.count()


# ---------------------------------------------------------------------------
# customSchema partial override
# ---------------------------------------------------------------------------


def test_custom_schema(spark, jdbc_opts):
    df = (
        spark.read.format("jdbc")
        .option("dbtable", "products")
        .option("customSchema", "price DOUBLE, quantity INTEGER")
        .options(**jdbc_opts)
        .load()
    )
    schema_map = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert "double" in schema_map.get("price", "")
    assert "int" in schema_map.get("quantity", "")


# ---------------------------------------------------------------------------
# pushDownPredicate=false
# ---------------------------------------------------------------------------


def test_push_down_predicate_false(spark, jdbc_opts):
    df = (
        spark.read.format("jdbc")
        .option("dbtable", "users")
        .option("pushDownPredicate", "false")
        .options(**jdbc_opts)
        .load()
        .filter("active = true")
    )
    rows = df.collect()
    assert len(rows) > 0
    assert all(r.active for r in rows)


# ---------------------------------------------------------------------------
# dbtable + query raises ValueError
# ---------------------------------------------------------------------------


def test_dbtable_and_query_raises(spark, jdbc_opts):
    with pytest.raises(Exception, match=r"mutually exclusive|dbtable"):
        spark.read.format("jdbc").option("dbtable", "users").option("query", "SELECT 1").options(
            **jdbc_opts
        ).load().collect()


# ---------------------------------------------------------------------------
# predicates in spark.read.jdbc() raises error
# ---------------------------------------------------------------------------


def test_predicates_raises(spark, jdbc_url):
    with pytest.raises(Exception):  # noqa: B017, PT011
        spark.read.jdbc(
            jdbc_url,
            "users",
            predicates=["id < 5", "id >= 5"],
            properties={"user": _PG_USER, "password": _PG_PASSWORD},
        ).collect()


# ---------------------------------------------------------------------------
# Non-existent table raises error
# ---------------------------------------------------------------------------


def test_error_nonexistent_table(spark, jdbc_opts):
    with pytest.raises(Exception):  # noqa: B017, PT011
        spark.read.format("jdbc").option("dbtable", "nonexistent_table_12345").options(**jdbc_opts).load().collect()


# ---------------------------------------------------------------------------
# NULL value handling
# ---------------------------------------------------------------------------


def test_null_values_handling(spark, jdbc_opts):
    df = spark.read.format("jdbc").option("dbtable", "users").options(**jdbc_opts).load()
    null_ages = df.filter("age IS NULL").collect()
    assert len(null_ages) >= 1

    non_null = df.filter("age IS NOT NULL").collect()
    assert len(non_null) == 14  # noqa: PLR2004

    assert len(null_ages) + len(non_null) == 15  # noqa: PLR2004


# ---------------------------------------------------------------------------
# Empty table
# ---------------------------------------------------------------------------


def test_empty_table(spark, jdbc_opts):
    df = spark.read.format("jdbc").option("dbtable", "empty_table").options(**jdbc_opts).load()
    assert df.count() == 0
    assert df.filter("id > 0").collect() == []


# ---------------------------------------------------------------------------
# Large dataset (10K rows)
# ---------------------------------------------------------------------------


def test_large_dataset(spark, jdbc_opts):
    df = spark.read.format("jdbc").option("dbtable", "large_table").options(**jdbc_opts).load()
    assert df.count() == 10000  # noqa: PLR2004
    assert df.filter("id > 5000").count() == 5000  # noqa: PLR2004


# ---------------------------------------------------------------------------
# Unicode strings
# ---------------------------------------------------------------------------


def test_unicode_strings(spark, jdbc_opts):
    df = spark.read.format("jdbc").option("dbtable", "users").options(**jdbc_opts).load()
    assert len(df.filter("name = '张伟'").collect()) == 1
    assert len(df.filter("name = 'José García'").collect()) == 1
    assert len(df.filter("name = 'محمد علي'").collect()) == 1
    assert len(df.filter("name LIKE '%😀%'").collect()) == 1


# ---------------------------------------------------------------------------
# Data types coverage
# ---------------------------------------------------------------------------


def test_data_types(spark, jdbc_opts):
    df = spark.read.format("jdbc").option("dbtable", "data_types_test").options(**jdbc_opts).load()
    row = df.filter("id = 1").collect()[0]

    assert row.col_smallint == 100  # noqa: PLR2004
    assert row.col_integer == 10000  # noqa: PLR2004
    assert row.col_bigint == 1000000000  # noqa: PLR2004
    assert abs(row.col_real - 3.14) < 0.01  # noqa: PLR2004
    assert abs(row.col_double - 2.718281828) < 0.0001  # noqa: PLR2004
    assert row.col_text == "Sample text"
    assert row.col_varchar == "Sample varchar"
    assert row.col_boolean is True

    null_row = df.filter("id = 2").collect()[0]
    assert null_row.col_integer is None


# ---------------------------------------------------------------------------
# JOIN operations
# ---------------------------------------------------------------------------


def test_join_operations(spark, jdbc_opts):
    users = spark.read.format("jdbc").option("dbtable", "users").options(**jdbc_opts).load()
    orders = spark.read.format("jdbc").option("dbtable", "orders").options(**jdbc_opts).load()
    products = spark.read.format("jdbc").option("dbtable", "products").options(**jdbc_opts).load()

    users.createOrReplaceTempView("jdbc_users")
    orders.createOrReplaceTempView("jdbc_orders")
    products.createOrReplaceTempView("jdbc_products")

    result = spark.sql("""
        SELECT u.name, o.order_id, o.quantity
        FROM jdbc_users u
        INNER JOIN jdbc_orders o ON u.id = o.user_id
        WHERE u.age IS NOT NULL
        ORDER BY u.name
    """).collect()
    assert len(result) > 0

    result3 = spark.sql("""
        SELECT u.name, p.name AS product_name, o.quantity
        FROM jdbc_orders o
        INNER JOIN jdbc_users u ON o.user_id = u.id
        INNER JOIN jdbc_products p ON o.product_id = p.id
        WHERE u.age IS NOT NULL
        LIMIT 5
    """).collect()
    assert len(result3) > 0


# ---------------------------------------------------------------------------
# Complex filters (OR, IN, LIKE)
# ---------------------------------------------------------------------------


def test_complex_filters(spark, jdbc_opts):
    df = spark.read.format("jdbc").option("dbtable", "users").options(**jdbc_opts).load()

    result = df.filter("age = 30 OR age = 35").collect()
    assert len(result) >= 2  # noqa: PLR2004

    result = df.filter("age IN (25, 30, 35)").collect()
    assert len(result) >= 3  # noqa: PLR2004

    result = df.filter("email LIKE '%example.com'").collect()
    assert len(result) > 10  # noqa: PLR2004

    result = df.filter("NOT (age > 35)").collect()
    assert len(result) > 0


# ---------------------------------------------------------------------------
# Partition with NULLs in non-partition column
# ---------------------------------------------------------------------------


def test_partition_with_nulls(spark, jdbc_opts):
    df = (
        spark.read.format("jdbc")
        .option("dbtable", "users")
        .option("partitionColumn", "id")
        .option("lowerBound", "1")
        .option("upperBound", "15")
        .option("numPartitions", "3")
        .options(**jdbc_opts)
        .load()
    )
    assert df.count() == 15  # noqa: PLR2004
    assert df.filter("age IS NULL").count() >= 1


# ---------------------------------------------------------------------------
# Filter value SQL injection protection
# ---------------------------------------------------------------------------


def test_sql_injection_filter_value(spark, jdbc_opts):
    from pyspark.sql.functions import col

    orders_before = spark.read.format("jdbc").option("dbtable", "orders").options(**jdbc_opts).load().count()
    assert orders_before == 8  # noqa: PLR2004

    injection_payload = "'; DROP TABLE orders; --"
    users_df = spark.read.format("jdbc").option("dbtable", "users").options(**jdbc_opts).load()
    result = users_df.filter(col("name") == injection_payload).collect()
    assert len(result) == 0

    orders_after = spark.read.format("jdbc").option("dbtable", "orders").options(**jdbc_opts).load().count()
    assert orders_after == 8  # noqa: PLR2004


# ---------------------------------------------------------------------------
# special_chars table — reading rows with special characters
# ---------------------------------------------------------------------------


def test_special_chars_table(spark, jdbc_opts):
    df = spark.read.format("jdbc").option("dbtable", "special_chars").options(**jdbc_opts).load()
    rows = df.collect()
    assert len(rows) == 5  # noqa: PLR2004
    names = {r.name for r in rows}
    assert "Normal Name" in names
    assert "O'Reilly" in names
    assert 'Quote"Test' in names
    assert any("\t" in n for n in names)
    assert any("\\" in n for n in names)


# ---------------------------------------------------------------------------
# lowerBound > upperBound raises ValueError
# ---------------------------------------------------------------------------


def test_lower_bound_gt_upper_bound(spark, jdbc_opts):
    with pytest.raises(Exception, match=r"lowerbound|lowerBound"):
        spark.read.format("jdbc").option("dbtable", "users").option("partitionColumn", "id").option(
            "lowerBound", "100"
        ).option("upperBound", "10").option("numPartitions", "2").options(**jdbc_opts).load().collect()


# ---------------------------------------------------------------------------
# lowerBound == upperBound raises ValueError
# ---------------------------------------------------------------------------


def test_lower_bound_eq_upper_bound(spark, jdbc_opts):
    with pytest.raises(Exception, match=r"lowerbound|lowerBound"):
        spark.read.format("jdbc").option("dbtable", "users").option("partitionColumn", "id").option(
            "lowerBound", "10"
        ).option("upperBound", "10").option("numPartitions", "2").options(**jdbc_opts).load().collect()


# ---------------------------------------------------------------------------
# Non-integer lowerBound/upperBound raises ValueError
# ---------------------------------------------------------------------------


def test_non_integer_bounds(spark, jdbc_opts):
    with pytest.raises(Exception, match="integer"):
        spark.read.format("jdbc").option("dbtable", "users").option("partitionColumn", "id").option(
            "lowerBound", "1.5"
        ).option("upperBound", "10").option("numPartitions", "2").options(**jdbc_opts).load().collect()


# ---------------------------------------------------------------------------
# Empty dbtable raises ValueError
# ---------------------------------------------------------------------------


def test_empty_dbtable(spark, jdbc_opts):
    with pytest.raises(Exception, match="dbtable"):
        spark.read.format("jdbc").option("dbtable", "").options(**jdbc_opts).load().collect()


# ---------------------------------------------------------------------------
# Boolean filter pushdown
# ---------------------------------------------------------------------------


def test_boolean_filter_pushdown(spark, jdbc_opts):
    df = spark.read.format("jdbc").option("dbtable", "users").options(**jdbc_opts).load().filter("active = true")
    rows = df.collect()
    assert len(rows) == 12  # noqa: PLR2004
    assert all(r.active for r in rows)


# ---------------------------------------------------------------------------
# Rows outside [lowerBound, upperBound] are included
# ---------------------------------------------------------------------------


def test_rows_outside_bounds_included(spark, jdbc_opts):
    df_partitioned = (
        spark.read.format("jdbc")
        .option("dbtable", "large_table")
        .option("partitionColumn", "id")
        .option("lowerBound", "1000")
        .option("upperBound", "9000")
        .option("numPartitions", "4")
        .options(**jdbc_opts)
        .load()
    )
    df_full = spark.read.format("jdbc").option("dbtable", "large_table").options(**jdbc_opts).load()
    assert df_partitioned.count() == df_full.count()


# ---------------------------------------------------------------------------
# customSchema unknown column is silently ignored
# ---------------------------------------------------------------------------


def test_custom_schema_unknown_column(spark, jdbc_opts):
    df = (
        spark.read.format("jdbc")
        .option("dbtable", "users")
        .option("customSchema", "nonexistent_col BIGINT, age BIGINT")
        .options(**jdbc_opts)
        .load()
    )
    schema_map = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert "bigint" in schema_map.get("age", "")
    assert "nonexistent_col" not in schema_map


# ---------------------------------------------------------------------------
# customSchema column name matching is case-insensitive
# ---------------------------------------------------------------------------


def test_custom_schema_case_insensitive(spark, jdbc_opts):
    df = (
        spark.read.format("jdbc")
        .option("dbtable", "users")
        .option("customSchema", "AGE BIGINT, NAME STRING")
        .options(**jdbc_opts)
        .load()
    )
    schema_map = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert "bigint" in schema_map.get("age", "")
    assert "string" in schema_map.get("name", "") or "utf8" in schema_map.get("name", "")


# ---------------------------------------------------------------------------
# Unit tests — no Spark or DB required
# ---------------------------------------------------------------------------


def test_filter_to_sql_unit():
    from pyspark.sql.datasource import (
        EqualTo,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual,
    )

    from pysail.spark.datasource.jdbc import _filter_to_sql

    cases = [
        (EqualTo(("age",), 28), '"age" = 28'),
        (GreaterThan(("score",), 9.0), '"score" > 9.0'),
        (GreaterThanOrEqual(("id",), 1), '"id" >= 1'),
        (LessThan(("age",), 40), '"age" < 40'),
        (LessThanOrEqual(("age",), 35), '"age" <= 35'),
        (EqualTo(("name",), "Alice"), "\"name\" = 'Alice'"),
        (EqualTo(("name",), "O'Reilly"), "\"name\" = 'O''Reilly'"),
        (EqualTo(("active",), True), '"active" = TRUE'),
        (EqualTo(("active",), False), '"active" = FALSE'),
    ]

    for f, expected in cases:
        result = _filter_to_sql(f)
        assert result == expected, f"_filter_to_sql({f!r}) = {result!r}, expected {expected!r}"


def test_jdbc_url_to_dsn_unit():
    from pysail.spark.datasource.jdbc import _jdbc_url_to_dsn

    assert _jdbc_url_to_dsn("jdbc:postgresql://localhost:5432/db", None, None) == "postgresql://localhost:5432/db"
    assert (
        _jdbc_url_to_dsn("jdbc:postgresql://localhost:5432/db", "alice", None) == "postgresql://alice@localhost:5432/db"
    )
    assert (
        _jdbc_url_to_dsn("jdbc:postgresql://localhost:5432/db", "alice", "secret")
        == "postgresql://alice:secret@localhost:5432/db"
    )

    result = _jdbc_url_to_dsn("jdbc:postgresql://h:5432/db", "u", "p@ss/w0rd")
    assert "p%40ss%2Fw0rd" in result

    with pytest.raises(ValueError, match="Invalid JDBC URL"):
        _jdbc_url_to_dsn("postgresql://localhost/db", None, None)


def test_parse_custom_schema_unit():
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import _parse_custom_schema

    result = _parse_custom_schema("id BIGINT, name STRING, score DOUBLE, active BOOLEAN")
    assert result["id"] == pa.int64()
    assert result["name"] == pa.large_utf8()
    assert result["score"] == pa.float64()
    assert result["active"] == pa.bool_()

    result = _parse_custom_schema("price DECIMAL(10,2)")
    assert result["price"] == pa.decimal128(10, 2)

    result = _parse_custom_schema("MyCol INTEGER")
    assert "mycol" in result
    assert result["mycol"] == pa.int32()

    assert _parse_custom_schema("") == {}


def test_lit_unit():
    import datetime as dt

    from pyspark.sql.datasource import EqualTo

    from pysail.spark.datasource.jdbc import _filter_to_sql

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


def test_quote_identifier_unit():
    from pysail.spark.datasource.jdbc import _quote_identifier

    assert _quote_identifier("age") == '"age"'
    assert _quote_identifier("my col") == '"my col"'
    assert _quote_identifier('col"name') == '"col""name"'
    assert _quote_identifier("") == '""'
