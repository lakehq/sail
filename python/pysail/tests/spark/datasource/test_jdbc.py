"""Integration tests for the JDBC data source using testcontainers.

A PostgreSQL container is started once per session and torn down at the end.
All tests run against the container using the Sail Spark Connect server.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from testcontainers.postgres import PostgresContainer

from pysail.testing.spark.utils.common import pyspark_version

pytestmark = pytest.mark.integration

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


# ===========================================================================
# Writer integration tests — require Spark + Postgres container
# ===========================================================================


@pytest.fixture(scope="module")
def pg_dsn(pg_container):
    """Return a raw psycopg DSN for the test container."""
    host = pg_container.get_container_host_ip()
    port = pg_container.get_exposed_port(5432)
    return f"postgresql://{_PG_USER}:{_PG_PASSWORD}@{host}:{port}/{_PG_DB}"


@pytest.fixture
def write_table(pg_dsn, request):
    """Create an empty write-target table and drop it after the test.

    The table name is passed via ``request.param``:
        @pytest.mark.parametrize("write_table", ["my_table"], indirect=True)

    Column layout: id INTEGER, name TEXT, score DOUBLE PRECISION
    """
    import psycopg

    table = request.param
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id INTEGER, name TEXT, score DOUBLE PRECISION)')
    yield table
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')


def _read_pg_table(spark, jdbc_opts, table: str):
    """Helper: read a table back via the JDBC data source."""
    return spark.read.format("jdbc").option("dbtable", table).options(**jdbc_opts).load()


@pytest.mark.parametrize("write_table", ["wt_append_basic"], indirect=True)
def test_write_append_basic(spark, jdbc_opts, write_table):
    """Append-mode write round-trips rows correctly."""
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("score", DoubleType()),
        ]
    )
    data = [(1, "Alice", 9.5), (2, "Bob", 7.2), (3, "Charlie", 8.8)]
    df = spark.createDataFrame(data, schema)

    df.write.format("jdbc").option("dbtable", write_table).options(**jdbc_opts).mode("append").save()

    result = _read_pg_table(spark, jdbc_opts, write_table)
    assert result.count() == 3  # noqa: PLR2004
    names = {r.name for r in result.collect()}
    assert names == {"Alice", "Bob", "Charlie"}


@pytest.mark.parametrize("write_table", ["wt_overwrite"], indirect=True)
def test_write_overwrite_replaces(spark, jdbc_opts, write_table):
    """Overwrite-mode write replaces all existing rows."""
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("score", DoubleType()),
        ]
    )
    first = spark.createDataFrame([(1, "Alice", 9.5), (2, "Bob", 7.2)], schema)
    second = spark.createDataFrame([(3, "Charlie", 8.8)], schema)

    first.write.format("jdbc").option("dbtable", write_table).options(**jdbc_opts).mode("append").save()
    assert _read_pg_table(spark, jdbc_opts, write_table).count() == 2  # noqa: PLR2004

    second.write.format("jdbc").option("dbtable", write_table).options(**jdbc_opts).mode("overwrite").save()
    result = _read_pg_table(spark, jdbc_opts, write_table)
    assert result.count() == 1
    assert result.collect()[0].name == "Charlie"


@pytest.mark.parametrize("write_table", ["wt_empty_df"], indirect=True)
def test_write_empty_df(spark, jdbc_opts, write_table):
    """Writing an empty DataFrame leaves the table empty."""
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("score", DoubleType()),
        ]
    )
    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("jdbc").option("dbtable", write_table).options(**jdbc_opts).mode("append").save()

    assert _read_pg_table(spark, jdbc_opts, write_table).count() == 0


@pytest.mark.parametrize("write_table", ["wt_nulls"], indirect=True)
def test_write_null_values(spark, jdbc_opts, write_table):
    """Rows with NULL values survive the write round-trip."""
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("score", DoubleType()),
        ]
    )
    data = [(1, None, 9.5), (2, "Bob", None)]
    df = spark.createDataFrame(data, schema)
    df.write.format("jdbc").option("dbtable", write_table).options(**jdbc_opts).mode("append").save()

    result = _read_pg_table(spark, jdbc_opts, write_table).collect()
    assert len(result) == 2  # noqa: PLR2004
    assert any(r.name is None for r in result)
    assert any(r.score is None for r in result)


@pytest.mark.parametrize("write_table", ["wt_append_twice"], indirect=True)
def test_write_append_twice(spark, jdbc_opts, write_table):
    """Two consecutive appends accumulate rows."""
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("score", DoubleType()),
        ]
    )
    df = spark.createDataFrame([(1, "Alice", 9.5)], schema)
    df.write.format("jdbc").option("dbtable", write_table).options(**jdbc_opts).mode("append").save()
    df.write.format("jdbc").option("dbtable", write_table).options(**jdbc_opts).mode("append").save()

    assert _read_pg_table(spark, jdbc_opts, write_table).count() == 2  # noqa: PLR2004


# ===========================================================================
# Overwrite-mode integration tests
# ===========================================================================


@pytest.mark.parametrize("write_table", ["wt_atomic_overwrite"], indirect=True)
def test_write_overwrite_atomic_replaces(spark, jdbc_opts, write_table):
    """atomic overwrite replaces all existing rows atomically."""
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("score", DoubleType()),
        ]
    )
    first = spark.createDataFrame([(1, "Alice", 9.5), (2, "Bob", 7.2)], schema)
    second = spark.createDataFrame([(3, "Charlie", 8.8)], schema)

    first.write.format("jdbc").option("dbtable", write_table).options(**jdbc_opts).mode("append").save()
    assert _read_pg_table(spark, jdbc_opts, write_table).count() == 2  # noqa: PLR2004

    second.write.format("jdbc").option("dbtable", write_table).option("overwriteMode", "atomic").options(
        **jdbc_opts
    ).mode("overwrite").save()

    result = _read_pg_table(spark, jdbc_opts, write_table)
    assert result.count() == 1
    assert result.collect()[0].name == "Charlie"


@pytest.mark.parametrize("write_table", ["wt_truncate_overwrite"], indirect=True)
def test_write_overwrite_truncate_replaces(spark, jdbc_opts, write_table):
    """truncate overwrite replaces all existing rows."""
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("score", DoubleType()),
        ]
    )
    first = spark.createDataFrame([(1, "Alice", 9.5), (2, "Bob", 7.2)], schema)
    second = spark.createDataFrame([(3, "Charlie", 8.8)], schema)

    first.write.format("jdbc").option("dbtable", write_table).options(**jdbc_opts).mode("append").save()
    assert _read_pg_table(spark, jdbc_opts, write_table).count() == 2  # noqa: PLR2004

    second.write.format("jdbc").option("dbtable", write_table).option("overwriteMode", "truncate").options(
        **jdbc_opts
    ).mode("overwrite").save()

    result = _read_pg_table(spark, jdbc_opts, write_table)
    assert result.count() == 1
    assert result.collect()[0].name == "Charlie"


@pytest.mark.parametrize("write_table", ["wt_atomic_empty"], indirect=True)
def test_write_atomic_empty_df(spark, jdbc_opts, write_table):
    """atomic overwrite with empty df leaves table empty."""
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("score", DoubleType()),
        ]
    )
    # Seed the table with one row first
    seed = spark.createDataFrame([(1, "Seed", 1.0)], schema)
    seed.write.format("jdbc").option("dbtable", write_table).options(**jdbc_opts).mode("append").save()
    assert _read_pg_table(spark, jdbc_opts, write_table).count() == 1

    # Overwrite with empty
    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("jdbc").option("dbtable", write_table).option("overwriteMode", "atomic").options(
        **jdbc_opts
    ).mode("overwrite").save()

    assert _read_pg_table(spark, jdbc_opts, write_table).count() == 0


@pytest.mark.parametrize("write_table", ["wt_atomic_nulls"], indirect=True)
def test_write_atomic_null_values(spark, jdbc_opts, write_table):
    """atomic overwrite with nulls survives round-trip."""
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("score", DoubleType()),
        ]
    )
    data = [(1, None, 9.5), (2, "Bob", None)]
    df = spark.createDataFrame(data, schema)
    df.write.format("jdbc").option("dbtable", write_table).option("overwriteMode", "atomic").options(**jdbc_opts).mode(
        "overwrite"
    ).save()

    result = _read_pg_table(spark, jdbc_opts, write_table).collect()
    assert len(result) == 2  # noqa: PLR2004
    assert any(r.name is None for r in result)
    assert any(r.score is None for r in result)


@pytest.mark.parametrize("write_table", ["wt_concurrent_atomic"], indirect=True)
def test_concurrent_atomic_overwrites_no_corruption(pg_dsn, write_table):
    """Two concurrent atomic overwrites to the same table do not mix data.

    This test verifies the run_id fix: each PgWriteEngine generates its own
    run_id so staging table names are distinct even when targeting the same table.
    Both jobs write, commit sequentially; last commit wins. The winner's data
    is intact — no mixing of rows from both jobs.
    """
    import threading

    import psycopg
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import PgWriteEngine, _staging_name_atomic

    dsn = pg_dsn

    # Two engines targeting the same table — must get distinct run_ids and staging names
    engine_a = PgWriteEngine(dsn=dsn, dbtable=write_table, overwrite_mode="atomic")
    engine_b = PgWriteEngine(dsn=dsn, dbtable=write_table, overwrite_mode="atomic")

    # Distinct run_ids → distinct staging table names (the concurrency-collision fix)
    assert engine_a.run_id != engine_b.run_id
    staging_a = _staging_name_atomic(write_table, engine_a.run_id)
    staging_b = _staging_name_atomic(write_table, engine_b.run_id)
    assert staging_a != staging_b

    batch_a = pa.record_batch(
        {"id": pa.array([10], type=pa.int32()), "name": pa.array(["JobA"]), "score": pa.array([1.0])},
    )
    batch_b = pa.record_batch(
        {"id": pa.array([20], type=pa.int32()), "name": pa.array(["JobB"]), "score": pa.array([2.0])},
    )

    results_a: list = []
    results_b: list = []
    errors: list = []

    def run_a():
        try:
            results_a.append(engine_a.write_partition(0, [batch_a]))
        except Exception as exc:  # noqa: BLE001
            errors.append(("a-write", exc))

    def run_b():
        try:
            results_b.append(engine_b.write_partition(0, [batch_b]))
        except Exception as exc:  # noqa: BLE001
            errors.append(("b-write", exc))

    # Both jobs write their partitions concurrently (each to its own staging table)
    t_a = threading.Thread(target=run_a)
    t_b = threading.Thread(target=run_b)
    t_a.start()
    t_b.start()
    t_a.join(timeout=30)
    t_b.join(timeout=30)

    assert not errors, f"concurrent write phase errors: {errors}"
    assert len(results_a) == 1
    assert len(results_b) == 1

    # Both staging tables exist and have the correct rows (no mixing yet)
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'SELECT name FROM "{staging_a}"')  # noqa: S608
        rows_a = {r[0] for r in cur.fetchall()}
        cur.execute(f'SELECT name FROM "{staging_b}"')  # noqa: S608
        rows_b = {r[0] for r in cur.fetchall()}

    assert rows_a == {"JobA"}, f"staging_a has wrong rows: {rows_a}"
    assert rows_b == {"JobB"}, f"staging_b has wrong rows: {rows_b}"

    # Commit job A first, then job B — last commit (B) wins
    engine_a.commit(results_a)
    engine_b.commit(results_b)

    # After both commits: only JobB rows present (B's DROP+RENAME ran last)
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'SELECT name FROM "{write_table}"')  # noqa: S608
        final_rows = {r[0] for r in cur.fetchall()}

    # No mixing: only one job's data survives
    assert final_rows <= {"JobA", "JobB"}, f"mixed data detected: {final_rows}"
    assert len(final_rows) == 1, f"expected exactly one job's data, got: {final_rows}"


@pytest.mark.parametrize("write_table", ["wt_abort_atomic"], indirect=True)
def test_write_atomic_abort_cleanup(spark, jdbc_opts, pg_dsn, write_table):  # noqa: ARG001
    """abort() drops the atomic staging table."""
    import psycopg
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import PartitionResult, PgWriteEngine, _staging_name_atomic  # noqa: F401

    dsn = pg_dsn
    engine = PgWriteEngine(dsn=dsn, dbtable=write_table, overwrite_mode="atomic")

    # Write one partition (creates staging table)
    batch = pa.record_batch(
        {"id": pa.array([1], type=pa.int32()), "name": pa.array(["X"]), "score": pa.array([1.0])},
    )
    result = engine.write_partition(0, [batch])
    assert result.staging_table == _staging_name_atomic(write_table, engine.run_id)

    # Verify staging table exists
    staging_name = _staging_name_atomic(write_table, engine.run_id)
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = %s",
            (staging_name,),
        )
        assert cur.fetchone()[0] == 1  # type: ignore[index]

    # Abort should drop it
    engine.abort([result])

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = %s",
            (staging_name,),
        )
        assert cur.fetchone()[0] == 0  # type: ignore[index]


# ===========================================================================
# Schema-qualified write targets (dbtable = "schema.table")
# ===========================================================================


@pytest.fixture
def qualified_write_table(pg_dsn):
    """Create ``wsch.events`` in a non-default schema; drop the schema afterwards.

    Columns: id INTEGER, name TEXT, score DOUBLE PRECISION.
    """
    import psycopg

    schema, table = "wsch", "events"
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
        cur.execute(f'CREATE TABLE "{schema}"."{table}" (id INTEGER, name TEXT, score DOUBLE PRECISION)')
    yield f"{schema}.{table}", schema, table
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def _count_in_schema(pg_dsn, schema: str, table: str) -> int:
    import psycopg

    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')  # noqa: S608
        return cur.fetchone()[0]  # type: ignore[index]


def _public_table_exists(pg_dsn, table: str) -> bool:
    import psycopg

    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s",
            (table,),
        )
        return cur.fetchone()[0] > 0  # type: ignore[index]


def _schema_df():
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    return StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("score", DoubleType()),
        ]
    )


def test_write_append_schema_qualified(spark, jdbc_opts, pg_dsn, qualified_write_table):
    """Append to a schema-qualified dbtable lands rows in the right schema."""
    dbtable, schema, table = qualified_write_table
    df = spark.createDataFrame([(1, "Alice", 9.5), (2, "Bob", 7.2)], _schema_df())

    df.write.format("jdbc").option("dbtable", dbtable).options(**jdbc_opts).mode("append").save()

    assert _count_in_schema(pg_dsn, schema, table) == 2  # noqa: PLR2004
    assert not _public_table_exists(pg_dsn, table), "rows leaked into public schema"


def test_write_overwrite_atomic_schema_qualified(spark, jdbc_opts, pg_dsn, qualified_write_table):
    """Atomic overwrite of a schema-qualified table replaces rows in-place.

    Guards that the staging table is created in the target's schema, so the
    post-rename target stays there and the original table is not replaced by a
    public-schema copy.
    """
    dbtable, schema, table = qualified_write_table
    first = spark.createDataFrame([(1, "Alice", 9.5), (2, "Bob", 7.2)], _schema_df())
    second = spark.createDataFrame([(3, "Charlie", 8.8)], _schema_df())

    first.write.format("jdbc").option("dbtable", dbtable).options(**jdbc_opts).mode("append").save()
    assert _count_in_schema(pg_dsn, schema, table) == 2  # noqa: PLR2004

    second.write.format("jdbc").option("dbtable", dbtable).option("overwriteMode", "atomic").options(**jdbc_opts).mode(
        "overwrite"
    ).save()

    assert _count_in_schema(pg_dsn, schema, table) == 1
    assert not _public_table_exists(pg_dsn, table), "atomic overwrite created a public-schema copy"
    assert _read_pg_table(spark, jdbc_opts, dbtable).collect()[0].name == "Charlie"


def test_write_overwrite_truncate_schema_qualified(spark, jdbc_opts, pg_dsn, qualified_write_table):
    """Truncate overwrite of a schema-qualified table replaces rows in-place."""
    dbtable, schema, table = qualified_write_table
    first = spark.createDataFrame([(1, "Alice", 9.5), (2, "Bob", 7.2)], _schema_df())
    second = spark.createDataFrame([(3, "Charlie", 8.8)], _schema_df())

    first.write.format("jdbc").option("dbtable", dbtable).options(**jdbc_opts).mode("append").save()
    assert _count_in_schema(pg_dsn, schema, table) == 2  # noqa: PLR2004

    second.write.format("jdbc").option("dbtable", dbtable).option("overwriteMode", "truncate").options(
        **jdbc_opts
    ).mode("overwrite").save()

    assert _count_in_schema(pg_dsn, schema, table) == 1
    assert _read_pg_table(spark, jdbc_opts, dbtable).collect()[0].name == "Charlie"
