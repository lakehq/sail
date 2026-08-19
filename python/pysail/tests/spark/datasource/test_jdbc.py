"""Integration tests for the JDBC data source using testcontainers.

A PostgreSQL container is started once per session and torn down at the end.
All tests run against the container using the Sail Spark Connect server.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from testcontainers.community.postgres import PostgresContainer

from pysail.testing.spark.jdbc_oracle import (
    SPARK_TYPE_MATRIX_SELECT_EXPRS,
    native_spark_4_1_2_python,
    run_native_jdbc_write,
)
from pysail.testing.spark.utils.common import pyspark_version

pytestmark = pytest.mark.integration

try:
    from pyspark.sql.datasource import DataSourceArrowWriter  # noqa: F401  (Spark 4.0+)
except ImportError:
    pytest.skip("JDBC data source requires the PySpark Python DataSource API (4.0+)", allow_module_level=True)

# Sail's engine-level filter pushdown (crates/sail-data-source/.../filter.rs) unconditionally
# constructs `pyspark.sql.datasource` filter class instances (EqualTo, IsNotNull, etc.) for any
# `.filter()`/join predicate against a Python data source, regardless of what the data source's
# own `pushFilters()` does with them. Those classes are 4.1-only, so any test that filters a JDBC
# DataFrame fails on 4.0 with an AttributeError raised from Sail's Rust side — this is a Sail
# engine limitation, not something `jdbc.py` can guard against.
_requires_pushdown_classes = pytest.mark.skipif(
    pyspark_version() < (4, 1),
    reason="Sail's engine-level filter pushdown requires PySpark 4.1+ filter classes",
)

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


@_requires_pushdown_classes
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


@_requires_pushdown_classes
def test_empty_table(spark, jdbc_opts):
    df = spark.read.format("jdbc").option("dbtable", "empty_table").options(**jdbc_opts).load()
    assert df.count() == 0
    assert df.filter("id > 0").collect() == []


# ---------------------------------------------------------------------------
# Large dataset (10K rows)
# ---------------------------------------------------------------------------


@_requires_pushdown_classes
def test_large_dataset(spark, jdbc_opts):
    df = spark.read.format("jdbc").option("dbtable", "large_table").options(**jdbc_opts).load()
    assert df.count() == 10000  # noqa: PLR2004
    assert df.filter("id > 5000").count() == 5000  # noqa: PLR2004


# ---------------------------------------------------------------------------
# Unicode strings
# ---------------------------------------------------------------------------


@_requires_pushdown_classes
def test_unicode_strings(spark, jdbc_opts):
    df = spark.read.format("jdbc").option("dbtable", "users").options(**jdbc_opts).load()
    assert len(df.filter("name = '张伟'").collect()) == 1
    assert len(df.filter("name = 'José García'").collect()) == 1
    assert len(df.filter("name = 'محمد علي'").collect()) == 1
    assert len(df.filter("name LIKE '%😀%'").collect()) == 1


# ---------------------------------------------------------------------------
# Data types coverage
# ---------------------------------------------------------------------------


@_requires_pushdown_classes
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


@_requires_pushdown_classes
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


@_requires_pushdown_classes
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


@_requires_pushdown_classes
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


@_requires_pushdown_classes
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


def _managed_table(pg_dsn, table, ddl):
    """Create *table* via *ddl*, yield its name, and drop it afterwards.

    Shared scaffold for the write-target fixtures; ``ddl`` is the only thing that varies.
    """
    import psycopg

    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(ddl)
    try:
        yield table
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')


@pytest.fixture
def write_table(pg_dsn, request):
    """Empty write target (``id INTEGER, name TEXT, score DOUBLE PRECISION``).

    Name via ``request.param``: ``@pytest.mark.parametrize("write_table", ["my_table"], indirect=True)``.
    """
    yield from _managed_table(
        pg_dsn, request.param, f'CREATE TABLE "{request.param}" (id INTEGER, name TEXT, score DOUBLE PRECISION)'
    )


@pytest.fixture
def serial_write_table(pg_dsn, request):
    """Write target with a SERIAL primary key.

    Exercises the atomic swap's owned-sequence handling: LIKE INCLUDING ALL ties the
    staging's default to the target's sequence, which must be detached before DROP and
    re-synced onto the swapped-in table.
    """
    yield from _managed_table(
        pg_dsn, request.param, f'CREATE TABLE "{request.param}" (id SERIAL PRIMARY KEY, name TEXT)'
    )


def _read_pg_table(spark, jdbc_opts, table: str):
    """Helper: read a table back via the JDBC data source."""
    return spark.read.format("jdbc").option("dbtable", table).options(**jdbc_opts).load()


def test_postgres_server_version_is_pinned(pg_dsn):
    import psycopg

    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("SHOW server_version")
        assert cur.fetchone()[0].startswith("16.")


def _pg_snapshot(pg_dsn, table):
    import psycopg

    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name, data_type, is_nullable, identity_generation "
            "FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position",
            (table,),
        )
        columns = cur.fetchall()
        cur.execute(
            "SELECT a.attname FROM pg_index i "
            "JOIN pg_class t ON t.oid = i.indrelid "
            "JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey) "
            "WHERE t.oid = to_regclass(%s) ORDER BY a.attname",
            (table,),
        )
        indexed_columns = cur.fetchall()
        cur.execute(f'SELECT * FROM "{table}"')  # noqa: S608
        return columns, indexed_columns, sorted(cur.fetchall(), key=repr)


def test_native_spark_oracle_not_silently_skipped():
    """CI guard: when the environment demands the oracle, differentials must not skip.

    The differential tests skip when SAIL_SPARK_4_1_2_PYTHON is unset. If that env
    var ever disappears from the CI job, every differential would silently skip and
    the job would stay green; this sentinel fails instead.
    """
    import os

    if os.environ.get("SAIL_JDBC_REQUIRE_ORACLE") != "1":
        pytest.skip("SAIL_JDBC_REQUIRE_ORACLE is not set")
    python = native_spark_4_1_2_python()
    assert python is not None, "SAIL_SPARK_4_1_2_PYTHON must point at the Spark 4.1.2 oracle interpreter"
    assert python.exists(), f"oracle interpreter missing: {python}"


@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_postgres_created_types_match_native_spark_4_1_2(spark, jdbc_opts, pg_dsn):
    """Create-table type mapping parity across Spark's PostgreSQL write matrix.

    Both engines evaluate the same SELECT expressions and auto-create their target,
    so the created column types (PostgresDialect.getJDBCType + common fallback) and
    the round-tripped values must be identical.
    """
    import psycopg

    exprs = [*SPARK_TYPE_MATRIX_SELECT_EXPRS, "ARRAY(1, 2) AS c_array"]
    tables = ("wt_pg_native_type_matrix", "wt_pg_sail_type_matrix")
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        for table in tables:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
    try:
        run_native_jdbc_write(
            dialect="postgresql",
            jdbc_url=jdbc_opts["url"],
            dbtable=tables[0],
            user=jdbc_opts["user"],
            password=jdbc_opts["password"],
            schema_json=None,
            rows=[],
            mode="append",
            select_exprs=exprs,
        )
        spark.range(1).selectExpr(*exprs).write.format("jdbc").option("dbtable", tables[1]).options(**jdbc_opts).mode(
            "append"
        ).save()
        with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
            snapshots = []
            for table in tables:
                cur.execute(
                    "SELECT column_name, data_type, udt_name, is_nullable, numeric_precision, numeric_scale "
                    "FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position",
                    (table,),
                )
                columns = cur.fetchall()
                cur.execute(f'SELECT * FROM "{table}"')  # noqa: S608
                snapshots.append((columns, cur.fetchall()))
        assert snapshots[0] == snapshots[1]
        assert len(snapshots[0][0]) == len(exprs)
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            for table in tables:
                cur.execute(f'DROP TABLE IF EXISTS "{table}"')


@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_postgres_application_name_matches_native_spark_4_1_2(spark, jdbc_opts, pg_dsn):
    """pgJDBC's ApplicationName property maps to libpq application_name with equal effect.

    The target's ``app`` column defaults to ``current_setting('application_name')``,
    so every inserted row records the connection property the writer actually applied.
    Appending a subset of the target's columns also exercises name-based resolution
    against a wider existing table on both engines.
    """
    import psycopg
    from pyspark.sql.types import IntegerType, StructField, StructType

    table = "wt_pg_app_name_probe"
    schema = StructType([StructField("id", IntegerType())])
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f"CREATE TABLE \"{table}\" (id integer, app text DEFAULT current_setting('application_name'))")
    try:
        run_native_jdbc_write(
            dialect="postgresql",
            jdbc_url=jdbc_opts["url"],
            dbtable=table,
            user=jdbc_opts["user"],
            password=jdbc_opts["password"],
            schema_json=schema.jsonValue(),
            rows=[[1]],
            mode="append",
            options={"ApplicationName": "sail-parity-probe"},
        )
        spark.createDataFrame([(2,)], schema).write.format("jdbc").option("dbtable", table).option(
            "ApplicationName", "sail-parity-probe"
        ).options(**jdbc_opts).mode("append").save()
        with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
            cur.execute(f'SELECT id, app FROM "{table}" ORDER BY id')  # noqa: S608
            assert cur.fetchall() == [(1, "sail-parity-probe"), (2, "sail-parity-probe")]
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')


@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_postgres_write_matches_native_spark_4_1_2(spark, jdbc_opts, pg_dsn):
    import psycopg
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    native_table = "wt_pg_native_oracle"
    sail_table = "wt_pg_sail_oracle"
    schema = StructType(
        [StructField("id", IntegerType()), StructField("name", StringType()), StructField("score", DoubleType())]
    )
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{native_table}"')
        cur.execute(f'DROP TABLE IF EXISTS "{sail_table}"')
    try:
        rows = [[1, "Alice", 9.5], [2, "Bob", 7.2]]
        run_native_jdbc_write(
            dialect="postgresql",
            jdbc_url=jdbc_opts["url"],
            dbtable=native_table,
            user=jdbc_opts["user"],
            password=jdbc_opts["password"],
            schema_json=schema.jsonValue(),
            rows=rows,
            mode="append",
        )
        spark.createDataFrame([tuple(row) for row in rows], schema).write.format("jdbc").option(
            "dbtable", sail_table
        ).options(**jdbc_opts).mode("append").save()
        with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
            snapshots = []
            for table in (native_table, sail_table):
                cur.execute(
                    "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
                    "WHERE table_name = %s ORDER BY ordinal_position",
                    (table,),
                )
                columns = cur.fetchall()
                cur.execute(f'SELECT id, name, score FROM "{table}" ORDER BY id')  # noqa: S608
                snapshots.append((columns, cur.fetchall()))
            assert snapshots[0] == snapshots[1]
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{native_table}"')
            cur.execute(f'DROP TABLE IF EXISTS "{sail_table}"')


@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_postgres_dbtable_whitespace_matches_native_spark_4_1_2(spark, jdbc_opts, pg_dsn):
    """Spark's JDBCOptions trims dbtable; spaces are not part of the identifier."""
    import psycopg
    from pyspark.sql.types import IntegerType, StructField, StructType

    tables = ("wt_pg_native_trimmed", "wt_pg_sail_trimmed")
    schema = StructType([StructField("id", IntegerType())])
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        for table in tables:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
    try:
        run_native_jdbc_write(
            dialect="postgresql",
            jdbc_url=jdbc_opts["url"],
            dbtable=f"  {tables[0]}  ",
            user=jdbc_opts["user"],
            password=jdbc_opts["password"],
            schema_json=schema.jsonValue(),
            rows=[[1]],
            mode="append",
        )
        spark.createDataFrame([(1,)], schema).write.format("jdbc").option("dbtable", f"  {tables[1]}  ").options(
            **jdbc_opts
        ).mode("append").save()
        with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
            for table in tables:
                cur.execute(f'SELECT id FROM "{table}"')  # noqa: S608
                assert cur.fetchall() == [(1,)]
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            for table in tables:
                cur.execute(f'DROP TABLE IF EXISTS "{table}"')


@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_postgres_create_options_match_native_spark_4_1_2(spark, jdbc_opts, pg_dsn):
    import psycopg
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    tables = ("wt_pg_native_create_options", "wt_pg_sail_create_options")
    schema = StructType([StructField("id", IntegerType()), StructField("label", StringType())])
    options = {
        "createTableColumnTypes": "LABEL VARCHAR(32)",
        "createTableOptions": "WITH (fillfactor=70)",
        "tableComment": "spark parity evidence",
        "isolationLevel": "SERIALIZABLE",
        "queryTimeout": "10",
    }
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        for table in tables:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
    try:
        run_native_jdbc_write(
            dialect="postgresql",
            jdbc_url=jdbc_opts["url"],
            dbtable=tables[0],
            user=jdbc_opts["user"],
            password=jdbc_opts["password"],
            schema_json=schema.jsonValue(),
            rows=[[1, "one"]],
            mode="append",
            options=options,
        )
        spark.createDataFrame([(1, "one")], schema).write.format("jdbc").option("dbtable", tables[1]).options(
            **jdbc_opts, **options
        ).mode("append").save()
        with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
            snapshots = []
            for table in tables:
                cur.execute(
                    "SELECT character_maximum_length FROM information_schema.columns "
                    "WHERE table_name = %s AND column_name = 'label'",
                    (table,),
                )
                length = cur.fetchone()[0]
                cur.execute("SELECT reloptions, obj_description(oid) FROM pg_class WHERE oid = %s::regclass", (table,))
                snapshots.append((length, *cur.fetchone()))
            assert snapshots[0] == snapshots[1] == (32, ["fillfactor=70"], "spark parity evidence")
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            for table in tables:
                cur.execute(f'DROP TABLE IF EXISTS "{table}"')


@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_postgres_isolation_and_timeout_match_native_spark_4_1_2(spark, jdbc_opts, pg_dsn):
    import psycopg
    from pyspark.sql.types import IntegerType, StructField, StructType

    schema = StructType([StructField("id", IntegerType())])
    isolation_tables = ("wt_pg_native_isolation", "wt_pg_sail_isolation")
    timeout_tables = ("wt_pg_native_timeout", "wt_pg_sail_timeout")
    all_tables = (*isolation_tables, *timeout_tables)
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "CREATE OR REPLACE FUNCTION wt_pg_record_isolation() RETURNS trigger LANGUAGE plpgsql AS "
            "$$ BEGIN NEW.level := current_setting('transaction_isolation'); RETURN NEW; END $$"
        )
        cur.execute(
            "CREATE OR REPLACE FUNCTION wt_pg_sleep() RETURNS trigger LANGUAGE plpgsql AS "
            "$$ BEGIN PERFORM pg_sleep(2); RETURN NEW; END $$"
        )
        for table in isolation_tables:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            cur.execute(f'CREATE TABLE "{table}" (id INTEGER, level TEXT)')
            cur.execute(
                f'CREATE TRIGGER "{table}_trigger" BEFORE INSERT ON "{table}" '  # noqa: S608
                "FOR EACH ROW EXECUTE FUNCTION wt_pg_record_isolation()"
            )
        for table in timeout_tables:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            cur.execute(f'CREATE TABLE "{table}" (id INTEGER)')
            cur.execute(
                f'CREATE TRIGGER "{table}_trigger" BEFORE INSERT ON "{table}" '  # noqa: S608
                "FOR EACH ROW EXECUTE FUNCTION wt_pg_sleep()"
            )
    try:
        run_native_jdbc_write(
            dialect="postgresql",
            jdbc_url=jdbc_opts["url"],
            dbtable=isolation_tables[0],
            user=jdbc_opts["user"],
            password=jdbc_opts["password"],
            schema_json=schema.jsonValue(),
            rows=[[1]],
            mode="append",
            options={"isolationLevel": "SERIALIZABLE"},
        )
        spark.createDataFrame([(1,)], schema).write.format("jdbc").option("dbtable", isolation_tables[1]).options(
            **jdbc_opts, isolationLevel="SERIALIZABLE"
        ).mode("append").save()
        for table, native in zip(timeout_tables, (True, False), strict=True):
            with pytest.raises(Exception) as failure:  # noqa: B017
                if native:
                    run_native_jdbc_write(
                        dialect="postgresql",
                        jdbc_url=jdbc_opts["url"],
                        dbtable=table,
                        user=jdbc_opts["user"],
                        password=jdbc_opts["password"],
                        schema_json=schema.jsonValue(),
                        rows=[[1]],
                        mode="append",
                        options={"queryTimeout": "1"},
                    )
                else:
                    spark.createDataFrame([(1,)], schema).write.format("jdbc").option("dbtable", table).options(
                        **jdbc_opts, queryTimeout="1"
                    ).mode("append").save()
            assert failure.value is not None
        with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
            cur.execute(
                f'SELECT level FROM "{isolation_tables[0]}" UNION ALL '  # noqa: S608
                f'SELECT level FROM "{isolation_tables[1]}" ORDER BY level'
            )
            assert cur.fetchall() == [("serializable",), ("serializable",)]
            for table in timeout_tables:
                cur.execute(f'SELECT count(*) FROM "{table}"')  # noqa: S608
                assert cur.fetchone()[0] == 0
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            for table in all_tables:
                cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            cur.execute("DROP FUNCTION IF EXISTS wt_pg_record_isolation()")
            cur.execute("DROP FUNCTION IF EXISTS wt_pg_sleep()")


@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_postgres_reordered_case_varied_append_matches_native_spark_4_1_2(spark, jdbc_opts, pg_dsn):
    import psycopg
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    native_table = "wt_pg_native_columns"
    sail_table = "wt_pg_sail_columns"
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        for table in (native_table, sail_table):
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            cur.execute(f'CREATE TABLE "{table}" ("ID" INTEGER, "DisplayName" TEXT)')
    try:
        schema = StructType([StructField("displayname", StringType()), StructField("id", IntegerType())])
        rows = [["Alice", 1], ["Bob", 2]]
        run_native_jdbc_write(
            dialect="postgresql",
            jdbc_url=jdbc_opts["url"],
            dbtable=native_table,
            user=jdbc_opts["user"],
            password=jdbc_opts["password"],
            schema_json=schema.jsonValue(),
            rows=rows,
            mode="append",
        )
        spark.createDataFrame([tuple(row) for row in rows], schema).write.format("jdbc").option(
            "dbtable", sail_table
        ).options(**jdbc_opts).mode("append").save()

        assert _pg_snapshot(pg_dsn, native_table) == _pg_snapshot(pg_dsn, sail_table)
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{native_table}"')
            cur.execute(f'DROP TABLE IF EXISTS "{sail_table}"')


@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_postgres_default_overwrite_schema_change_matches_native_spark_4_1_2(spark, jdbc_opts, pg_dsn):
    import psycopg
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    native_table = "wt_pg_native_schema"
    sail_table = "wt_pg_sail_schema"
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        for table in (native_table, sail_table):
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            cur.execute(f'CREATE TABLE "{table}" (old_id INTEGER, old_value TEXT)')
    try:
        schema = StructType([StructField("new_id", IntegerType()), StructField("label", StringType())])
        rows = [[1, "new"]]
        run_native_jdbc_write(
            dialect="postgresql",
            jdbc_url=jdbc_opts["url"],
            dbtable=native_table,
            user=jdbc_opts["user"],
            password=jdbc_opts["password"],
            schema_json=schema.jsonValue(),
            rows=rows,
            mode="overwrite",
        )
        spark.createDataFrame([tuple(row) for row in rows], schema).write.format("jdbc").option(
            "dbtable", sail_table
        ).options(**jdbc_opts).mode("overwrite").save()

        assert _pg_snapshot(pg_dsn, native_table) == _pg_snapshot(pg_dsn, sail_table)
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{native_table}"')
            cur.execute(f'DROP TABLE IF EXISTS "{sail_table}"')


@pytest.mark.parametrize("mode", [None, "error", "ignore"])
@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_postgres_existing_table_nonwriting_modes_match_native_spark_4_1_2(
    spark,
    jdbc_opts,
    pg_dsn,
    mode,
):
    import subprocess

    import psycopg
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    suffix = mode or "default"
    native_table = f"wt_pg_native_{suffix}"
    sail_table = f"wt_pg_sail_{suffix}"
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        for table in (native_table, sail_table):
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            cur.execute(f'CREATE TABLE "{table}" (id INTEGER, name TEXT)')
            cur.execute(f'INSERT INTO "{table}" VALUES (1, %s)', ("old",))
    try:
        schema = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
        rows = [[2, "new"]]

        def native_call():
            run_native_jdbc_write(
                dialect="postgresql",
                jdbc_url=jdbc_opts["url"],
                dbtable=native_table,
                user=jdbc_opts["user"],
                password=jdbc_opts["password"],
                schema_json=schema.jsonValue(),
                rows=rows,
                mode=mode,
                select_exprs=["raise_error('ignore evaluated input') AS id"] if mode == "ignore" else None,
            )

        def sail_call():
            df = (
                spark.range(1).selectExpr("raise_error('ignore evaluated input') AS id")
                if mode == "ignore"
                else spark.createDataFrame([tuple(row) for row in rows], schema)
            )
            (df.write.format("jdbc").option("dbtable", sail_table).options(**jdbc_opts).mode(mode).save())

        if mode in (None, "error"):
            with pytest.raises(subprocess.CalledProcessError):
                native_call()
            with pytest.raises(Exception, match="already exists"):
                sail_call()
        else:
            native_call()
            sail_call()

        assert _pg_snapshot(pg_dsn, native_table) == _pg_snapshot(pg_dsn, sail_table)
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{native_table}"')
            cur.execute(f'DROP TABLE IF EXISTS "{sail_table}"')


@pytest.mark.parametrize("mode", [None, "append", "overwrite", "ignore"])
@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_postgres_missing_table_save_modes_match_native_spark_4_1_2(
    spark,
    jdbc_opts,
    pg_dsn,
    mode,
):
    import psycopg

    suffix = mode or "default"
    native_table = f"wt_pg_native_missing_{suffix}"
    sail_table = f"wt_pg_sail_missing_{suffix}"
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{native_table}"')
        cur.execute(f'DROP TABLE IF EXISTS "{sail_table}"')
    try:
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        schema = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
        rows = [[1, "new"]]
        run_native_jdbc_write(
            dialect="postgresql",
            jdbc_url=jdbc_opts["url"],
            dbtable=native_table,
            user=jdbc_opts["user"],
            password=jdbc_opts["password"],
            schema_json=schema.jsonValue(),
            rows=rows,
            mode=mode,
        )
        writer = (
            spark.createDataFrame([tuple(row) for row in rows], schema)
            .write.format("jdbc")
            .option("dbtable", sail_table)
            .options(**jdbc_opts)
        )
        if mode is not None:
            writer = writer.mode(mode)
        writer.save()

        assert _pg_snapshot(pg_dsn, native_table) == _pg_snapshot(pg_dsn, sail_table)
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{native_table}"')
            cur.execute(f'DROP TABLE IF EXISTS "{sail_table}"')


@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_postgres_truncate_option_matches_native_spark_4_1_2(spark, jdbc_opts, pg_dsn):
    """Settle Spark docs/source disagreement using observable database state."""
    import psycopg
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    native_table = "wt_pg_native_truncate"
    sail_table = "wt_pg_sail_truncate"
    schema = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
    rows = [[2, "new"]]
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        for table in (native_table, sail_table):
            cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')  # noqa: S608
            cur.execute(f'CREATE TABLE "{table}" (id INTEGER, name TEXT)')  # noqa: S608
            cur.execute(f'CREATE INDEX "{table}_name_idx" ON "{table}" (name)')  # noqa: S608
            cur.execute(f'INSERT INTO "{table}" VALUES (1, %s)', ("old",))  # noqa: S608
    try:
        run_native_jdbc_write(
            dialect="postgresql",
            jdbc_url=jdbc_opts["url"],
            dbtable=native_table,
            user=jdbc_opts["user"],
            password=jdbc_opts["password"],
            schema_json=schema.jsonValue(),
            rows=rows,
            mode="overwrite",
            options={"truncate": "true"},
        )
        spark.createDataFrame([tuple(row) for row in rows], schema).write.format("jdbc").option(
            "dbtable", sail_table
        ).option("truncate", "true").options(**jdbc_opts).mode("overwrite").save()

        with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
            snapshots = []
            for table in (native_table, sail_table):
                cur.execute(
                    "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
                    "WHERE table_name = %s ORDER BY ordinal_position",
                    (table,),
                )
                columns = cur.fetchall()
                cur.execute("SELECT to_regclass(%s)", (f"{table}_name_idx",))
                index_exists = cur.fetchone()[0] is not None
                cur.execute(f'SELECT id, name FROM "{table}" ORDER BY id')  # noqa: S608
                snapshots.append((columns, index_exists, cur.fetchall()))
            assert snapshots[0] == snapshots[1]
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{native_table}" CASCADE')
            cur.execute(f'DROP TABLE IF EXISTS "{sail_table}" CASCADE')


@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_postgres_truncate_schema_mismatch_failure_matches_native_spark_4_1_2(
    spark,
    jdbc_opts,
    pg_dsn,
):
    import subprocess

    import psycopg
    from pyspark.sql.types import IntegerType, StructField, StructType

    native_table = "wt_pg_native_truncate_mismatch"
    sail_table = "wt_pg_sail_truncate_mismatch"
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        for table in (native_table, sail_table):
            cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
            cur.execute(f'CREATE TABLE "{table}" (old_id INTEGER)')
            cur.execute(f'INSERT INTO "{table}" VALUES (1)')
    try:
        schema = StructType([StructField("new_id", IntegerType())])
        rows = [[2]]
        with pytest.raises(subprocess.CalledProcessError):
            run_native_jdbc_write(
                dialect="postgresql",
                jdbc_url=jdbc_opts["url"],
                dbtable=native_table,
                user=jdbc_opts["user"],
                password=jdbc_opts["password"],
                schema_json=schema.jsonValue(),
                rows=rows,
                mode="overwrite",
                options={"truncate": "true"},
            )
        with pytest.raises(Exception, match=r"new_id|schema"):
            (
                spark.createDataFrame([tuple(row) for row in rows], schema)
                .write.format("jdbc")
                .option("dbtable", sail_table)
                .option("truncate", "true")
                .options(**jdbc_opts)
                .mode("overwrite")
                .save()
            )

        assert _pg_snapshot(pg_dsn, native_table) == _pg_snapshot(pg_dsn, sail_table)
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{native_table}" CASCADE')
            cur.execute(f'DROP TABLE IF EXISTS "{sail_table}" CASCADE')


@pytest.mark.parametrize("cascade", [False, True])
@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_postgres_cascade_truncate_matches_native_spark_4_1_2(
    spark,
    jdbc_opts,
    pg_dsn,
    cascade,
):
    import subprocess

    import psycopg
    from pyspark.sql.types import IntegerType, StructField, StructType

    native_parent = f"wt_pg_native_fk_{str(cascade).lower()}"
    sail_parent = f"wt_pg_sail_fk_{str(cascade).lower()}"
    native_child = f"{native_parent}_child"
    sail_child = f"{sail_parent}_child"
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        for parent, child in ((native_parent, native_child), (sail_parent, sail_child)):
            cur.execute(f'DROP TABLE IF EXISTS "{child}"')
            cur.execute(f'DROP TABLE IF EXISTS "{parent}" CASCADE')
            cur.execute(f'CREATE TABLE "{parent}" (id INTEGER PRIMARY KEY)')
            cur.execute(f'CREATE TABLE "{child}" (parent_id INTEGER REFERENCES "{parent}" (id))')
            cur.execute(f'INSERT INTO "{parent}" VALUES (1)')
            cur.execute(f'INSERT INTO "{child}" VALUES (1)')
    try:
        schema = StructType([StructField("id", IntegerType())])
        rows = [[2]]

        def native_call():
            run_native_jdbc_write(
                dialect="postgresql",
                jdbc_url=jdbc_opts["url"],
                dbtable=native_parent,
                user=jdbc_opts["user"],
                password=jdbc_opts["password"],
                schema_json=schema.jsonValue(),
                rows=rows,
                mode="overwrite",
                options={
                    "truncate": "true",
                    "cascadeTruncate": str(cascade).lower(),
                },
            )

        def sail_call():
            (
                spark.createDataFrame([tuple(row) for row in rows], schema)
                .write.format("jdbc")
                .option("dbtable", sail_parent)
                .option("truncate", "true")
                .option("cascadeTruncate", str(cascade).lower())
                .options(**jdbc_opts)
                .mode("overwrite")
                .save()
            )

        if cascade:
            native_call()
            sail_call()
        else:
            with pytest.raises(subprocess.CalledProcessError):
                native_call()
            with pytest.raises(Exception, match=r"foreign key|constraint"):
                sail_call()

        assert _pg_snapshot(pg_dsn, native_parent) == _pg_snapshot(pg_dsn, sail_parent)
        assert _pg_snapshot(pg_dsn, native_child) == _pg_snapshot(pg_dsn, sail_child)
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            for parent, child in ((native_parent, native_child), (sail_parent, sail_child)):
                cur.execute(f'DROP TABLE IF EXISTS "{child}"')
                cur.execute(f'DROP TABLE IF EXISTS "{parent}" CASCADE')


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


@pytest.mark.parametrize("write_table", ["wt_batched_overwrite"], indirect=True)
def test_write_overwrite_small_batchsize_writes_all_rows(spark, jdbc_opts, write_table):
    """A batchsize smaller than the partition chunks the ADBC ingest yet writes every row."""
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
    df = spark.createDataFrame([(i, f"n{i}") for i in range(1, 6)], schema)

    # batchsize 2 over 5 rows exercises the chunk loop incl. a partial final chunk.
    df.write.format("jdbc").option("dbtable", write_table).option("batchsize", "2").options(**jdbc_opts).mode(
        "overwrite"
    ).save()

    result = _read_pg_table(spark, jdbc_opts, write_table)
    assert result.count() == 5  # noqa: PLR2004
    assert {r.id for r in result.collect()} == {1, 2, 3, 4, 5}


@pytest.mark.parametrize(
    ("input_partitions", "limit", "expected_connections"),
    [(8, 2, 2), (1, 4, 1)],
)
def test_write_num_partitions_caps_connections_without_scaling_up(
    spark,
    jdbc_opts,
    pg_dsn,
    input_partitions,
    limit,
    expected_connections,
):
    import psycopg

    table = f"wt_num_partitions_{input_partitions}_{limit}"
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id BIGINT, writer_pid INTEGER DEFAULT pg_backend_pid())')
    try:
        (
            spark.range(800)
            .repartition(input_partitions)
            .write.format("jdbc")
            .option("dbtable", table)
            .option("numPartitions", str(limit))
            .options(**jdbc_opts)
            .mode("append")
            .save()
        )
        with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*), COUNT(DISTINCT writer_pid) FROM "{table}"')
            row_count, connection_count = cur.fetchone()
        assert row_count == 800  # noqa: PLR2004
        assert connection_count == expected_connections
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')


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

    second.write.format("jdbc").option("dbtable", write_table).option("sail.jdbc.overwriteMode", "atomic").options(
        **jdbc_opts
    ).mode("overwrite").save()

    result = _read_pg_table(spark, jdbc_opts, write_table)
    assert result.count() == 1
    assert result.collect()[0].name == "Charlie"


@pytest.mark.parametrize("serial_write_table", ["wt_atomic_serial"], indirect=True)
def test_write_overwrite_atomic_serial_sequence(spark, jdbc_opts, pg_dsn, serial_write_table):
    """atomic overwrite works on a serial/owned-sequence column and leaves the sequence
    usable and past the loaded max. Regression: DROP was refused because the staging
    default depended on the target's owned sequence.
    """
    import psycopg
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
    first = spark.createDataFrame([(1, "Alice"), (2, "Bob")], schema)
    second = spark.createDataFrame([(10, "Charlie"), (11, "Dave")], schema)

    first.write.format("jdbc").option("dbtable", serial_write_table).options(**jdbc_opts).mode("append").save()
    second.write.format("jdbc").option("dbtable", serial_write_table).option(
        "sail.jdbc.overwriteMode", "atomic"
    ).options(**jdbc_opts).mode("overwrite").save()

    result = _read_pg_table(spark, jdbc_opts, serial_write_table)
    assert sorted(r.name for r in result.collect()) == ["Charlie", "Dave"]

    # The serial default must still work and hand out an id past the loaded max (11).
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'INSERT INTO "{serial_write_table}" (name) VALUES (%s) RETURNING id', ("Eve",))  # noqa: S608
        new_id = cur.fetchone()[0]
    assert new_id > 11  # noqa: PLR2004


@pytest.mark.parametrize("generation", ["ALWAYS", "BY DEFAULT"])
def test_write_overwrite_atomic_identity_sequence(spark, jdbc_opts, pg_dsn, generation):
    """Atomic swap must retain identity semantics and advance past explicitly loaded IDs."""
    import psycopg
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    table = f"wt_atomic_identity_{generation.lower().replace(' ', '_')}"
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')  # noqa: S608
        cur.execute(  # noqa: S608
            f'CREATE TABLE "{table}" (id INTEGER GENERATED {generation} AS IDENTITY PRIMARY KEY, name TEXT)'
        )
    try:
        schema = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
        spark.createDataFrame([(10, "ten"), (11, "eleven")], schema).write.format("jdbc").option(
            "dbtable", table
        ).option("sail.jdbc.overwriteMode", "atomic").options(**jdbc_opts).mode("overwrite").save()

        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT identity_generation FROM information_schema.columns "
                "WHERE table_name = %s AND column_name = 'id'",
                (table,),
            )
            assert cur.fetchone()[0] == generation
            cur.execute(f'INSERT INTO "{table}" (name) VALUES (%s) RETURNING id', ("next",))  # noqa: S608
            assert cur.fetchone()[0] > 11  # noqa: PLR2004
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')  # noqa: S608


def test_postgres_partition_failure_cleans_atomic_staging(pg_dsn, monkeypatch):
    import adbc_driver_postgresql.dbapi as pg_dbapi
    import psycopg
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import PgWriteEngine, _staging_name_atomic

    table = "wt_pg_failed_partition"
    run_id = "failed"
    staging = _staging_name_atomic(table, run_id)
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{staging}"')
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id INTEGER)')
    try:
        writer = PgWriteEngine(
            dsn=pg_dsn,
            dbtable=table,
            overwrite_mode="atomic",
            batch_size=1000,
            run_id=run_id,
        )

        def fail_after_create(*_args, **_kwargs):
            raise RuntimeError("injected ingest failure")

        monkeypatch.setattr(pg_dbapi, "connect", fail_after_create)
        with pytest.raises(RuntimeError, match="injected ingest failure"):
            writer.write_partition(0, [pa.RecordBatch.from_pydict({"id": [1]})])

        with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (staging,))
            assert cur.fetchone()[0] is None
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{staging}"')
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')


@pytest.mark.parametrize("table", ["t" * 63, "é" * 31])
def test_postgres_atomic_overwrite_long_target_is_collision_safe(pg_dsn, table):
    import psycopg
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import PgWriteEngine, _staging_name_atomic

    writer = PgWriteEngine(
        dsn=pg_dsn,
        dbtable=table,
        overwrite_mode="atomic",
        batch_size=1000,
        run_id="longname",
    )
    staging = _staging_name_atomic(table, writer.run_id)
    assert staging != table
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{staging}"')
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id INTEGER)')
    try:
        batch = pa.RecordBatch.from_pydict({"id": pa.array([1], type=pa.int32())})
        result = writer.write_partition(0, [batch])
        writer.commit([result])
        with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
            cur.execute(f'SELECT id FROM "{table}"')
            assert cur.fetchall() == [(1,)]
            cur.execute("SELECT to_regclass(%s)", (staging,))
            assert cur.fetchone()[0] is None
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{staging}"')
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')


def test_postgres_partition_failure_rolls_back_only_failed_partition(pg_dsn):
    import psycopg
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import PgWriteEngine

    table = "wt_pg_partition_rollback"
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY)')
    try:
        writer = PgWriteEngine(dsn=pg_dsn, dbtable=table, batch_size=1000)
        first = pa.RecordBatch.from_pydict({"id": pa.array([1], type=pa.int32())})
        duplicate = pa.RecordBatch.from_pydict({"id": pa.array([2, 1], type=pa.int32())})
        writer.write_partition(0, [first])
        with pytest.raises(RuntimeError):
            writer.write_partition(1, [duplicate])

        with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
            cur.execute(f'SELECT id FROM "{table}" ORDER BY id')
            assert cur.fetchall() == [(1,)]
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')


def test_postgres_retry_after_commit_is_at_least_once(pg_dsn):
    import psycopg
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import PgWriteEngine

    table = "wt_pg_partition_retry"
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id INTEGER)')
    try:
        writer = PgWriteEngine(dsn=pg_dsn, dbtable=table, batch_size=1000)
        batch = pa.RecordBatch.from_pydict({"id": pa.array([1], type=pa.int32())})
        writer.write_partition(0, [batch])
        writer.write_partition(0, [batch])

        with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
            cur.execute(f'SELECT id FROM "{table}" ORDER BY id')
            assert cur.fetchall() == [(1,), (1,)]
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')


def test_write_overwrite_truncate_does_not_truncate_inheritance_children(spark, jdbc_opts, pg_dsn):
    """PostgreSQL truncate paths must use ONLY, matching Spark's dialect SQL."""
    import psycopg
    from pyspark.sql.types import IntegerType, StructField, StructType

    parent = "wt_truncate_parent"
    child = "wt_truncate_child"
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{child}"')
        cur.execute(f'DROP TABLE IF EXISTS "{parent}" CASCADE')
        cur.execute(f'CREATE TABLE "{parent}" (id INTEGER)')
        cur.execute(f'CREATE TABLE "{child}" () INHERITS ("{parent}")')
        cur.execute(f'INSERT INTO "{parent}" VALUES (1)')
        cur.execute(f'INSERT INTO "{child}" VALUES (2)')
    try:
        spark.createDataFrame([(3,)], StructType([StructField("id", IntegerType())])).write.format("jdbc").option(
            "dbtable", parent
        ).option("sail.jdbc.overwriteMode", "truncate").options(**jdbc_opts).mode("overwrite").save()

        with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
            cur.execute(f'SELECT id FROM ONLY "{parent}" ORDER BY id')
            assert cur.fetchall() == [(3,)]
            cur.execute(f'SELECT id FROM ONLY "{child}" ORDER BY id')
            assert cur.fetchall() == [(2,)]
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{child}"')
            cur.execute(f'DROP TABLE IF EXISTS "{parent}" CASCADE')


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

    second.write.format("jdbc").option("dbtable", write_table).option("sail.jdbc.overwriteMode", "truncate").options(
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
    empty_df.write.format("jdbc").option("dbtable", write_table).option("sail.jdbc.overwriteMode", "atomic").options(
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
    df.write.format("jdbc").option("dbtable", write_table).option("sail.jdbc.overwriteMode", "atomic").options(
        **jdbc_opts
    ).mode("overwrite").save()

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

    second.write.format("jdbc").option("dbtable", dbtable).option("sail.jdbc.overwriteMode", "atomic").options(
        **jdbc_opts
    ).mode("overwrite").save()

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

    second.write.format("jdbc").option("dbtable", dbtable).option("sail.jdbc.overwriteMode", "truncate").options(
        **jdbc_opts
    ).mode("overwrite").save()

    assert _count_in_schema(pg_dsn, schema, table) == 1
    assert _read_pg_table(spark, jdbc_opts, dbtable).collect()[0].name == "Charlie"
