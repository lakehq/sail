"""Integration tests for the SQLAlchemy write fallback (MySQL, SQL Server).

Each dialect starts its own testcontainer.  Writes go through the Sail Spark
Connect server (``df.write.format("jdbc")``); results are verified with a direct
SQLAlchemy query so the assertions do not depend on the reader.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration

try:
    from pyspark.sql.datasource import DataSourceArrowWriter  # noqa: F401  (Spark 4.0+)
except ImportError:
    pytest.skip("JDBC data source requires the PySpark Python DataSource API (4.0+)", allow_module_level=True)


@pytest.fixture(scope="module", autouse=True)
def register_jdbc(spark):
    from pysail.spark.datasource.jdbc import JdbcDataSource

    spark.dataSource.register(JdbcDataSource)


def _schema():
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    return StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("score", DoubleType()),
        ]
    )


def _count_names(sa_url, table):
    import sqlalchemy as sa

    engine = sa.create_engine(sa_url)
    try:
        with engine.connect() as conn:
            rows = conn.execute(sa.text(f"SELECT name FROM {table}")).fetchall()  # noqa: S608
    finally:
        engine.dispose()
    return len(rows), {r[0] for r in rows}


# ---------------------------------------------------------------------------
# MySQL
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def mysql_ctx():
    from testcontainers.mysql import MySqlContainer

    with MySqlContainer("mysql:8.4") as c:
        host = c.get_container_host_ip()
        port = c.get_exposed_port(3306)
        jdbc_url = f"jdbc:mysql://{host}:{port}/{c.dbname}"
        sa_url = f"mysql+pymysql://{c.username}:{c.password}@{host}:{port}/{c.dbname}"
        opts = {"url": jdbc_url, "user": c.username, "password": c.password}
        yield opts, sa_url


@pytest.fixture
def mysql_table(mysql_ctx):
    import sqlalchemy as sa

    opts, sa_url = mysql_ctx
    table = "wt_mysql"
    engine = sa.create_engine(sa_url)
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
        conn.execute(sa.text(f"CREATE TABLE {table} (id INT, name VARCHAR(64), score DOUBLE)"))
    engine.dispose()
    yield table, opts, sa_url
    engine = sa.create_engine(sa_url)
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
    engine.dispose()


def test_mysql_write_append(spark, mysql_table):
    table, opts, sa_url = mysql_table
    df = spark.createDataFrame([(1, "Alice", 9.5), (2, "Bob", 7.2)], _schema())
    df.write.format("jdbc").option("dbtable", table).options(**opts).mode("append").save()

    count, names = _count_names(sa_url, table)
    assert count == 2  # noqa: PLR2004
    assert names == {"Alice", "Bob"}


def test_mysql_write_overwrite(spark, mysql_table):
    table, opts, sa_url = mysql_table
    first = spark.createDataFrame([(1, "Alice", 9.5), (2, "Bob", 7.2)], _schema())
    second = spark.createDataFrame([(3, "Charlie", 8.8)], _schema())

    first.write.format("jdbc").option("dbtable", table).options(**opts).mode("append").save()
    second.write.format("jdbc").option("dbtable", table).options(**opts).mode("overwrite").save()

    count, names = _count_names(sa_url, table)
    assert count == 1
    assert names == {"Charlie"}


# ---------------------------------------------------------------------------
# Integer fidelity: NULLs stay NULL and bigints > 2^53 keep exact precision.
# Regression guard for the pandas to_sql float64 coercion bug.
# ---------------------------------------------------------------------------

_BIG = 2**53 + 1  # 9007199254740993 — not representable as a float64


def _bigint_schema():
    from pyspark.sql.types import LongType, StructField, StructType

    return StructType([StructField("id", LongType()), StructField("big", LongType())])


def _read_id_big(sa_url, table):
    import sqlalchemy as sa

    engine = sa.create_engine(sa_url)
    try:
        with engine.connect() as conn:
            rows = conn.execute(sa.text(f"SELECT id, big FROM {table} ORDER BY id")).fetchall()  # noqa: S608
    finally:
        engine.dispose()
    return rows


@pytest.fixture
def mysql_bigint_table(mysql_ctx):
    import sqlalchemy as sa

    opts, sa_url = mysql_ctx
    table = "wt_mysql_bigint"
    engine = sa.create_engine(sa_url)
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
        conn.execute(sa.text(f"CREATE TABLE {table} (id BIGINT, big BIGINT)"))
    engine.dispose()
    yield table, opts, sa_url
    engine = sa.create_engine(sa_url)
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
    engine.dispose()


def test_mysql_integer_fidelity(spark, mysql_bigint_table):
    """A NULL int and a bigint > 2^53 survive the write exactly (no float coercion)."""
    table, opts, sa_url = mysql_bigint_table
    df = spark.createDataFrame([(1, _BIG), (2, None)], _bigint_schema())
    df.write.format("jdbc").option("dbtable", table).options(**opts).mode("append").save()

    rows = _read_id_big(sa_url, table)
    assert rows == [(1, _BIG), (2, None)]


@pytest.fixture
def mssql_bigint_table(mssql_ctx):
    import sqlalchemy as sa

    opts, sa_url = mssql_ctx
    table = "wt_mssql_bigint"
    engine = sa.create_engine(sa_url)
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
        conn.execute(sa.text(f"CREATE TABLE {table} (id BIGINT, big BIGINT)"))
    engine.dispose()
    yield table, opts, sa_url
    engine = sa.create_engine(sa_url)
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
    engine.dispose()


def test_mssql_integer_fidelity(spark, mssql_bigint_table):
    """A NULL int and a bigint > 2^53 survive the write exactly (no float coercion)."""
    table, opts, sa_url = mssql_bigint_table
    df = spark.createDataFrame([(1, _BIG), (2, None)], _bigint_schema())
    df.write.format("jdbc").option("dbtable", table).options(**opts).mode("append").save()

    rows = _read_id_big(sa_url, table)
    assert rows == [(1, _BIG), (2, None)]


# ---------------------------------------------------------------------------
# SQL Server
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def mssql_ctx():
    from testcontainers.mssql import SqlServerContainer

    with SqlServerContainer("mcr.microsoft.com/mssql/server:2022-latest") as c:
        host = c.get_container_host_ip()
        port = c.get_exposed_port(1433)
        jdbc_url = f"jdbc:sqlserver://{host}:{port};databaseName={c.dbname}"
        sa_url = f"mssql+pymssql://{c.username}:{c.password}@{host}:{port}/{c.dbname}"
        opts = {"url": jdbc_url, "user": c.username, "password": c.password}
        yield opts, sa_url


@pytest.fixture
def mssql_table(mssql_ctx):
    import sqlalchemy as sa

    opts, sa_url = mssql_ctx
    table = "wt_mssql"
    engine = sa.create_engine(sa_url)
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
        conn.execute(sa.text(f"CREATE TABLE {table} (id INT, name VARCHAR(64), score FLOAT)"))
    engine.dispose()
    yield table, opts, sa_url
    engine = sa.create_engine(sa_url)
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
    engine.dispose()


def test_mssql_write_append(spark, mssql_table):
    table, opts, sa_url = mssql_table
    df = spark.createDataFrame([(1, "Alice", 9.5), (2, "Bob", 7.2)], _schema())
    df.write.format("jdbc").option("dbtable", table).options(**opts).mode("append").save()

    count, names = _count_names(sa_url, table)
    assert count == 2  # noqa: PLR2004
    assert names == {"Alice", "Bob"}


def test_mssql_write_overwrite(spark, mssql_table):
    table, opts, sa_url = mssql_table
    first = spark.createDataFrame([(1, "Alice", 9.5), (2, "Bob", 7.2)], _schema())
    second = spark.createDataFrame([(3, "Charlie", 8.8)], _schema())

    first.write.format("jdbc").option("dbtable", table).options(**opts).mode("append").save()
    second.write.format("jdbc").option("dbtable", table).options(**opts).mode("overwrite").save()

    count, names = _count_names(sa_url, table)
    assert count == 1
    assert names == {"Charlie"}
