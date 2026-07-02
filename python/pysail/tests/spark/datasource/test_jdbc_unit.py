"""Pure-unit tests for the JDBC data source — no Spark session or DB container required.

No ``pytestmark = pytest.mark.integration``, so these run without Docker on every
Spark tier that has the Python DataSource write API. They import ``jdbc.py``, which
needs ``pyspark.sql.datasource`` (Spark 4.1+), so the module is skipped below that.
"""

from __future__ import annotations

import datetime as dt

import pyarrow as pa
import pytest

from pysail.testing.spark.utils.common import pyspark_version

if pyspark_version() < (4, 1):
    pytest.skip("JDBC data source requires the PySpark Python DataSource write API (4.1+)", allow_module_level=True)

# ---------------------------------------------------------------------------
# _filter_to_sql
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


# ---------------------------------------------------------------------------
# _jdbc_url_to_dsn
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# _parse_custom_schema
# ---------------------------------------------------------------------------


def test_parse_custom_schema_unit():
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


# ---------------------------------------------------------------------------
# _lit (via _filter_to_sql)
# ---------------------------------------------------------------------------


def test_lit_unit():
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


# ---------------------------------------------------------------------------
# _quote_identifier
# ---------------------------------------------------------------------------


def test_quote_identifier_unit():
    from pysail.spark.datasource.jdbc import _quote_identifier

    assert _quote_identifier("age") == '"age"'
    assert _quote_identifier("my col") == '"my col"'
    assert _quote_identifier('col"name') == '"col""name"'
    assert _quote_identifier("") == '""'


# ---------------------------------------------------------------------------
# writer() dialect dispatch
# ---------------------------------------------------------------------------


def test_write_supported_dialects_return_writers():
    """PostgreSQL, MySQL and SQL Server URLs return a writer."""
    from pyspark.sql.types import IntegerType, StructField, StructType

    from pysail.spark.datasource.jdbc import (
        JdbcDataSource,
        JdbcDataSourceWriter,
        SqlAlchemyDataSourceWriter,
    )

    schema = StructType([StructField("id", IntegerType())])
    cases = [
        ("jdbc:postgresql://localhost:5432/db", JdbcDataSourceWriter),
        ("jdbc:mysql://localhost:3306/db", SqlAlchemyDataSourceWriter),
        ("jdbc:sqlserver://localhost:1433;databaseName=db", SqlAlchemyDataSourceWriter),
    ]
    for url, expected in cases:
        ds = JdbcDataSource(options={"url": url, "dbtable": "t"})
        assert isinstance(ds.writer(schema, overwrite=False), expected)


def test_write_unsupported_dialect_raises():
    """Dialects without a write backend raise a clear error."""
    from pyspark.sql.types import IntegerType, StructField, StructType

    from pysail.spark.datasource.jdbc import JdbcDataSource

    schema = StructType([StructField("id", IntegerType())])
    for url in ["jdbc:oracle:thin:@//localhost:1521/db", "jdbc:h2:mem:test"]:
        ds = JdbcDataSource(options={"url": url, "dbtable": "t"})
        with pytest.raises(ValueError, match="PostgreSQL, MySQL and SQL Server"):
            ds.writer(schema, overwrite=False)


def test_sqlalchemy_url_translation():
    """_sqlalchemy_url maps stripped JDBC DSNs to (SQLAlchemy URL, connect_args)."""
    from pysail.spark.datasource.jdbc import _sqlalchemy_url

    assert _sqlalchemy_url("mysql://u:p@h:3306/db") == ("mysql+pymysql://u:p@h:3306/db", {})
    url, connect_args = _sqlalchemy_url("sqlserver://u:p@h:1433;databaseName=db")
    assert url == "mssql+pymssql://u:p@h:1433/db"
    assert connect_args == {}
    with pytest.raises(ValueError, match="Unsupported"):
        _sqlalchemy_url("oracle://u:p@h:1521/db")


def test_sqlserver_url_parsing_preserves_params():
    """The SQL Server URL parser keeps every ;key=value; param, not just databaseName.

    This is the regression guard for the earlier bug where encrypt /
    trustServerCertificate / applicationIntent were silently dropped.
    """
    from pysail.spark.datasource.jdbc import _parse_sqlserver_url, _sqlalchemy_url

    url, connect_args = _sqlalchemy_url(
        "sqlserver://u:p@h:1433;databaseName=db;encrypt=true;trustServerCertificate=true;applicationIntent=ReadOnly"
    )
    assert url == "mssql+pymssql://u:p@h:1433/db"
    assert connect_args["encryption"] == "require"
    assert connect_args["read_only"] is True
    # trustServerCertificate is accepted but intentionally not mapped (FreeTDS handles trust)
    assert "trustServerCertificate" not in connect_args

    # encrypt=false maps to encryption off
    _, ca = _parse_sqlserver_url("u:p@h:1433;databaseName=db;encrypt=false")
    assert ca["encryption"] == "off"

    # No credentials, named instance, no explicit port
    url2, _ = _parse_sqlserver_url("host\\SQLEXPRESS;databaseName=db")
    assert url2 == "mssql+pymssql://host\\SQLEXPRESS/db"

    # Host + port, no database
    url3, ca3 = _parse_sqlserver_url("h:1433")
    assert url3 == "mssql+pymssql://h:1433"
    assert ca3 == {}

    # Instance + port together
    url4, _ = _parse_sqlserver_url("h\\inst:1433;databaseName=db")
    assert url4 == "mssql+pymssql://h\\inst:1433/db"

    # Unknown params are ignored, not carried
    _, ca5 = _parse_sqlserver_url("h:1433;databaseName=db;someFutureParam=x")
    assert "somefutureparam" not in ca5
    assert ca5 == {}


def test_write_options_no_url_raises():
    """writer() options resolver should raise when url is missing."""
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    from pysail.spark.datasource.jdbc import JdbcDataSource

    schema = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
    ds = JdbcDataSource(options={"dbtable": "t"})
    with pytest.raises((ValueError, Exception), match="url"):
        ds.writer(schema, overwrite=False)


def test_write_options_no_dbtable_raises():
    """writer() should reject a query option (can't write to a query)."""
    from pyspark.sql.types import IntegerType, StructField, StructType

    from pysail.spark.datasource.jdbc import JdbcDataSource

    schema = StructType([StructField("id", IntegerType())])
    ds = JdbcDataSource(options={"url": "jdbc:postgresql://localhost:5432/db", "query": "SELECT 1"})
    with pytest.raises((ValueError, Exception), match=r"dbtable|query"):
        ds.writer(schema, overwrite=False)


# ---------------------------------------------------------------------------
# PgWriteEngine identifier + staging name helpers
# ---------------------------------------------------------------------------


def test_quote_qualified_schema_qualified():
    """_quote_qualified handles schema-qualified names (used by the write engine)."""
    from pysail.spark.datasource.jdbc import _quote_qualified

    assert _quote_qualified("public.foo") == '"public"."foo"'
    assert _quote_qualified("foo") == '"foo"'


def test_split_schema():
    """_split_schema separates the schema from the table name."""
    from pysail.spark.datasource.jdbc import _split_schema

    assert _split_schema("orders") == (None, "orders")
    assert _split_schema("public.orders") == ("public", "orders")


def test_pg_write_engine_staging_name():
    """_staging_name_atomic is deterministic and keeps the target's schema."""
    from pysail.spark.datasource.jdbc import _staging_name_atomic

    name = _staging_name_atomic("myschema.orders", "abc123")
    assert name == "myschema.orders__sail_stg_abc123"
    # Deterministic for the same (dbtable, run_id)
    assert _staging_name_atomic("myschema.orders", "abc123") == name
    # An unqualified target stays unqualified
    assert _staging_name_atomic("orders", "abc123") == "orders__sail_stg_abc123"


# ---------------------------------------------------------------------------
# overwrite_mode option resolution
# ---------------------------------------------------------------------------


def test_overwrite_mode_default_is_atomic():
    """JdbcDataSource.writer(overwrite=True) uses 'atomic' by default."""
    from pyspark.sql.types import IntegerType, StructField, StructType

    from pysail.spark.datasource.jdbc import JdbcDataSource, JdbcDataSourceWriter

    schema = StructType([StructField("id", IntegerType())])
    ds = JdbcDataSource(options={"url": "jdbc:postgresql://localhost:5432/db", "dbtable": "t"})
    writer = ds.writer(schema, overwrite=True)
    assert isinstance(writer, JdbcDataSourceWriter)
    assert writer._engine.overwrite_mode == "atomic"  # noqa: SLF001


def test_overwrite_mode_truncate_valid():
    """overwriteMode='truncate' is accepted."""
    from pyspark.sql.types import IntegerType, StructField, StructType

    from pysail.spark.datasource.jdbc import JdbcDataSource

    schema = StructType([StructField("id", IntegerType())])
    ds = JdbcDataSource(
        options={"url": "jdbc:postgresql://localhost:5432/db", "dbtable": "t", "overwriteMode": "truncate"}
    )
    writer = ds.writer(schema, overwrite=True)
    assert writer._engine.overwrite_mode == "truncate"  # noqa: SLF001


def test_overwrite_mode_invalid_raises():
    """Unknown overwriteMode raises ValueError."""
    from pyspark.sql.types import IntegerType, StructField, StructType

    from pysail.spark.datasource.jdbc import JdbcDataSource

    schema = StructType([StructField("id", IntegerType())])
    ds = JdbcDataSource(
        options={"url": "jdbc:postgresql://localhost:5432/db", "dbtable": "t", "overwriteMode": "badmode"}
    )
    with pytest.raises(ValueError, match="badmode"):
        ds.writer(schema, overwrite=True)


def test_staging_name_atomic_run_id_suffix():
    """_staging_name_atomic suffixes the run_id so concurrent writers don't collide."""
    from pysail.spark.datasource.jdbc import _staging_name_atomic

    assert _staging_name_atomic("public.orders", "deadbeef") == "public.orders__sail_stg_deadbeef"
    assert _staging_name_atomic("orders", "deadbeef") == "orders__sail_stg_deadbeef"
    # Different run_ids → different staging tables (the concurrency-collision fix)
    assert _staging_name_atomic("orders", "run1") != _staging_name_atomic("orders", "run2")


def test_pg_write_engine_generates_run_id():
    """Each engine instance gets a run_id; passing one is honored."""
    from pysail.spark.datasource.jdbc import PgWriteEngine

    e1 = PgWriteEngine(dsn="postgresql://x/y", dbtable="t", overwrite_mode="atomic")
    e2 = PgWriteEngine(dsn="postgresql://x/y", dbtable="t", overwrite_mode="atomic")
    assert e1.run_id
    assert e2.run_id
    assert e1.run_id != e2.run_id  # distinct engines → distinct run_ids
    assert PgWriteEngine(dsn="postgresql://x/y", dbtable="t", run_id="fixed").run_id == "fixed"


def test_concurrent_engines_have_distinct_staging_names():
    """Two PgWriteEngine instances for the same target produce distinct staging table names.

    This is the pure-unit proof of the run_id fix: no DB required.
    The property ensures concurrent jobs cannot accidentally share a staging table
    and corrupt each other's data.
    """
    from pysail.spark.datasource.jdbc import PgWriteEngine, _staging_name_atomic

    engines = [
        PgWriteEngine(dsn="postgresql://x/y", dbtable="public.orders", overwrite_mode="atomic") for _ in range(8)
    ]

    staging_names = [_staging_name_atomic("public.orders", e.run_id) for e in engines]

    # All run_ids are distinct
    run_ids = [e.run_id for e in engines]
    assert len(set(run_ids)) == len(run_ids), f"duplicate run_ids: {run_ids}"

    # All staging table names are distinct
    assert len(set(staging_names)) == len(staging_names), f"duplicate staging names: {staging_names}"

    # Every staging name encodes the base table name and keeps the schema
    for name in staging_names:
        assert name.startswith("public.orders__sail_stg_")


# ---------------------------------------------------------------------------
# Credential scrubbing: psycopg errors must not leak the DSN
# ---------------------------------------------------------------------------


def test_safe_error_scrubs_password():
    """_safe_error must remove password substrings from exception messages."""
    from pysail.spark.datasource.jdbc import _safe_error

    dsn = "postgresql://user:s3cr3t@myhost:5432/db"
    exc = Exception(f"connection to server at 'myhost' failed: {dsn}: Connection refused")
    result = _safe_error(exc, dsn)
    assert "s3cr3t" not in result
    assert "<dsn-redacted>" in result


def test_atomic_staging_psycopg_error_is_scrubbed():
    """A psycopg failure during atomic staging CREATE is wrapped through _safe_error.

    No raw DSN/password should appear in the raised RuntimeError.
    """
    from unittest import mock

    import psycopg

    from pysail.spark.datasource.jdbc import PgWriteEngine

    dsn = "postgresql://user:t0ps3cr3t@badhost:9999/db"
    engine = PgWriteEngine(dsn=dsn, dbtable="t", overwrite_mode="atomic", run_id="r1")

    with (
        mock.patch("psycopg.connect", side_effect=psycopg.OperationalError(f"could not connect: {dsn}")),
        pytest.raises(RuntimeError) as exc_info,
    ):
        engine.write_partition(0, [])

    err_str = str(exc_info.value)
    assert "t0ps3cr3t" not in err_str, f"Password leaked in error: {err_str!r}"


def test_truncate_advisory_lock_psycopg_error_is_scrubbed():
    """A psycopg failure during truncate-mode advisory lock acquisition is scrubbed."""
    from unittest import mock

    import psycopg

    from pysail.spark.datasource.jdbc import PgWriteEngine

    dsn = "postgresql://user:adv1s0ry@badhost:9999/db"
    engine = PgWriteEngine(dsn=dsn, dbtable="t", overwrite_mode="truncate", run_id="r1")

    with (
        mock.patch("psycopg.connect", side_effect=psycopg.OperationalError(f"could not connect: {dsn}")),
        pytest.raises(RuntimeError) as exc_info,
    ):
        engine.write_partition(0, [])

    err_str = str(exc_info.value)
    assert "adv1s0ry" not in err_str, f"Password leaked in error: {err_str!r}"


def test_commit_atomic_psycopg_error_is_scrubbed():
    """A psycopg failure during commit() atomic rename is scrubbed."""
    from unittest import mock

    import psycopg

    from pysail.spark.datasource.jdbc import PartitionResult, PgWriteEngine

    dsn = "postgresql://user:c0mm1ts3cr3t@badhost:9999/db"
    engine = PgWriteEngine(dsn=dsn, dbtable="t", overwrite_mode="atomic", run_id="r1")
    fake_result = PartitionResult(partition_id=0, rows_written=1, staging_table="t__sail_stg_r1")

    with (
        mock.patch("psycopg.connect", side_effect=psycopg.OperationalError(f"could not connect: {dsn}")),
        pytest.raises(RuntimeError) as exc_info,
    ):
        engine.commit([fake_result])

    err_str = str(exc_info.value)
    assert "c0mm1ts3cr3t" not in err_str, f"Password leaked in error: {err_str!r}"
