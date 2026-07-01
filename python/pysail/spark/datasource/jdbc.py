"""JDBC data source for Sail, backed by connectorX (reads) and ADBC/SQLAlchemy (writes).

Supports ``spark.read.format("jdbc")``, ``spark.read.jdbc()`` and
``df.write.format("jdbc").mode("append"|"overwrite").save()`` with options
consistent with the PySpark JDBC API.

Reads use any database connectorX supports.  Writes use ADBC ``adbc_ingest``
for PostgreSQL (binary COPY) and fall back to a SQLAlchemy-core ``INSERT`` for
other dialects (MySQL, SQL Server).

Install the optional dependency::

    pip install pysail[jdbc]

Writing to MySQL or SQL Server additionally needs a DBAPI driver::

    pip install pymysql   # MySQL
    pip install pymssql   # SQL Server
"""

from __future__ import annotations

import contextlib
import datetime
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import quote

import pyarrow as pa

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


def _import_connectorx():
    """Lazy connectorX import so the module loads on workers without it."""
    try:
        import connectorx as cx  # noqa: PLC0415
    except ImportError as e:
        msg = "connectorX is required for the JDBC data source. Install it with: pip install pysail[jdbc]"
        raise ImportError(msg) from e
    return cx


try:
    from pyspark.sql.datasource import (
        DataSource,
        DataSourceArrowWriter,
        DataSourceReader,
        EqualTo,
        Filter,
        GreaterThan,
        GreaterThanOrEqual,
        InputPartition,
        LessThan,
        LessThanOrEqual,
        WriterCommitMessage,
    )
except ImportError as e:
    msg = "PySpark with the DataSource API is required (PySpark >= 4.0)"
    raise ImportError(msg) from e


# ============================================================================
# URL helpers
# ============================================================================


def _jdbc_url_to_dsn(url: str, user: str | None, password: str | None) -> str:
    """Strip the ``jdbc:`` prefix and embed credentials into the DSN.

    Examples::

        jdbc:postgresql://host:5432/db  ->  postgresql://user:pass@host:5432/db
    """
    if not url.startswith("jdbc:"):
        msg = f"Invalid JDBC URL: {url!r}. Expected format: jdbc:<subprotocol>://<host>:<port>/<database>"
        raise ValueError(msg)
    dsn = url[5:]  # strip 'jdbc:'

    if user is not None or password is not None:
        sep = "://"
        try:
            idx = dsn.index(sep)
        except ValueError:
            msg = f"Invalid JDBC URL: {url!r}. Expected format: jdbc:<subprotocol>://<host>:<port>/<database>"
            raise ValueError(msg) from None
        scheme = dsn[:idx]
        rest = dsn[idx + len(sep) :]

        creds = ""
        if user is not None:
            creds = quote(user, safe="")
            if password is not None:
                creds += f":{quote(password, safe='')}"
            creds += "@"
        dsn = f"{scheme}://{creds}{rest}"

    return dsn


# ============================================================================
# Identifier quoting
# ============================================================================


def _quote_identifier(name: str) -> str:
    """Double-quote a SQL identifier, escaping any embedded double quotes."""
    return '"' + name.replace('"', '""') + '"'


def _quote_qualified(name: str) -> str:
    """Double-quote a possibly schema-qualified identifier (``schema.table``)."""
    return ".".join('"' + part.replace('"', '""') + '"' for part in name.split("."))


# ============================================================================
# Filter helpers
# ============================================================================


def _filter_to_sql(f: Filter) -> str | None:
    """Convert a PySpark Filter to a SQL WHERE clause fragment, or None if unsupported."""
    col = ".".join(_quote_identifier(part) for part in f.attribute)

    def _lit(v: object) -> str:
        if v is None:
            return "NULL"
        if isinstance(v, bool):  # before int — bool is a subclass of int
            return "TRUE" if v else "FALSE"
        if isinstance(v, str):
            escaped = v.replace("'", "''")
            return f"'{escaped}'"
        if isinstance(v, (datetime.datetime, datetime.date)):
            return f"'{v.isoformat()}'"
        return str(v)

    if isinstance(f, EqualTo):
        return f"{col} = {_lit(f.value)}"
    if isinstance(f, GreaterThan):
        return f"{col} > {_lit(f.value)}"
    if isinstance(f, GreaterThanOrEqual):
        return f"{col} >= {_lit(f.value)}"
    if isinstance(f, LessThan):
        return f"{col} < {_lit(f.value)}"
    if isinstance(f, LessThanOrEqual):
        return f"{col} <= {_lit(f.value)}"
    return None


# ============================================================================
# InputPartition
# ============================================================================


class JdbcInputPartition(InputPartition):
    """A single JDBC read partition holding a self-contained SQL query."""

    def __init__(self, partition_id: int, query: str, conn_str: str) -> None:
        super().__init__(partition_id)
        self.query = query
        self.conn_str = conn_str


# ============================================================================
# DataSourceReader
# ============================================================================


class JdbcDataSourceReader(DataSourceReader):
    """Reader for :class:`JdbcDataSource`."""

    def __init__(
        self,
        *,
        conn_str: str,
        dbtable: str | None,
        query: str | None,
        num_partitions: int,
        partition_column: str | None,
        lower_bound: int | None,
        upper_bound: int | None,
        push_down_predicate: bool,
    ) -> None:
        self.conn_str = conn_str
        self.dbtable = dbtable
        self.query = query
        self.num_partitions = num_partitions
        self.partition_column = partition_column
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.push_down_predicate = push_down_predicate
        self._sql_filters: list[str] = []

    # ------------------------------------------------------------------
    # Filter pushdown
    # ------------------------------------------------------------------

    def pushFilters(self, filters: list[Filter]) -> Iterator[Filter]:  # noqa: N802
        if not self.push_down_predicate:
            yield from filters
            return

        for f in filters:
            sql = _filter_to_sql(f)
            if sql is not None:
                self._sql_filters.append(sql)
            else:
                yield f  # Reject unsupported filter; Sail will apply it post-read

    # ------------------------------------------------------------------
    # Partitions
    # ------------------------------------------------------------------

    def _base_ref(self) -> str:
        """Return the FROM-clause reference (table name or subquery)."""
        if self.query is not None:
            return f"({self.query}) AS _cx_subq"
        return self.dbtable  # type: ignore[return-value]

    def partitions(self) -> list[InputPartition]:
        base = self._base_ref()

        if self.num_partitions <= 1 or self.partition_column is None:
            # Single partition — may include pushed-down filters
            where = _build_where(self._sql_filters)
            q = f"SELECT * FROM {base}{where}"  # noqa: S608
            return [JdbcInputPartition(0, q, self.conn_str)]

        # Range-stride partitioning
        col = _quote_identifier(self.partition_column)  # type: ignore[arg-type]
        lb = self.lower_bound
        ub = self.upper_bound
        n = self.num_partitions
        stride = (ub - lb) / n  # type: ignore[operator]

        parts: list[InputPartition] = []
        for i in range(n):
            s_lb = lb + int(i * stride)  # type: ignore[operator]
            s_ub = lb + int((i + 1) * stride)  # type: ignore[operator]

            if i == 0:
                # First partition: no lower bound — also captures NULLs and rows below lowerBound
                range_cond = f"({col} < {s_ub} OR {col} IS NULL)"
            elif i == n - 1:
                # Last partition: no upper bound — captures rows above upperBound
                range_cond = f"{col} >= {s_lb}"
            else:
                range_cond = f"{col} >= {s_lb} AND {col} < {s_ub}"

            conditions = [range_cond, *self._sql_filters]
            q = f"SELECT * FROM {base} WHERE {' AND '.join(conditions)}"  # noqa: S608
            parts.append(JdbcInputPartition(i, q, self.conn_str))

        return parts

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def read(self, partition: InputPartition) -> Iterator[pa.RecordBatch]:
        if not isinstance(partition, JdbcInputPartition):
            msg = f"Expected JdbcInputPartition, got {type(partition)}"
            raise TypeError(msg)

        cx = _import_connectorx()
        try:
            table: pa.Table = cx.read_sql(partition.conn_str, partition.query, return_type="arrow")
        except Exception as e:
            msg = f"JDBC read failed. Query: {partition.query!r}. Error: {e}"
            raise RuntimeError(msg) from e

        yield from table.to_batches()


def _build_where(filters: list[str]) -> str:
    if not filters:
        return ""
    return " WHERE " + " AND ".join(filters)


# ============================================================================
# Write engine
# ============================================================================
#
# Bulk writes: ADBC ``adbc_ingest`` for PostgreSQL (binary COPY), DDL via psycopg.
# Overwrite modes:
#   * append   — ingest into the target (must exist). At-least-once: a retried task
#                re-ingests, so duplicates are possible (as with Spark's JDBC writer).
#   * atomic   — ingest into a staging table; commit() RENAMEs it over the target in
#                one txn. Never leaves the target partially written, but does NOT
#                preserve grants/RLS/FK back-references (use truncate if they must survive).
#   * truncate — advisory lock lets one partition TRUNCATE, then all ingest directly.
#                Preserves the table object but is NON-ATOMIC (target left partial if a
#                task dies mid-run). Prefer atomic unless grants/RLS must survive.
#
# Concurrent overwrites to the same table are unsupported: the final RENAME (atomic)
# or shared TRUNCATE (truncate) can race another job. Run overwrites one at a time.
# Failed cleanup can orphan ``*__sail_stg_*`` / ``*__sail_trunc_*`` tables — safe to drop.


_STAGING_PREFIX = "__sail_stg_"
_TRUNC_SENTINEL_PREFIX = "__sail_trunc_"


def _split_schema(qualified: str) -> tuple[str | None, str]:
    """Split a possibly schema-qualified name into ``(schema_or_None, table)``.

    ADBC ingest and SQLAlchemy take the schema as a separate argument, so the parts
    must stay apart. ``"public.orders"`` -> ``("public", "orders")``.
    """
    schema, sep, table = qualified.rpartition(".")
    return (schema, table) if sep else (None, qualified)


def _staging_name_atomic(dbtable: str, run_id: str) -> str:
    """Per-run atomic staging table name, in the target's schema so the RENAME keeps
    it there. The run-id suffix isolates concurrent writers.
    """
    schema, table = _split_schema(dbtable)
    staging = f"{table}{_STAGING_PREFIX}{run_id}"
    return f"{schema}.{staging}" if schema else staging


def _staging_name_truncate_sentinel(dbtable: str, run_id: str) -> str:
    """Return the per-run sentinel table name used by the truncate-mode advisory lock."""
    schema, table = _split_schema(dbtable)
    sentinel = f"{table}{_TRUNC_SENTINEL_PREFIX}{run_id}"
    return f"{schema}.{sentinel}" if schema else sentinel


def _safe_error(exc: BaseException, dsn: str) -> str:
    """Return ``str(exc)`` with the DSN (and any ``scheme://creds@host``) scrubbed.

    ADBC wraps libpq errors that may embed the DSN with credentials; strip to avoid leaks.
    """
    import re  # noqa: PLC0415

    scrubbed = re.sub(r"[a-zA-Z][a-zA-Z0-9+\-.]*://[^\s,;)]*", "<dsn-redacted>", str(exc))
    if dsn and dsn in scrubbed:
        scrubbed = scrubbed.replace(dsn, "<dsn-redacted>")
    return scrubbed


@dataclass
class PartitionResult:
    """Carry-token returned by :meth:`PgWriteEngine.write_partition`."""

    partition_id: int
    rows_written: int
    staging_table: str | None  # set only in atomic overwrite mode


class PgWriteEngine:
    """PostgreSQL write engine: ADBC ``adbc_ingest`` for bulk ingest, psycopg for DDL."""

    _VALID_MODES: frozenset[str] = frozenset({"append", "atomic", "truncate"})

    def __init__(
        self,
        *,
        dsn: str,
        dbtable: str,
        overwrite_mode: str = "append",
        batch_size: int = 65_536,
        run_id: str | None = None,
    ) -> None:
        if overwrite_mode not in self._VALID_MODES:
            msg = f"Invalid overwrite_mode {overwrite_mode!r}. Valid values: {sorted(self._VALID_MODES)}"
            raise ValueError(msg)

        self.dsn = dsn
        self.dbtable = dbtable
        self.overwrite_mode = overwrite_mode
        self.batch_size = batch_size
        if run_id is None:
            import uuid  # noqa: PLC0415

            run_id = uuid.uuid4().hex[:12]
        self.run_id = run_id

    # ------------------------------------------------------------------
    # Private per-mode write helpers (executor side)
    # ------------------------------------------------------------------

    def _prepare_atomic(self, qstaging: str, qtarget: str) -> None:
        """Create the staging table (matching the target) if it does not exist."""
        import psycopg  # noqa: PLC0415

        try:
            with psycopg.connect(self.dsn, autocommit=True) as conn, conn.cursor() as cur:
                cur.execute(f"CREATE TABLE IF NOT EXISTS {qstaging} (LIKE {qtarget} INCLUDING ALL)")
        except Exception as e:
            safe_msg = _safe_error(e, self.dsn)
            msg = f"Failed to create atomic staging table {qstaging!r}: {safe_msg}"
            raise RuntimeError(msg) from e

    def _prepare_truncate(self, qtarget: str) -> None:
        """Truncate the target exactly once using a distributed advisory lock.

        A sentinel table records which partition performed the TRUNCATE so that
        concurrent partitions skip it.
        """
        import hashlib  # noqa: PLC0415

        import psycopg  # noqa: PLC0415

        lock_key = int(hashlib.md5(f"sail_trunc_{self.dbtable}".encode()).hexdigest()[:15], 16) % (2**63)  # noqa: S324
        sentinel = _staging_name_truncate_sentinel(self.dbtable, self.run_id)
        qsentinel = _quote_qualified(sentinel)

        try:
            with psycopg.connect(self.dsn, autocommit=True) as lock_conn, lock_conn.cursor() as cur:
                cur.execute(f"SELECT pg_advisory_lock({lock_key})")
                try:
                    cur.execute(f"CREATE TABLE IF NOT EXISTS {qsentinel} (done BOOLEAN)")
                    cur.execute(f"SELECT COUNT(*) FROM {qsentinel}")  # noqa: S608
                    row = cur.fetchone()
                    if row[0] == 0:  # type: ignore[index]
                        cur.execute(f"TRUNCATE {qtarget}")
                        cur.execute(f"INSERT INTO {qsentinel} VALUES (TRUE)")  # noqa: S608
                finally:
                    cur.execute(f"SELECT pg_advisory_unlock({lock_key})")
        except Exception as e:
            safe_msg = _safe_error(e, self.dsn)
            msg = f"Truncate-mode advisory-lock failed for {self.dbtable!r}: {safe_msg}"
            raise RuntimeError(msg) from e

    # ------------------------------------------------------------------
    # Executor side
    # ------------------------------------------------------------------

    def write_partition(self, partition_id: int, batches: Iterable[pa.RecordBatch]) -> PartitionResult:
        """Write one partition. Returns a :class:`PartitionResult` for the driver."""
        import adbc_driver_postgresql.dbapi as pg_dbapi  # noqa: PLC0415

        collected = [b for b in batches if b.num_rows > 0]
        table_obj = pa.Table.from_batches(collected) if collected else None

        staging: str | None = None

        if self.overwrite_mode == "atomic":
            staging = _staging_name_atomic(self.dbtable, self.run_id)
            self._prepare_atomic(_quote_qualified(staging), _quote_qualified(self.dbtable))
            target = staging
        elif self.overwrite_mode == "truncate":
            self._prepare_truncate(_quote_qualified(self.dbtable))
            target = self.dbtable
        else:  # append
            target = self.dbtable

        rows = 0
        if table_obj is not None and table_obj.num_rows > 0:
            ingest_schema, ingest_table = _split_schema(target)
            try:
                with pg_dbapi.connect(self.dsn) as conn:
                    with conn.cursor() as cur:
                        cur.adbc_ingest(ingest_table, table_obj, mode="append", db_schema_name=ingest_schema)
                    conn.commit()
                rows = table_obj.num_rows
            except Exception as e:
                safe_msg = _safe_error(e, self.dsn)
                msg = f"ADBC ingest failed for partition {partition_id} into {target!r}: {safe_msg}"
                raise RuntimeError(msg) from e

        return PartitionResult(partition_id=partition_id, rows_written=rows, staging_table=staging)

    # ------------------------------------------------------------------
    # Driver side
    # ------------------------------------------------------------------

    def commit(self, results: list[PartitionResult]) -> int:
        """Finalise the write. Returns total row count.

        ``atomic`` renames the staging table over the target in one transaction;
        ``truncate`` drops the sentinel (rows are already in the target).
        """
        total = sum(r.rows_written for r in results)

        if self.overwrite_mode == "append":
            return total

        import psycopg  # noqa: PLC0415

        if self.overwrite_mode == "atomic":
            staging_name = _staging_name_atomic(self.dbtable, self.run_id)
            qstaging = _quote_qualified(staging_name)
            qtarget = _quote_qualified(self.dbtable)
            try:
                with psycopg.connect(self.dsn) as conn:  # NOT autocommit — single txn
                    with conn.cursor() as cur:
                        cur.execute(f"DROP TABLE IF EXISTS {qtarget}")
                        # RENAME target is the bare table name (staging's schema is kept).
                        qrename = _quote_qualified(_split_schema(self.dbtable)[1])
                        cur.execute(f"ALTER TABLE {qstaging} RENAME TO {qrename}")
                    conn.commit()
            except Exception as e:
                safe_msg = _safe_error(e, self.dsn)
                msg = f"Atomic overwrite commit failed (target {self.dbtable!r} may be missing): {safe_msg}"
                raise RuntimeError(msg) from e

        elif self.overwrite_mode == "truncate":
            sentinel = _staging_name_truncate_sentinel(self.dbtable, self.run_id)
            qsentinel = _quote_qualified(sentinel)
            try:
                with psycopg.connect(self.dsn, autocommit=True) as conn, conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {qsentinel}")
            except Exception:  # noqa: BLE001, S110
                pass  # sentinel cleanup failure must not mask success

        return total

    def abort(self, results: list[PartitionResult]) -> None:  # noqa: ARG002
        """Drop staging / sentinel tables created during a failed write."""
        if self.overwrite_mode == "append":
            return

        import psycopg  # noqa: PLC0415

        try:
            with psycopg.connect(self.dsn, autocommit=True) as conn, conn.cursor() as cur:
                if self.overwrite_mode == "atomic":
                    staging_name = _staging_name_atomic(self.dbtable, self.run_id)
                    cur.execute(f"DROP TABLE IF EXISTS {_quote_qualified(staging_name)}")
                elif self.overwrite_mode == "truncate":
                    sentinel = _staging_name_truncate_sentinel(self.dbtable, self.run_id)
                    cur.execute(f"DROP TABLE IF EXISTS {_quote_qualified(sentinel)}")
                    # Partitions wrote directly to target — those writes cannot be undone here.
        except Exception:  # noqa: BLE001, S110 — abort must not mask original error
            pass


def _split_sqlserver_authority(authority: str) -> tuple[str, str, str | None, str | None]:
    """Split ``[user[:pass]@]host[\\instance][:port]`` into its parts.

    Returns ``(userinfo, host, instance_or_None, port_or_None)`` where *userinfo*
    is the empty string when no credentials are present (it already includes the
    trailing ``@`` when non-empty, so it can be concatenated directly).
    """
    userinfo = ""
    at = authority.rfind("@")
    if at != -1:
        userinfo = authority[: at + 1]  # keep the '@'
        hostport = authority[at + 1 :]
    else:
        hostport = authority

    port: str | None = None
    if ":" in hostport:
        hostport, _, port = hostport.rpartition(":")
        port = port or None

    instance: str | None = None
    if "\\" in hostport:
        hostport, _, instance = hostport.partition("\\")
        instance = instance or None

    return userinfo, hostport, instance, port


def _parse_sqlserver_url(rest: str) -> tuple[str, dict[str, object]]:
    """Parse the JDBC SQL Server tail (after ``sqlserver://``) into a
    ``(sqlalchemy_url, connect_args)`` pair for the ``mssql+pymssql`` dialect.

    Form: ``[user:pass@]host[\\instance][:port][;key=value[;...]]``. Known params:
    ``databaseName`` -> URL db segment; ``encrypt`` -> ``encryption`` (require/off);
    ``applicationIntent=ReadOnly`` -> ``read_only=True``. ``trustServerCertificate`` is
    accepted but ignored (FreeTDS controls trust, not a per-connection flag). Unknown
    params are dropped (pymssql would reject them).
    """
    authority, _, param_str = rest.partition(";")
    userinfo, host, instance, port = _split_sqlserver_authority(authority)

    params: dict[str, str] = {}
    if param_str:
        for raw in param_str.split(";"):
            if not raw.strip():
                continue
            key, _, value = raw.partition("=")
            params[key.strip().lower()] = value.strip()

    database = params.get("databasename", "")

    # pymssql addresses a named instance via ``server\instance`` (TDS resolves the port).
    host_segment = f"{host}\\{instance}" if instance else host
    netloc = f"{userinfo}{host_segment}"
    if port is not None:
        netloc += f":{port}"

    url = f"mssql+pymssql://{netloc}/{database}" if database else f"mssql+pymssql://{netloc}"

    connect_args: dict[str, object] = {}
    encrypt = params.get("encrypt")
    if encrypt is not None:
        connect_args["encryption"] = "require" if encrypt.lower() == "true" else "off"
    if params.get("applicationintent", "").lower() == "readonly":
        connect_args["read_only"] = True

    return url, connect_args


def _sqlalchemy_url(dsn: str) -> tuple[str, dict]:
    """Translate a stripped JDBC DSN into a ``(SQLAlchemy URL, connect_args)`` pair.

    ``connect_args`` is empty for MySQL and carries the mapped SQL Server
    connection options (see :func:`_parse_sqlserver_url`).
    """
    scheme, sep, rest = dsn.partition("://")
    if not sep:
        msg = f"Cannot build a SQLAlchemy URL from {dsn!r}"
        raise ValueError(msg)
    if scheme == "mysql":
        return f"mysql+pymysql://{rest}", {}
    if scheme == "sqlserver":
        return _parse_sqlserver_url(rest)
    msg = f"Unsupported JDBC subprotocol for writes: {scheme!r}"
    raise ValueError(msg)


class SqlAlchemyWriteEngine:
    """Fallback write engine for non-PostgreSQL dialects (MySQL, SQL Server).

    Rows go through a parameterised SQLAlchemy-core ``INSERT`` built from the Arrow
    table's Python values (``to_pylist``), which preserves exact ints (bigints > 2**53)
    and keeps NULL distinct from 0 — unlike a pandas ``to_sql`` float64 round-trip.

    * ``append``    ingests into the target. At-least-once: a retried task re-inserts,
      so duplicates are possible (as with Spark's JDBC writer); use a unique constraint.
    * ``overwrite`` loads each partition into its own staging table, then the driver
      swaps them in via one ``DELETE`` + ``INSERT ... SELECT`` transaction.

    Concurrent overwrites to the same table are unsupported: two jobs can interleave
    their DELETE/INSERT and leave a mixed result. Run overwrites one at a time.
    Failed cleanup can orphan ``*__sail_stg_*`` tables — safe to drop.
    """

    def __init__(
        self,
        *,
        url: str,
        dbtable: str,
        columns: list[str],
        overwrite: bool,
        batch_size: int,
        run_id: str,
        connect_args: dict | None = None,
    ) -> None:
        self.url = url
        self.dbtable = dbtable
        self.columns = columns
        self.overwrite = overwrite
        self.batch_size = batch_size
        self.run_id = run_id
        self.connect_args = connect_args or {}
        self.schema, self.table = _split_schema(dbtable)

    def _create_engine(self):
        import sqlalchemy as sa  # noqa: PLC0415
        from sqlalchemy import NullPool  # noqa: PLC0415

        # Short-lived, single-connection engine — skip the connection pool.
        return sa.create_engine(self.url, poolclass=NullPool, connect_args=self.connect_args)

    def _staging(self, partition_id: int) -> str:
        return f"{self.table}{_STAGING_PREFIX}{self.run_id}_{partition_id}"

    @staticmethod
    def _qualified(prep, schema: str | None, name: str) -> str:
        return f"{prep.quote(schema)}.{prep.quote(name)}" if schema else prep.quote(name)

    def _reflect_table(self, engine, table_name: str):
        """Reflect *table_name* from the live database into an ``sa.Table``."""
        import sqlalchemy as sa  # noqa: PLC0415

        return sa.Table(table_name, sa.MetaData(), schema=self.schema, autoload_with=engine)

    def _insert_arrow(self, engine, sa_table, table_obj: pa.Table) -> None:
        """Insert an Arrow table via a parameterised INSERT, in ``batch_size`` chunks.

        *sa_table*'s column SQL types drive the bind (ints stay ints); ``to_pylist`` per
        chunk lets int/None flow through without float coercion or materialising all rows.
        """
        import sqlalchemy as sa  # noqa: PLC0415

        n = table_obj.num_rows
        if n == 0:
            return
        with engine.begin() as conn:
            for start in range(0, n, self.batch_size):
                chunk = table_obj.slice(start, self.batch_size).to_pylist()
                conn.execute(sa.insert(sa_table), chunk)

    def _create_staging_like_target(self, engine, staging: str):
        """Create an empty staging table matching the target's columns; return its
        ``sa.Table`` so the caller can insert without a second reflection round-trip.
        """
        import sqlalchemy as sa  # noqa: PLC0415

        target = self._reflect_table(engine, self.table)
        staging_cols = [
            sa.Column(c.name, c.type, nullable=c.nullable, primary_key=c.primary_key) for c in target.columns
        ]
        # run_id/partition_id suffix makes names collision-free; prior orphans are
        # handled by abort()/cleanup, not a pre-create drop.
        staging_table = sa.Table(staging, sa.MetaData(), *staging_cols, schema=self.schema)
        with engine.begin() as conn:
            staging_table.create(conn)
        return staging_table

    def write_partition(self, partition_id: int, batches: Iterable[pa.RecordBatch]) -> PartitionResult:
        collected = [b for b in batches if b.num_rows > 0]
        table_obj = pa.Table.from_batches(collected) if collected else None

        staging: str | None = None
        rows = 0
        engine = self._create_engine()
        try:
            if table_obj is not None and table_obj.num_rows > 0:
                if self.overwrite:
                    staging = self._staging(partition_id)
                    staging_table = self._create_staging_like_target(engine, staging)
                    self._insert_arrow(engine, staging_table, table_obj)
                else:
                    target_table = self._reflect_table(engine, self.table)
                    self._insert_arrow(engine, target_table, table_obj)
                rows = table_obj.num_rows
        except Exception as e:
            safe_msg = _safe_error(e, self.url)
            msg = f"SQLAlchemy write failed for partition {partition_id} into {self.dbtable!r}: {safe_msg}"
            raise RuntimeError(msg) from e
        finally:
            engine.dispose()

        return PartitionResult(partition_id=partition_id, rows_written=rows, staging_table=staging)

    def commit(self, results: list[PartitionResult]) -> int:
        total = sum(r.rows_written for r in results)
        if not self.overwrite:
            return total

        import sqlalchemy as sa  # noqa: PLC0415

        engine = self._create_engine()
        prep = engine.dialect.identifier_preparer
        qtarget = self._qualified(prep, self.schema, self.table)
        cols = ", ".join(prep.quote(c) for c in self.columns)
        stagings = [r.staging_table for r in results if r.staging_table]
        try:
            with engine.begin() as conn:
                conn.execute(sa.text(f"DELETE FROM {qtarget}"))  # noqa: S608
                for staging in stagings:
                    qstaging = self._qualified(prep, self.schema, staging)
                    conn.execute(sa.text(f"INSERT INTO {qtarget} ({cols}) SELECT {cols} FROM {qstaging}"))  # noqa: S608
            # Swap already committed; staging cleanup is best-effort, must not fail the job.
            with contextlib.suppress(Exception):
                self._drop_stagings(engine, stagings, prep)
        except Exception as e:
            safe_msg = _safe_error(e, self.url)
            msg = f"SQLAlchemy overwrite commit failed for {self.dbtable!r}: {safe_msg}"
            raise RuntimeError(msg) from e
        finally:
            engine.dispose()
        return total

    def abort(self, results: list[PartitionResult]) -> None:
        if not self.overwrite:
            return
        engine = self._create_engine()
        try:
            self._drop_stagings(
                engine, [r.staging_table for r in results if r.staging_table], engine.dialect.identifier_preparer
            )
        except Exception:  # noqa: BLE001, S110 — abort must not mask the original error
            pass
        finally:
            engine.dispose()

    def _drop_stagings(self, engine, stagings: list[str], prep) -> None:
        import sqlalchemy as sa  # noqa: PLC0415

        with engine.begin() as conn:
            for staging in stagings:
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {self._qualified(prep, self.schema, staging)}"))


@dataclass
class _JdbcCommitMessage(WriterCommitMessage):
    result: PartitionResult


class _ArrowWriter(DataSourceArrowWriter):
    """:class:`DataSourceArrowWriter` adapter delegating to a write *engine*.

    The engine (``PgWriteEngine`` or ``SqlAlchemyWriteEngine``) exposes
    ``write_partition`` / ``commit`` / ``abort``.  Spark passes ``pa.RecordBatch``
    iterators, so there is no Row serialisation overhead.
    """

    def __init__(self, engine) -> None:
        self._engine = engine

    def write(self, iterator: Iterator[pa.RecordBatch]) -> WriterCommitMessage:
        from pyspark import TaskContext  # noqa: PLC0415

        ctx = TaskContext.get()
        pid = ctx.partitionId() if ctx is not None else 0
        return _JdbcCommitMessage(self._engine.write_partition(pid, iterator))

    def commit(self, messages: list[WriterCommitMessage]) -> None:
        self._engine.commit([m.result for m in messages if isinstance(m, _JdbcCommitMessage)])

    def abort(self, messages: list[WriterCommitMessage]) -> None:
        self._engine.abort([m.result for m in messages if isinstance(m, _JdbcCommitMessage)])


class JdbcDataSourceWriter(_ArrowWriter):
    """PostgreSQL ADBC writer (kept as a named class for backward compatibility)."""

    def __init__(
        self, *, conn_str: str, dbtable: str, overwrite_mode: str, batch_size: int, run_id: str | None = None
    ) -> None:
        super().__init__(
            PgWriteEngine(
                dsn=conn_str,
                dbtable=dbtable,
                overwrite_mode=overwrite_mode,
                batch_size=batch_size,
                run_id=run_id,
            )
        )


class SqlAlchemyDataSourceWriter(_ArrowWriter):
    """Fallback writer for non-PostgreSQL dialects."""

    def __init__(
        self,
        *,
        url: str,
        dbtable: str,
        columns: list[str],
        overwrite: bool,
        batch_size: int,
        run_id: str,
        connect_args: dict | None = None,
    ) -> None:
        super().__init__(
            SqlAlchemyWriteEngine(
                url=url,
                dbtable=dbtable,
                columns=columns,
                overwrite=overwrite,
                batch_size=batch_size,
                run_id=run_id,
                connect_args=connect_args,
            )
        )


# ============================================================================
# DataSource
# ============================================================================


class JdbcDataSource(DataSource):
    """JDBC data source backed by connectorX.

    Register and use::

        from pysail.spark.datasource.jdbc import JdbcDataSource

        spark.dataSource.register(JdbcDataSource)

        # Using format("jdbc")
        df = (
            spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/mydb")
            .option("dbtable", "public.users")
            .option("user", "alice")
            .option("password", "secret")
            .load()
        )

        # Using spark.read.jdbc() shorthand (provided by PySpark Connect)
        df = spark.read.jdbc(
            "jdbc:postgresql://localhost:5432/mydb",
            "public.users",
            properties={"user": "alice", "password": "secret"},
        )

    Supported options (consistent with PySpark JDBC options):

    +--------------------+----------+---------+--------------------------------------------+
    | Option             | Required | Default | Description                                |
    +====================+==========+=========+============================================+
    | url                | Yes      |         | JDBC URL (jdbc:<proto>://host:port/db)     |
    +--------------------+----------+---------+--------------------------------------------+
    | dbtable            | Yes*     |         | Table name (mutually exclusive with query) |
    +--------------------+----------+---------+--------------------------------------------+
    | query              | Yes*     |         | SQL query (mutually exclusive with dbtable)|
    +--------------------+----------+---------+--------------------------------------------+
    | user               | No       |         | Database username                          |
    +--------------------+----------+---------+--------------------------------------------+
    | password           | No       |         | Database password                          |
    +--------------------+----------+---------+--------------------------------------------+
    | partitionColumn    | No       |         | Column for range partitioning              |
    +--------------------+----------+---------+--------------------------------------------+
    | lowerBound         | No       |         | Lower bound of partition stride            |
    +--------------------+----------+---------+--------------------------------------------+
    | upperBound         | No       |         | Upper bound of partition stride            |
    +--------------------+----------+---------+--------------------------------------------+
    | numPartitions      | No       | 1       | Number of parallel partitions              |
    +--------------------+----------+---------+--------------------------------------------+
    | fetchsize          | No       | 0       | Rows per round-trip hint (advisory)        |
    +--------------------+----------+---------+--------------------------------------------+
    | pushDownPredicate  | No       | true    | Push WHERE filters to the database         |
    +--------------------+----------+---------+--------------------------------------------+
    | customSchema       | No       |         | Spark DDL to override inferred types       |
    +--------------------+----------+---------+--------------------------------------------+

    * Exactly one of ``dbtable`` or ``query`` is required.

    Not supported: ``driver``, ``predicates`` list, ``queryTimeout``,
    ``isolationLevel``, ``sessionInitStatement``, Kerberos.
    """

    @classmethod
    def name(cls) -> str:
        return "jdbc"

    # ------------------------------------------------------------------
    # Options resolution + validation
    # ------------------------------------------------------------------

    def _resolve_options(self) -> dict:
        opts = self.options

        # --- url ---
        url = opts.get("url")
        if not url:
            msg = "Option 'url' is required for the jdbc data source"
            raise ValueError(msg)
        if not url.startswith("jdbc:"):
            msg = f"Invalid JDBC URL: {url!r}. Expected format: jdbc:<subprotocol>://<host>:<port>/<database>"
            raise ValueError(msg)

        # --- table source ---
        dbtable = opts.get("dbtable")
        if dbtable is not None and not dbtable.strip():
            msg = "Option 'dbtable' cannot be empty"
            raise ValueError(msg)
        dbtable = dbtable or None
        query = opts.get("query") or None

        if dbtable and query:
            msg = "Options 'dbtable' and 'query' are mutually exclusive. Specify only one."
            raise ValueError(msg)
        if not dbtable and not query:
            msg = "Either 'dbtable' or 'query' must be specified for the jdbc data source."
            raise ValueError(msg)

        # --- auth ---
        user = opts.get("user") or None
        password = opts.get("password") or None
        conn_str = _jdbc_url_to_dsn(url, user, password)

        # --- partitioning ---
        num_partitions = int(opts.get("numPartitions", "1"))
        partition_column = opts.get("partitionColumn") or None
        lower_bound_raw = opts.get("lowerBound") or None
        upper_bound_raw = opts.get("upperBound") or None

        partition_opts_present = sum(v is not None for v in [partition_column, lower_bound_raw, upper_bound_raw])

        if partition_opts_present > 0:
            if partition_opts_present < 3:  # noqa: PLR2004
                missing = [
                    name
                    for name, val in [
                        ("partitionColumn", partition_column),
                        ("lowerBound", lower_bound_raw),
                        ("upperBound", upper_bound_raw),
                    ]
                    if val is None
                ]
                msg = (
                    "When using range partitioning, all of 'partitionColumn', "
                    "'lowerBound', 'upperBound', and 'numPartitions' must be specified. "
                    f"Missing: {missing}"
                )
                raise ValueError(msg)
            if num_partitions <= 1:
                msg = "Partitioning options 'partitionColumn', 'lowerBound', 'upperBound' require 'numPartitions' > 1."
                raise ValueError(msg)
            if query is not None:
                msg = (
                    "Options 'partitionColumn' and 'query' are incompatible. "
                    "Wrap your query in 'dbtable' as a subquery: "
                    "dbtable='(SELECT ...) AS subq'"
                )
                raise ValueError(msg)

        try:
            lower_bound = int(lower_bound_raw) if lower_bound_raw is not None else None
            upper_bound = int(upper_bound_raw) if upper_bound_raw is not None else None
        except ValueError:
            msg = (
                "'lowerBound' and 'upperBound' must be integers. "
                f"Got lowerBound={lower_bound_raw!r}, upperBound={upper_bound_raw!r}."
            )
            raise ValueError(msg) from None

        if lower_bound is not None and upper_bound is not None and lower_bound >= upper_bound:
            msg = f"'lowerBound' ({lower_bound}) must be strictly less than 'upperBound' ({upper_bound})."
            raise ValueError(msg)

        push_down_predicate = opts.get("pushDownPredicate", "true").lower() == "true"

        return {
            "conn_str": conn_str,
            "dbtable": dbtable,
            "query": query,
            "num_partitions": num_partitions,
            "partition_column": partition_column,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "push_down_predicate": push_down_predicate,
        }

    # ------------------------------------------------------------------
    # Schema inference
    # ------------------------------------------------------------------

    def schema(self) -> pa.Schema:
        resolved = self._resolve_options()
        conn_str = resolved["conn_str"]
        dbtable = resolved["dbtable"]
        query = resolved["query"]

        if query is not None:
            schema_query = f"SELECT * FROM ({query}) AS _cx_schema_q LIMIT 0"  # noqa: S608
        else:
            schema_query = f"SELECT * FROM {dbtable} LIMIT 0"  # noqa: S608

        cx = _import_connectorx()
        try:
            table: pa.Table = cx.read_sql(conn_str, schema_query, return_type="arrow")
        except Exception as e:
            msg = f"Failed to infer schema from JDBC source. Query: {schema_query!r}. Error: {e}"
            raise RuntimeError(msg) from e

        inferred = table.schema

        custom_schema_ddl = self.options.get("customSchema") or None
        if custom_schema_ddl:
            inferred = _apply_custom_schema(inferred, custom_schema_ddl)

        return inferred

    # ------------------------------------------------------------------
    # Reader
    # ------------------------------------------------------------------

    def reader(self, schema: pa.Schema) -> JdbcDataSourceReader:  # noqa: ARG002
        resolved = self._resolve_options()
        return JdbcDataSourceReader(**resolved)

    # ------------------------------------------------------------------
    # Writer
    # ------------------------------------------------------------------

    def writer(self, schema: pa.Schema, overwrite: bool) -> DataSourceArrowWriter:  # noqa: FBT001
        """Return a writer for the target database.

        PostgreSQL uses ADBC bulk ingest; MySQL and SQL Server use a SQLAlchemy
        fallback.  Options:

        * ``url`` — required JDBC URL (``jdbc:<dialect>://...``)
        * ``dbtable`` — required; ``query`` is rejected (cannot write to a query)
        * ``user`` / ``password`` — optional credentials
        * ``batchsize`` — rows per ingest call (default 65 536)
        * ``overwriteMode`` — ``"atomic"`` (default) or ``"truncate"``; PostgreSQL
          only, consulted when *overwrite* is ``True``

        Usage::

            df.write.format("jdbc") \\
                .option("url", "jdbc:postgresql://localhost:5432/mydb") \\
                .option("dbtable", "public.events") \\
                .mode("overwrite") \\
                .save()
        """
        opts = self.options

        url = opts.get("url")
        if not url:
            msg = "Option 'url' is required for the jdbc data source"
            raise ValueError(msg)
        subprotocol = url[5:].split("://", 1)[0].split(":", 1)[0] if url.startswith("jdbc:") else ""

        if opts.get("query"):
            msg = "Cannot write to a 'query'; specify 'dbtable' (a table name) for writes."
            raise ValueError(msg)

        dbtable = opts.get("dbtable")
        if not dbtable:
            msg = "Option 'dbtable' is required for jdbc writes."
            raise ValueError(msg)

        user = opts.get("user") or None
        password = opts.get("password") or None
        conn_str = _jdbc_url_to_dsn(url, user, password)
        batch_size = int(opts.get("batchsize", "65536"))

        import uuid  # noqa: PLC0415

        run_id = uuid.uuid4().hex[:12]

        if subprotocol == "postgresql":
            if overwrite:
                overwrite_mode = opts.get("overwriteMode", "atomic")
                valid = {"atomic", "truncate"}
                if overwrite_mode not in valid:
                    msg = f"Invalid overwriteMode {overwrite_mode!r}. Valid values: {sorted(valid)}"
                    raise ValueError(msg)
            else:
                overwrite_mode = "append"
            return JdbcDataSourceWriter(
                conn_str=conn_str,
                dbtable=dbtable,
                overwrite_mode=overwrite_mode,
                batch_size=batch_size,
                run_id=run_id,
            )

        if subprotocol in ("mysql", "sqlserver"):
            sa_url, connect_args = _sqlalchemy_url(conn_str)
            return SqlAlchemyDataSourceWriter(
                url=sa_url,
                dbtable=dbtable,
                columns=list(schema.names),
                overwrite=overwrite,
                batch_size=batch_size,
                run_id=run_id,
                connect_args=connect_args,
            )

        msg = f"The jdbc write path supports PostgreSQL, MySQL and SQL Server. Got subprotocol {subprotocol!r}."
        raise ValueError(msg)


# ============================================================================
# Custom schema helpers
# ============================================================================

# Mapping from lowercase Spark SQL type names → PyArrow types
_SPARK_TO_ARROW: dict[str, pa.DataType] = {
    "byte": pa.int8(),
    "tinyint": pa.int8(),
    "short": pa.int16(),
    "smallint": pa.int16(),
    "int": pa.int32(),
    "integer": pa.int32(),
    "long": pa.int64(),
    "bigint": pa.int64(),
    "float": pa.float32(),
    "real": pa.float32(),
    "double": pa.float64(),
    "string": pa.large_utf8(),
    "varchar": pa.large_utf8(),
    "char": pa.large_utf8(),
    "binary": pa.large_binary(),
    "boolean": pa.bool_(),
    "bool": pa.bool_(),
    "date": pa.date32(),
    "timestamp": pa.timestamp("us"),
    "timestamp_ntz": pa.timestamp("us", tz=None),
}


def _parse_custom_schema(ddl: str) -> dict[str, pa.DataType]:
    """Parse a Spark DDL schema string into a ``{column_name: arrow_type}`` mapping.

    Handles simple and DECIMAL/NUMERIC types.  Unknown types are skipped.
    Example input: ``"id DECIMAL(38,0), name STRING, active BOOLEAN"``
    """
    result: dict[str, pa.DataType] = {}

    # Split on commas that are NOT inside parentheses so DECIMAL(p,s) stays intact.
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in ddl:
        if ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current).strip())

    for part in parts:
        stripped = part.strip()
        if not stripped:
            continue
        tokens = stripped.split(None, 1)
        if len(tokens) != 2:  # noqa: PLR2004
            continue
        col_name, type_str = tokens
        type_upper = type_str.strip().upper()
        base_type = type_upper.split("(")[0].strip().lower()

        if base_type in ("decimal", "numeric"):
            if "(" in type_upper and ")" in type_upper:
                inner = type_upper[type_upper.index("(") + 1 : type_upper.index(")")]
                params = inner.split(",")
                precision = int(params[0].strip())
                scale = int(params[1].strip()) if len(params) > 1 else 0
            else:
                precision, scale = 38, 18
            result[col_name.lower()] = pa.decimal128(precision, scale)
        else:
            arrow_type = _SPARK_TO_ARROW.get(base_type)
            if arrow_type is not None:
                result[col_name.lower()] = arrow_type

    return result


def _apply_custom_schema(schema: pa.Schema, ddl: str) -> pa.Schema:
    """Override column types in *schema* using a Spark DDL string."""
    overrides = _parse_custom_schema(ddl)
    if not overrides:
        return schema

    new_fields = [pa.field(f.name, overrides.get(f.name.lower(), f.type), nullable=f.nullable) for f in schema]
    return pa.schema(new_fields)
