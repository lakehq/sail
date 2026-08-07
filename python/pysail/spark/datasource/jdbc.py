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
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

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
        InputPartition,
        WriterCommitMessage,
    )
except ImportError as e:
    msg = "PySpark with the Python DataSource API is required (PySpark >= 4.0)"
    raise ImportError(msg) from e

try:
    from pyspark.sql.datasource import (
        EqualTo,
        Filter,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual,
    )

    _HAS_FILTER_PUSHDOWN = True
except ImportError:
    _HAS_FILTER_PUSHDOWN = False


# ============================================================================
# URL helpers
# ============================================================================


def _redact_credentials(url: str) -> str:
    """Mask any ``user:pass@`` userinfo in a URL/DSN before it appears in an error message."""
    import re  # noqa: PLC0415

    return re.sub(r"(://)[^/@\s]*@", r"\1<redacted>@", url)


def _jdbc_url_to_dsn(url: str, user: str | None, password: str | None) -> str:
    """Strip the ``jdbc:`` prefix and embed credentials into the DSN.

    Examples::

        jdbc:postgresql://host:5432/db  ->  postgresql://user:pass@host:5432/db
    """
    if not url.startswith("jdbc:"):
        msg = f"Invalid JDBC URL: {_redact_credentials(url)!r}. Expected format: jdbc:<subprotocol>://<host>:<port>/<database>"
        raise ValueError(msg)
    dsn = url[5:]  # strip 'jdbc:'

    if user is not None or password is not None:
        sep = "://"
        try:
            idx = dsn.index(sep)
        except ValueError:
            msg = f"Invalid JDBC URL: {_redact_credentials(url)!r}. Expected format: jdbc:<subprotocol>://<host>:<port>/<database>"
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


def _postgresql_dsn_with_properties(dsn: str, options: dict[str, str]) -> str:
    """Map pgJDBC properties with exact libpq URI equivalents.

    Spark forwards non-core data-source options to the JDBC driver. Sail does
    not load that JVM driver, so only properties with identical libpq semantics
    are translated here. Values already present in the URL retain pgJDBC's
    documented precedence over the separate properties object.
    """
    names = {
        "applicationname": "application_name",
        "connecttimeout": "connect_timeout",
        "sslmode": "sslmode",
        "sslcert": "sslcert",
        "sslkey": "sslkey",
        "sslrootcert": "sslrootcert",
        "options": "options",
    }
    normalized = {key.lower(): value for key, value in options.items()}
    parts = urlsplit(dsn)
    query = [(names.get(key.lower(), key), value) for key, value in parse_qsl(parts.query, keep_blank_values=True)]
    present = {key.lower() for key, _ in query}
    for source, target in names.items():
        if source in normalized and target.lower() not in present:
            query.append((target, normalized[source]))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query, quote_via=quote), parts.fragment))


def _postgresql_dsn_with_timeout(dsn: str, query_timeout: int) -> str:
    if not query_timeout:
        return dsn
    parts = urlsplit(dsn)
    query = list(parse_qsl(parts.query, keep_blank_values=True))
    timeout = f"-c statement_timeout={query_timeout * 1000}"
    for index in range(len(query) - 1, -1, -1):
        key, value = query[index]
        if key.lower() == "options":
            query[index] = (key, f"{value} {timeout}".strip())
            break
    else:
        query.append(("options", timeout))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query, quote_via=quote), parts.fragment))


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
        if not _HAS_FILTER_PUSHDOWN:
            # Filter pushdown classes require PySpark >= 4.1; reject everything so
            # Sail applies the filters post-read instead.
            yield from filters
            return

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
# Overwrite modes (``spark`` is prepared on the driver, then uses append here):
#   * append   — ingest into the target (must exist). At-least-once: a retried task
#                re-ingests, so duplicates are possible (as with Spark's JDBC writer).
#   * atomic   — ingest into a shared staging table; commit() RENAMEs it over the target
#                in one txn. Never leaves the target partially written, but is at-least-once
#                under task retry (a re-run re-ingests into the shared staging, like append)
#                and does NOT preserve grants/RLS/FK back-refs (use truncate if they must).
#   * truncate — advisory lock lets one partition TRUNCATE, then all ingest directly.
#                Preserves the table object but is NON-ATOMIC (target left partial if a
#                task dies mid-run). This is an explicit Sail extension.
#
# Concurrent overwrites to the same table are unsupported: the final RENAME (atomic)
# or shared TRUNCATE (truncate) can race another job. Run overwrites one at a time.
# Failed cleanup can orphan ``*__sail_stg_*`` / ``*__sail_trunc_*`` tables — safe to drop.


_STAGING_PREFIX = "__sail_stg_"
_TRUNC_SENTINEL_PREFIX = "__sail_trunc_"
_POSTGRES_IDENTIFIER_BYTES = 63


def _split_schema(qualified: str) -> tuple[str | None, str]:
    """Split a possibly schema-qualified name into ``(schema_or_None, table)``.

    ADBC ingest and SQLAlchemy take the schema as a separate argument, so the parts
    must stay apart. ``"public.orders"`` -> ``("public", "orders")``.
    """
    schema, sep, table = qualified.rpartition(".")
    return (schema, table) if sep else (None, qualified)


def _postgres_generated_identifier(table: str, suffix: str) -> str:
    """Append *suffix* without relying on PostgreSQL's unsafe server truncation."""
    candidate = f"{table}{suffix}"
    if len(candidate.encode()) <= _POSTGRES_IDENTIFIER_BYTES:
        return candidate

    import hashlib  # noqa: PLC0415

    digest = hashlib.sha256(table.encode()).hexdigest()[:8]
    tail = f"_{digest}{suffix}"
    budget = _POSTGRES_IDENTIFIER_BYTES - len(tail.encode())
    prefix = table.encode()[:budget]
    while True:
        try:
            return f"{prefix.decode()}{tail}"
        except UnicodeDecodeError:
            prefix = prefix[:-1]


def _staging_name_atomic(dbtable: str, run_id: str) -> str:
    """Per-run atomic staging table name, in the target's schema so the RENAME keeps
    it there. The run-id suffix isolates concurrent writers.
    """
    schema, table = _split_schema(dbtable)
    staging = _postgres_generated_identifier(table, f"{_STAGING_PREFIX}{run_id}")
    return f"{schema}.{staging}" if schema else staging


def _staging_name_truncate_sentinel(dbtable: str, run_id: str) -> str:
    """Return the per-run sentinel table name used by the truncate-mode advisory lock."""
    schema, table = _split_schema(dbtable)
    sentinel = _postgres_generated_identifier(table, f"{_TRUNC_SENTINEL_PREFIX}{run_id}")
    return f"{schema}.{sentinel}" if schema else sentinel


def _iter_arrow_chunks(table_obj: pa.Table, batch_size: int) -> Iterator[pa.Table]:
    """Yield *table_obj* in ``batch_size``-row zero-copy slices."""
    for start in range(0, table_obj.num_rows, batch_size):
        yield table_obj.slice(start, batch_size)


def _owned_sequences(cur, dbtable: str) -> list[tuple[str, str]]:
    """Return ``(sequence, column)`` pairs for sequences OWNED BY a column of *dbtable*.

    A ``serial`` column's default reads such a sequence, which is dependency-linked to
    the target; the link must be broken before the target can be dropped in the atomic
    swap. ``to_regclass`` yields NULL (matching nothing) when the target is absent.
    """
    cur.execute(
        "SELECT dep.objid::regclass::text, att.attname "
        "FROM pg_depend dep "
        "JOIN pg_attribute att ON att.attrelid = dep.refobjid AND att.attnum = dep.refobjsubid "
        "WHERE dep.refobjid = to_regclass(%s) AND dep.classid = 'pg_class'::regclass AND dep.deptype = 'a'",
        (dbtable,),
    )
    return [(row[0], row[1]) for row in cur.fetchall()]


def _identity_sequences(cur, dbtable: str) -> list[tuple[str, str]]:
    """Return identity sequences owned internally by columns of *dbtable*."""
    cur.execute(
        "SELECT dep.objid::regclass::text, att.attname "
        "FROM pg_depend dep "
        "JOIN pg_attribute att ON att.attrelid = dep.refobjid AND att.attnum = dep.refobjsubid "
        "WHERE dep.refobjid = to_regclass(%s) AND dep.classid = 'pg_class'::regclass AND dep.deptype = 'i'",
        (dbtable,),
    )
    return [(row[0], row[1]) for row in cur.fetchall()]


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
    staging_table: str | None = None  # set only by PostgreSQL atomic overwrite


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
        case_sensitive: bool = False,
        isolation_level: str = "READ_UNCOMMITTED",
        query_timeout: int = 0,
    ) -> None:
        if overwrite_mode not in self._VALID_MODES:
            msg = f"Invalid overwrite_mode {overwrite_mode!r}. Valid values: {sorted(self._VALID_MODES)}"
            raise ValueError(msg)
        if batch_size <= 0:
            msg = f"batch_size must be a positive integer, got {batch_size}."
            raise ValueError(msg)

        self.dsn = dsn
        self.dbtable = dbtable
        self.overwrite_mode = overwrite_mode
        self.batch_size = batch_size
        self.case_sensitive = case_sensitive
        self.isolation_level = isolation_level
        self.query_timeout = query_timeout
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

    def _cleanup_atomic_staging(self, staging: str) -> None:
        import psycopg  # noqa: PLC0415

        with (
            contextlib.suppress(Exception),
            psycopg.connect(self.dsn, autocommit=True) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(f"DROP TABLE IF EXISTS {_quote_qualified(staging)}")

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
            # Single transaction with a txn-scoped advisory lock (auto-released on commit/
            # rollback): TRUNCATE and the sentinel insert must commit together, else a crash
            # between them leaves the target truncated but unmarked, and another partition
            # would TRUNCATE again after rows were ingested — wiping them.
            with psycopg.connect(self.dsn) as conn, conn.cursor() as cur:
                cur.execute(f"SELECT pg_advisory_xact_lock({lock_key})")
                cur.execute(f"CREATE TABLE IF NOT EXISTS {qsentinel} (done BOOLEAN)")
                cur.execute(f"SELECT COUNT(*) FROM {qsentinel}")  # noqa: S608
                row = cur.fetchone()
                if row[0] == 0:  # type: ignore[index]
                    cur.execute(f"TRUNCATE TABLE ONLY {qtarget}")
                    cur.execute(f"INSERT INTO {qsentinel} VALUES (TRUE)")  # noqa: S608
                conn.commit()
        except Exception as e:
            safe_msg = _safe_error(e, self.dsn)
            msg = f"Truncate-mode advisory-lock failed for {self.dbtable!r}: {safe_msg}"
            raise RuntimeError(msg) from e

    # ------------------------------------------------------------------
    # Executor side
    # ------------------------------------------------------------------

    def write_partition(self, partition_id: int, batches: Iterable[pa.RecordBatch]) -> PartitionResult:
        """Write one partition. Returns a :class:`PartitionResult` for the driver."""
        import itertools  # noqa: PLC0415

        import adbc_driver_postgresql.dbapi as pg_dbapi  # noqa: PLC0415

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
        nonempty = (batch for batch in batches if batch.num_rows > 0)
        first = next(nonempty, None)
        if first is not None:
            try:
                resolved = _resolve_column_names(
                    first.schema.names,
                    _pg_target_columns(self.dsn, self.dbtable),
                    case_sensitive=self.case_sensitive,
                )
            except Exception:
                if staging is not None:
                    self._cleanup_atomic_staging(staging)
                raise
            ingest_schema, ingest_table = _split_schema(target)
            try:
                with pg_dbapi.connect(self.dsn) as conn:
                    if self.isolation_level == "NONE":
                        conn.autocommit = True
                    with conn.cursor() as cur:
                        if self.isolation_level != "NONE":
                            cur.execute(f"SET TRANSACTION ISOLATION LEVEL {self.isolation_level.replace('_', ' ')}")
                        if self.query_timeout:
                            cur.execute(f"SET statement_timeout = {self.query_timeout * 1000}")
                        for batch in itertools.chain((first,), nonempty):
                            table_obj = pa.Table.from_batches([batch]).rename_columns(resolved)
                            for chunk in _iter_arrow_chunks(table_obj, self.batch_size):
                                cur.adbc_ingest(
                                    ingest_table,
                                    chunk,
                                    mode="append",
                                    db_schema_name=ingest_schema,
                                )
                            rows += batch.num_rows
                    if self.isolation_level != "NONE":
                        conn.commit()
            except Exception as e:
                if staging is not None:
                    self._cleanup_atomic_staging(staging)
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
            qrename = _quote_identifier(_split_schema(self.dbtable)[1])
            try:
                with psycopg.connect(self.dsn) as conn:  # NOT autocommit — single txn
                    with conn.cursor() as cur:
                        # Detach sequences OWNED BY the target (serial columns) so DROP is not
                        # refused, then re-own and re-sync them onto the swapped-in table.
                        owned = _owned_sequences(cur, self.dbtable)
                        identities = _identity_sequences(cur, staging_name)
                        for seq, _ in owned:
                            cur.execute(f"ALTER SEQUENCE {seq} OWNED BY NONE")
                        cur.execute(f"DROP TABLE IF EXISTS {qtarget}")
                        cur.execute(f"ALTER TABLE {qstaging} RENAME TO {qrename}")
                        for seq, col in owned:
                            qcol = _quote_identifier(col)
                            cur.execute(f"ALTER SEQUENCE {seq} OWNED BY {qtarget}.{qcol}")
                            cur.execute(
                                f"SELECT setval(%s, COALESCE(MAX({qcol}), 1), MAX({qcol}) IS NOT NULL) "  # noqa: S608
                                f"FROM {qtarget}",
                                (seq,),
                            )
                        for seq, col in identities:
                            qcol = _quote_identifier(col)
                            cur.execute(
                                f"SELECT setval(%s, COALESCE(MAX({qcol}), 1), MAX({qcol}) IS NOT NULL) "  # noqa: S608
                                f"FROM {qtarget}",
                                (seq,),
                            )
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
    ``databaseName`` -> URL db segment; ``user``/``username`` and ``password`` -> URL
    userinfo (the common MS JDBC form, e.g. ``...;user=alice;password=s3cret``) when the
    authority has none; ``encrypt=true|false`` -> ``encryption`` (require/off);
    ``applicationIntent=ReadOnly`` -> ``read_only=True``. Microsoft-JDBC-specific
    certificate properties and unsupported encryption modes are rejected because
    pymssql/FreeTDS cannot preserve their security semantics. Any other unknown
    param is rejected as well rather than silently dropped.
    """
    from urllib.parse import quote  # noqa: PLC0415

    authority, _, param_str = rest.partition(";")
    userinfo, host, instance, port = _split_sqlserver_authority(authority)

    params: dict[str, str] = {}
    param_names: dict[str, str] = {}
    if param_str:
        for raw in param_str.split(";"):
            if not raw.strip():
                continue
            key, _, value = raw.partition("=")
            normalized = key.strip().lower()
            params[normalized] = value.strip()
            param_names[normalized] = key.strip()

    unsupported_tls = {
        "trustservercertificate": "trustServerCertificate",
        "hostnameincertificate": "hostNameInCertificate",
        "truststore": "trustStore",
        "truststorepassword": "trustStorePassword",
    }
    for key, display_name in unsupported_tls.items():
        if key in params:
            msg = f"SQL Server JDBC property {display_name} is not supported by pymssql/FreeTDS."
            raise ValueError(msg)
    supported = {"databasename", "user", "username", "password", "encrypt", "applicationintent"}
    if unknown := next((key for key in params if key not in supported), None):
        msg = f"SQL Server JDBC property {param_names[unknown]} is not supported by pymssql/FreeTDS."
        raise ValueError(msg)

    database = params.get("databasename", "")

    # Credentials commonly arrive as ;user=;password= params rather than in the authority.
    # Only synthesise userinfo from params when the authority carried none (authority wins).
    if not userinfo:
        user = params.get("user") or params.get("username")
        if user:
            password = params.get("password")
            creds = quote(user, safe="") + (f":{quote(password, safe='')}" if password else "")
            userinfo = f"{creds}@"

    # pymssql addresses a named instance via ``server\instance`` (TDS resolves the port).
    host_segment = f"{host}\\{instance}" if instance else host
    netloc = f"{userinfo}{host_segment}"
    if port is not None:
        netloc += f":{port}"

    url = f"mssql+pymssql://{netloc}/{database}" if database else f"mssql+pymssql://{netloc}"

    connect_args: dict[str, object] = {}
    encrypt = params.get("encrypt")
    if encrypt is not None:
        normalized = encrypt.lower()
        if normalized not in {"true", "false"}:
            msg = "SQL Server JDBC property encrypt must be true or false with the pymssql backend."
            raise ValueError(msg)
        connect_args["encryption"] = "require" if normalized == "true" else "off"
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
        msg = f"Cannot build a SQLAlchemy URL from {_redact_credentials(dsn)!r}"
        raise ValueError(msg)
    if scheme == "mysql":
        return f"mysql+pymysql://{rest}", {}
    if scheme == "sqlserver":
        return _parse_sqlserver_url(rest)
    msg = f"Unsupported JDBC subprotocol for writes: {scheme!r}"
    raise ValueError(msg)


def _timeout_connect_args(url: str, connect_args: dict, query_timeout: int) -> dict:
    result = dict(connect_args)
    if query_timeout:
        if url.startswith("mysql+"):
            result.update(read_timeout=query_timeout, write_timeout=query_timeout)
        elif url.startswith("mssql+"):
            result["timeout"] = query_timeout
    return result


def _arrow_to_sqlalchemy_type(arrow_type: pa.DataType, dialect: str | None = None):
    """Map Arrow types to Spark-compatible SQL types for table creation."""
    import sqlalchemy as sa  # noqa: PLC0415

    if dialect == "postgresql":
        from sqlalchemy.dialects import postgresql  # noqa: PLC0415

        if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
            return postgresql.ARRAY(_arrow_to_sqlalchemy_type(arrow_type.value_type, dialect))
        mapping = (
            (pa.types.is_boolean, postgresql.BOOLEAN),
            (lambda t: pa.types.is_int8(t) or pa.types.is_int16(t), postgresql.SMALLINT),
            (pa.types.is_int32, postgresql.INTEGER),
            (pa.types.is_int64, postgresql.BIGINT),
            (pa.types.is_float32, postgresql.REAL),
            (pa.types.is_float64, postgresql.DOUBLE_PRECISION),
            (pa.types.is_date, postgresql.DATE),
            (lambda t: pa.types.is_string(t) or pa.types.is_large_string(t), postgresql.TEXT),
            (lambda t: pa.types.is_binary(t) or pa.types.is_large_binary(t), postgresql.BYTEA),
        )
        for predicate, sql_type in mapping:
            if predicate(arrow_type):
                return sql_type()
        if pa.types.is_decimal(arrow_type):
            return postgresql.NUMERIC(precision=arrow_type.precision, scale=arrow_type.scale)
        if pa.types.is_timestamp(arrow_type):
            return postgresql.TIMESTAMP(timezone=arrow_type.tz is not None)
        msg = f"Spark JDBC does not support automatic PostgreSQL table creation for Arrow type {arrow_type}."
        raise TypeError(msg)

    if dialect == "mysql":
        from sqlalchemy.dialects import mysql  # noqa: PLC0415

        if pa.types.is_boolean(arrow_type):
            return mysql.BIT(1)
        if pa.types.is_int8(arrow_type):
            return mysql.TINYINT()
        if pa.types.is_int16(arrow_type):
            return mysql.SMALLINT()
        if pa.types.is_int32(arrow_type):
            return mysql.INTEGER()
        if pa.types.is_int64(arrow_type):
            return mysql.BIGINT()
        if pa.types.is_float32(arrow_type):
            return mysql.FLOAT()
        if pa.types.is_float64(arrow_type):
            return mysql.DOUBLE()
        if pa.types.is_decimal(arrow_type):
            return mysql.DECIMAL(precision=arrow_type.precision, scale=arrow_type.scale)
        if pa.types.is_date(arrow_type):
            return mysql.DATE()
        if pa.types.is_timestamp(arrow_type):
            return mysql.TIMESTAMP() if arrow_type.tz is not None else mysql.DATETIME()
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return mysql.LONGTEXT()
        if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return mysql.BLOB()
        msg = f"Spark JDBC does not support automatic MySQL table creation for Arrow type {arrow_type}."
        raise TypeError(msg)
    if dialect == "mssql":
        from sqlalchemy.dialects import mssql  # noqa: PLC0415

        if pa.types.is_boolean(arrow_type):
            return mssql.BIT()
        if pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type):
            return mssql.SMALLINT()
        if pa.types.is_int32(arrow_type):
            return mssql.INTEGER()
        if pa.types.is_int64(arrow_type):
            return mssql.BIGINT()
        if pa.types.is_float32(arrow_type):
            return mssql.REAL()
        if pa.types.is_float64(arrow_type):
            return mssql.FLOAT(precision=53)
        if pa.types.is_decimal(arrow_type):
            return mssql.DECIMAL(precision=arrow_type.precision, scale=arrow_type.scale)
        if pa.types.is_date(arrow_type):
            return mssql.DATE()
        if pa.types.is_timestamp(arrow_type):
            return mssql.DATETIME()
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return mssql.NVARCHAR(None)
        if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return mssql.VARBINARY(None)
        msg = f"Spark JDBC does not support automatic SQL Server table creation for Arrow type {arrow_type}."
        raise TypeError(msg)

    if pa.types.is_boolean(arrow_type):
        return sa.Boolean()
    if pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type):
        return sa.SmallInteger()
    if pa.types.is_int32(arrow_type):
        return sa.Integer()
    if pa.types.is_int64(arrow_type):
        return sa.BigInteger()
    if pa.types.is_uint8(arrow_type):
        return sa.SmallInteger()
    if pa.types.is_uint16(arrow_type):
        return sa.Integer()
    if pa.types.is_uint32(arrow_type):
        return sa.BigInteger()
    if pa.types.is_uint64(arrow_type):
        return sa.Numeric(precision=20, scale=0)
    if pa.types.is_float32(arrow_type):
        return sa.Float(precision=24)
    if pa.types.is_float64(arrow_type):
        return sa.Float(precision=53)
    if pa.types.is_decimal(arrow_type):
        return sa.Numeric(precision=arrow_type.precision, scale=arrow_type.scale)
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return sa.Text()
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return sa.LargeBinary()
    if pa.types.is_date(arrow_type):
        return sa.Date()
    if pa.types.is_time(arrow_type):
        return sa.Time()
    if pa.types.is_timestamp(arrow_type):
        return sa.DateTime(timezone=arrow_type.tz is not None)
    msg = f"No automatic JDBC table-creation mapping for Arrow type {arrow_type}."
    raise TypeError(msg)


def _column_type_overrides(value: str, case_sensitive: bool = False) -> dict[str, str]:
    """Split Spark's ``createTableColumnTypes`` DDL at top-level commas."""
    parts: list[str] = []
    start = depth = 0
    for index, char in enumerate(value):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth < 0:
                raise ValueError("Invalid createTableColumnTypes: unmatched ')'.")
        elif char == "," and depth == 0:
            parts.append(value[start:index])
            start = index + 1
    if depth:
        raise ValueError("Invalid createTableColumnTypes: unmatched '('.")
    parts.append(value[start:])
    overrides: dict[str, str] = {}
    for part in parts:
        part = part.strip()
        if part.startswith("`"):
            end = part.find("`", 1)
            if end < 0:
                raise ValueError("Invalid createTableColumnTypes: unmatched '`'.")
            name, sql_type = part[1:end], part[end + 1 :].strip()
            separator = bool(sql_type)
        else:
            fields = part.split(maxsplit=1)
            name, sql_type = fields if len(fields) == 2 else (part, "")
            separator = bool(sql_type)
        if not separator or not name or not sql_type.strip():
            raise ValueError(f"Invalid createTableColumnTypes field: {part!r}.")
        duplicate = name if case_sensitive else name.casefold()
        existing = {key if case_sensitive else key.casefold() for key in overrides}
        if duplicate in existing:
            raise ValueError(f"createTableColumnTypes contains duplicate column {name!r}.")
        overrides[name] = sql_type.strip()
    return overrides


def _spark_ddl_type(value: str, dialect: str):
    """Map the Spark SQL types accepted by createTableColumnTypes."""
    import re  # noqa: PLC0415

    import sqlalchemy as sa  # noqa: PLC0415

    normalized = " ".join(value.upper().split())
    array = re.fullmatch(r"ARRAY\s*<\s*(.+)\s*>", normalized)
    if array:
        if dialect != "postgresql":
            raise ValueError(f"Spark JDBC does not support ARRAY create types for {dialect}.")
        from sqlalchemy.dialects import postgresql  # noqa: PLC0415

        return postgresql.ARRAY(_spark_ddl_type(array[1], dialect))
    sized = re.fullmatch(r"(CHAR|VARCHAR)\s*\(\s*(\d+)\s*\)", normalized)
    if sized:
        return sa.CHAR(int(sized[2])) if sized[1] == "CHAR" else sa.VARCHAR(int(sized[2]))
    decimal = re.fullmatch(r"(?:DECIMAL|NUMERIC)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", normalized)
    if decimal:
        return _arrow_to_sqlalchemy_type(pa.decimal128(int(decimal[1]), int(decimal[2])), dialect)
    if normalized in {"DECIMAL", "NUMERIC"}:
        return _arrow_to_sqlalchemy_type(pa.decimal128(10, 0), dialect)
    arrow_types = {
        "BOOLEAN": pa.bool_(),
        "BYTE": pa.int8(),
        "TINYINT": pa.int8(),
        "SHORT": pa.int16(),
        "SMALLINT": pa.int16(),
        "INT": pa.int32(),
        "INTEGER": pa.int32(),
        "LONG": pa.int64(),
        "BIGINT": pa.int64(),
        "FLOAT": pa.float32(),
        "REAL": pa.float32(),
        "DOUBLE": pa.float64(),
        "DOUBLE PRECISION": pa.float64(),
        "STRING": pa.string(),
        "BINARY": pa.binary(),
        "DATE": pa.date32(),
        "TIMESTAMP": pa.timestamp("us", tz="UTC"),
        "TIMESTAMP_NTZ": pa.timestamp("us"),
    }
    if normalized not in arrow_types:
        raise ValueError(f"Unsupported Spark type in createTableColumnTypes: {value!r}.")
    return _arrow_to_sqlalchemy_type(arrow_types[normalized], dialect)


def _create_table_sql(
    dbtable: str,
    schema: pa.Schema,
    dialect: str,
    options: str = "",
    column_types: str = "",
    case_sensitive: bool = False,
) -> str:
    """Compile Spark-compatible CREATE TABLE DDL and append its raw table options."""
    import sqlalchemy as sa  # noqa: PLC0415
    from sqlalchemy.dialects import mssql, mysql, postgresql  # noqa: PLC0415
    from sqlalchemy.schema import CreateTable  # noqa: PLC0415

    dialect_impl = {
        "postgresql": postgresql.dialect(),
        "mysql": mysql.dialect(),
        "mssql": mssql.dialect(),
    }[dialect]
    db_schema, table = _split_schema(dbtable)
    overrides = _column_type_overrides(column_types, case_sensitive) if column_types else {}
    if not case_sensitive:
        names = {name.casefold(): name for name in schema.names}
        overrides = {names.get(name.casefold(), name): value for name, value in overrides.items()}
    unknown = set(overrides).difference(schema.names)
    if unknown:
        raise ValueError(f"createTableColumnTypes contains unknown columns: {sorted(unknown)!r}.")
    columns = [
        sa.Column(
            field.name,
            _spark_ddl_type(overrides[field.name], dialect)
            if field.name in overrides
            else _arrow_to_sqlalchemy_type(field.type, dialect),
            nullable=field.nullable,
        )
        for field in schema
    ]
    sql = str(CreateTable(sa.Table(table, sa.MetaData(), *columns, schema=db_schema)).compile(dialect=dialect_impl))
    return f"{sql.strip()} {options.strip()}".rstrip()


def _validate_pg_create_schema(schema: pa.Schema) -> None:
    """Reject Arrow types outside Spark 4.1's PostgreSQL creation matrix."""

    def supported(data_type: pa.DataType) -> bool:
        if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
            return supported(data_type.value_type)
        return any(
            predicate(data_type)
            for predicate in (
                pa.types.is_boolean,
                pa.types.is_int8,
                pa.types.is_int16,
                pa.types.is_int32,
                pa.types.is_int64,
                pa.types.is_float32,
                pa.types.is_float64,
                pa.types.is_decimal,
                pa.types.is_date,
                pa.types.is_timestamp,
                pa.types.is_string,
                pa.types.is_large_string,
                pa.types.is_binary,
                pa.types.is_large_binary,
            )
        )

    for field in schema:
        if not supported(field.type):
            msg = (
                "Spark JDBC does not support automatic PostgreSQL table creation "
                f"for column {field.name!r} with Arrow type {field.type}."
            )
            raise TypeError(msg)


def _pg_table_exists(dsn: str, dbtable: str, query_timeout: int = 0) -> bool:
    import psycopg  # noqa: PLC0415

    with psycopg.connect(_postgresql_dsn_with_timeout(dsn, query_timeout)) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (dbtable,))
        return cur.fetchone()[0] is not None  # type: ignore[index]


def _resolve_column_names(source: list[str], target: list[str], *, case_sensitive: bool = False) -> list[str]:
    """Resolve source columns to target spelling using Spark's name-based rules."""
    resolved: list[str] = []
    for name in source:
        matches = [candidate for candidate in target if candidate == name]
        if not case_sensitive and not matches:
            matches = [candidate for candidate in target if candidate.casefold() == name.casefold()]
        if len(matches) != 1:
            msg = f"Column {name!r} cannot be resolved uniquely in target schema {target!r}."
            raise ValueError(msg)
        resolved.append(matches[0])
    return resolved


def _pg_target_columns(dsn: str, dbtable: str, query_timeout: int = 0) -> list[str]:
    import psycopg  # noqa: PLC0415

    with psycopg.connect(_postgresql_dsn_with_timeout(dsn, query_timeout)) as conn, conn.cursor() as cur:
        # Identifiers are parsed and quoted by _quote_qualified; query values are not interpolated.
        cur.execute(f"SELECT * FROM {_quote_qualified(dbtable)} LIMIT 0")  # noqa: S608
        return [column.name for column in cur.description or ()]


def _sqlalchemy_table_exists(url: str, connect_args: dict, dbtable: str, query_timeout: int = 0) -> bool:
    import sqlalchemy as sa  # noqa: PLC0415
    from sqlalchemy import NullPool  # noqa: PLC0415

    schema, table = _split_schema(dbtable)
    engine = sa.create_engine(
        url, connect_args=_timeout_connect_args(url, connect_args, query_timeout), poolclass=NullPool
    )
    try:
        return sa.inspect(engine).has_table(table, schema=schema)
    finally:
        engine.dispose()


def _sqlalchemy_target_columns(url: str, connect_args: dict, dbtable: str, query_timeout: int = 0) -> list[str]:
    import sqlalchemy as sa  # noqa: PLC0415
    from sqlalchemy import NullPool  # noqa: PLC0415

    schema, table = _split_schema(dbtable)
    engine = sa.create_engine(
        url, connect_args=_timeout_connect_args(url, connect_args, query_timeout), poolclass=NullPool
    )
    try:
        return [column["name"] for column in sa.inspect(engine).get_columns(table, schema=schema)]
    finally:
        engine.dispose()


def _create_pg_table(
    dsn: str,
    dbtable: str,
    schema: pa.Schema,
    options: str = "",
    column_types: str = "",
    case_sensitive: bool = False,
    table_comment: str = "",
    query_timeout: int = 0,
) -> None:
    """Create a missing PostgreSQL target once on the driver.

    Always compiled DDL, never ADBC's ``mode="create"`` ingest: ADBC infers
    types from the Arrow schema and loses Spark's exact DDL — e.g. it creates
    a bare ``numeric`` for ``DECIMAL(p,s)`` where Spark emits ``NUMERIC(p,s)``.
    """
    import psycopg  # noqa: PLC0415

    with (
        psycopg.connect(_postgresql_dsn_with_timeout(dsn, query_timeout), autocommit=True) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(_create_table_sql(dbtable, schema, "postgresql", options, column_types, case_sensitive))
    if table_comment:
        import psycopg  # noqa: PLC0415
        from psycopg import sql  # noqa: PLC0415

        try:
            with (
                psycopg.connect(_postgresql_dsn_with_timeout(dsn, query_timeout), autocommit=True) as conn,
                conn.cursor() as cur,
            ):
                cur.execute(
                    sql.SQL(f"COMMENT ON TABLE {_quote_qualified(dbtable)} IS {{}}").format(sql.Literal(table_comment))
                )
        except Exception:  # noqa: BLE001
            import warnings  # noqa: PLC0415

            warnings.warn("Cannot create JDBC table comment; comment ignored.", RuntimeWarning, stacklevel=2)


def _drop_pg_table(dsn: str, dbtable: str, query_timeout: int = 0) -> None:
    """Drop an existing PostgreSQL target before Spark-style overwrite."""
    import psycopg  # noqa: PLC0415

    with (
        psycopg.connect(_postgresql_dsn_with_timeout(dsn, query_timeout), autocommit=True) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(f"DROP TABLE {_quote_qualified(dbtable)}")


def _truncate_pg_table(dsn: str, dbtable: str, *, cascade: bool = False, query_timeout: int = 0) -> None:
    import psycopg  # noqa: PLC0415

    with (
        psycopg.connect(_postgresql_dsn_with_timeout(dsn, query_timeout), autocommit=True) as conn,
        conn.cursor() as cur,
    ):
        suffix = " CASCADE" if cascade else ""
        cur.execute(f"TRUNCATE TABLE ONLY {_quote_qualified(dbtable)}{suffix}")


def _create_sqlalchemy_table(
    url: str,
    connect_args: dict,
    dbtable: str,
    schema: pa.Schema,
    options: str = "",
    column_types: str = "",
    case_sensitive: bool = False,
    table_comment: str = "",
    query_timeout: int = 0,
) -> None:
    """Create a missing MySQL or SQL Server target once on the driver."""
    import sqlalchemy as sa  # noqa: PLC0415
    from sqlalchemy import NullPool  # noqa: PLC0415

    db_schema, table = _split_schema(dbtable)
    engine = sa.create_engine(
        url, connect_args=_timeout_connect_args(url, connect_args, query_timeout), poolclass=NullPool
    )
    try:
        columns = [
            sa.Column(f.name, _arrow_to_sqlalchemy_type(f.type, engine.dialect.name), nullable=f.nullable)
            for f in schema
        ]
        if options or column_types:
            with engine.begin() as conn:
                conn.exec_driver_sql(
                    _create_table_sql(dbtable, schema, engine.dialect.name, options, column_types, case_sensitive)
                )
        else:
            sa.Table(table, sa.MetaData(), *columns, schema=db_schema).create(engine, checkfirst=True)
        if table_comment:
            try:
                if engine.dialect.name != "mysql":
                    # Spark's MsSqlServerDialect rejects table comments; the shared
                    # createTable path swallows that and ignores the comment.
                    msg = "table comments are not supported for this dialect"
                    raise NotImplementedError(msg)
                prep = engine.dialect.identifier_preparer
                qualified = f"{prep.quote(db_schema)}.{prep.quote(table)}" if db_schema else prep.quote(table)
                with engine.begin() as conn:
                    conn.execute(sa.text(f"ALTER TABLE {qualified} COMMENT = :comment"), {"comment": table_comment})
            except Exception:  # noqa: BLE001
                import warnings  # noqa: PLC0415

                warnings.warn("Cannot create JDBC table comment; comment ignored.", RuntimeWarning, stacklevel=2)
    finally:
        engine.dispose()


def _reset_sqlalchemy_table(
    url: str, connect_args: dict, dbtable: str, *, truncate: bool, query_timeout: int = 0
) -> None:
    """Clear an existing target using Spark's truncate or drop/recreate decision."""
    import sqlalchemy as sa  # noqa: PLC0415
    from sqlalchemy import NullPool  # noqa: PLC0415

    db_schema, table = _split_schema(dbtable)
    engine = sa.create_engine(
        url, connect_args=_timeout_connect_args(url, connect_args, query_timeout), poolclass=NullPool
    )
    prep = engine.dialect.identifier_preparer
    qualified = f"{prep.quote(db_schema)}.{prep.quote(table)}" if db_schema else prep.quote(table)
    try:
        with engine.begin() as conn:
            statement = f"TRUNCATE TABLE {qualified}" if truncate else f"DROP TABLE {qualified}"
            conn.execute(sa.text(statement))
    finally:
        engine.dispose()


class SqlAlchemyWriteEngine:
    """Fallback write engine for non-PostgreSQL dialects (MySQL, SQL Server).

    Rows go through a parameterised SQLAlchemy-core ``INSERT`` built from the Arrow
    table's Python values (``to_pylist``), which preserves exact ints (bigints > 2**53)
    and keeps NULL distinct from 0 — unlike a pandas ``to_sql`` float64 round-trip.

    Partitions always append into the target; save-mode DDL (create, drop/recreate,
    truncate) happens once on the driver in ``_sail_prepare``, matching Spark's JDBC
    writer. At-least-once: a retried task re-inserts, so duplicates are possible
    (as with Spark's JDBC writer); use a unique constraint.
    """

    def __init__(
        self,
        *,
        url: str,
        dbtable: str,
        batch_size: int,
        connect_args: dict | None = None,
        case_sensitive: bool = False,
        isolation_level: str = "READ_UNCOMMITTED",
        query_timeout: int = 0,
    ) -> None:
        if batch_size <= 0:
            msg = f"batch_size must be a positive integer, got {batch_size}."
            raise ValueError(msg)
        self.url = url
        self.dbtable = dbtable
        self.batch_size = batch_size
        self.connect_args = connect_args or {}
        self.case_sensitive = case_sensitive
        self.isolation_level = isolation_level
        self.query_timeout = query_timeout
        self.schema, self.table = _split_schema(dbtable)

    def _create_engine(self):
        import sqlalchemy as sa  # noqa: PLC0415
        from sqlalchemy import NullPool  # noqa: PLC0415

        # Short-lived, single-connection engine — skip the connection pool.
        isolation = "AUTOCOMMIT" if self.isolation_level == "NONE" else self.isolation_level.replace("_", " ")
        connect_args = _timeout_connect_args(self.url, self.connect_args, self.query_timeout)
        engine = sa.create_engine(
            self.url,
            poolclass=NullPool,
            connect_args=connect_args,
            isolation_level=isolation,
        )
        try:
            with engine.connect():
                pass
        except (sa.exc.ArgumentError, NotImplementedError):
            import warnings  # noqa: PLC0415

            engine.dispose()
            warnings.warn(
                f"Requested isolation level {self.isolation_level} is unsupported; using driver default.",
                RuntimeWarning,
                stacklevel=2,
            )
            engine = sa.create_engine(
                self.url,
                poolclass=NullPool,
                connect_args=connect_args,
            )
        return engine

    def _reflect_table(self, engine, table_name: str):
        """Reflect *table_name* from the live database into an ``sa.Table``."""
        import sqlalchemy as sa  # noqa: PLC0415

        return sa.Table(table_name, sa.MetaData(), schema=self.schema, autoload_with=engine)

    def _insert_arrow(self, engine, sa_table, table_obj: pa.Table, *, conn=None) -> None:
        """Insert an Arrow table via a parameterised INSERT, in ``batch_size`` chunks.

        *sa_table*'s column SQL types drive the bind (ints stay ints); ``to_pylist`` per
        chunk lets int/None flow through without float coercion or materialising all rows.
        """
        import sqlalchemy as sa  # noqa: PLC0415

        if table_obj.num_rows == 0:
            return
        resolved = _resolve_column_names(
            table_obj.column_names,
            [column.name for column in sa_table.columns],
            case_sensitive=self.case_sensitive,
        )
        table_obj = table_obj.rename_columns(resolved)

        def insert_batches(connection) -> None:
            for chunk in _iter_arrow_chunks(table_obj, self.batch_size):
                connection.execute(sa.insert(sa_table), chunk.to_pylist())

        if conn is not None:
            insert_batches(conn)
        else:
            with engine.begin() as transaction:
                insert_batches(transaction)

    def write_partition(self, partition_id: int, batches: Iterable[pa.RecordBatch]) -> PartitionResult:
        import itertools  # noqa: PLC0415

        rows = 0
        engine = self._create_engine()
        try:
            nonempty = (batch for batch in batches if batch.num_rows > 0)
            first = next(nonempty, None)
            if first is not None:
                target_table = self._reflect_table(engine, self.table)
                with engine.begin() as conn:
                    for batch in itertools.chain((first,), nonempty):
                        self._insert_arrow(
                            engine,
                            target_table,
                            pa.Table.from_batches([batch]),
                            conn=conn,
                        )
                        rows += batch.num_rows
        except Exception as e:
            safe_msg = _safe_error(e, self.url)
            msg = f"SQLAlchemy write failed for partition {partition_id} into {self.dbtable!r}: {safe_msg}"
            raise RuntimeError(msg) from e
        finally:
            engine.dispose()

        return PartitionResult(partition_id=partition_id, rows_written=rows)

    def commit(self, results: list[PartitionResult]) -> int:
        return sum(r.rows_written for r in results)

    def abort(self, results: list[PartitionResult]) -> None:
        """Partitions append directly and roll back their own transaction on failure."""


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

        # Sail does not populate Spark's TaskContext on the write path, so pid is 0 for
        # every partition. It is used only for logging/PartitionResult, never for correctness.
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
        self,
        *,
        conn_str: str,
        dbtable: str,
        schema: pa.Schema,
        save_mode: str,
        overwrite_mode: str,
        batch_size: int,
        case_sensitive: bool,
        cascade_truncate: bool = False,
        create_table_options: str = "",
        create_table_column_types: str = "",
        isolation_level: str = "READ_UNCOMMITTED",
        query_timeout: int = 0,
        table_comment: str = "",
        run_id: str | None = None,
    ) -> None:
        self._conn_str = conn_str
        self._dbtable = dbtable
        self._schema = schema
        self._save_mode = save_mode
        self._overwrite_mode = overwrite_mode
        self._case_sensitive = case_sensitive
        self._cascade_truncate = cascade_truncate
        self._create_table_options = create_table_options
        self._create_table_column_types = create_table_column_types
        self._table_comment = table_comment
        self._query_timeout = query_timeout
        super().__init__(
            PgWriteEngine(
                dsn=conn_str,
                dbtable=dbtable,
                overwrite_mode="atomic" if overwrite_mode == "atomic" else "append",
                batch_size=batch_size,
                run_id=run_id,
                case_sensitive=case_sensitive,
                isolation_level=isolation_level,
                query_timeout=query_timeout,
            )
        )

    def _sail_prepare(self) -> str:
        try:
            exists = _pg_table_exists(self._conn_str, self._dbtable, self._query_timeout)
            if self._save_mode == "errorifexists" and exists:
                msg = f"Target table {self._dbtable!r} already exists."
                raise ValueError(msg)  # noqa: TRY301
            if self._save_mode == "ignore" and exists:
                return "skip"

            if not exists:
                _validate_pg_create_schema(self._schema)
                _create_pg_table(
                    self._conn_str,
                    self._dbtable,
                    self._schema,
                    self._create_table_options,
                    self._create_table_column_types,
                    self._case_sensitive,
                    self._table_comment,
                    self._query_timeout,
                )
            elif self._save_mode == "overwrite" and self._overwrite_mode == "spark":
                _validate_pg_create_schema(self._schema)
                _drop_pg_table(self._conn_str, self._dbtable, self._query_timeout)
                _create_pg_table(
                    self._conn_str,
                    self._dbtable,
                    self._schema,
                    self._create_table_options,
                    self._create_table_column_types,
                    self._case_sensitive,
                    self._table_comment,
                    self._query_timeout,
                )
            else:
                if self._save_mode == "overwrite" and self._overwrite_mode == "truncate":
                    _truncate_pg_table(
                        self._conn_str,
                        self._dbtable,
                        cascade=self._cascade_truncate,
                        query_timeout=self._query_timeout,
                    )
                _resolve_column_names(
                    list(self._schema.names),
                    _pg_target_columns(self._conn_str, self._dbtable, self._query_timeout),
                    case_sensitive=self._case_sensitive,
                )
            return "write"  # noqa: TRY300
        except Exception as e:
            if isinstance(e, (TypeError, ValueError)):
                raise
            msg = f"JDBC write preparation failed for {self._dbtable!r}: {_safe_error(e, self._conn_str)}"
            raise RuntimeError(msg) from e


class SqlAlchemyDataSourceWriter(_ArrowWriter):
    """Fallback writer for non-PostgreSQL dialects."""

    def __init__(
        self,
        *,
        url: str,
        dbtable: str,
        schema: pa.Schema,
        dialect: str,
        save_mode: str,
        truncate: bool,
        case_sensitive: bool,
        batch_size: int,
        create_table_options: str = "",
        create_table_column_types: str = "",
        isolation_level: str = "READ_UNCOMMITTED",
        query_timeout: int = 0,
        table_comment: str = "",
        connect_args: dict | None = None,
    ) -> None:
        self._url = url
        self._dbtable = dbtable
        self._schema = schema
        self._dialect = dialect
        self._save_mode = save_mode
        self._truncate = truncate
        self._case_sensitive = case_sensitive
        self._connect_args = connect_args or {}
        self._create_table_options = create_table_options
        self._create_table_column_types = create_table_column_types
        self._table_comment = table_comment
        self._query_timeout = query_timeout
        super().__init__(
            SqlAlchemyWriteEngine(
                url=url,
                dbtable=dbtable,
                batch_size=batch_size,
                connect_args=connect_args,
                case_sensitive=case_sensitive,
                isolation_level=isolation_level,
                query_timeout=query_timeout,
            )
        )

    def _sail_prepare(self) -> str:
        try:
            exists = _sqlalchemy_table_exists(self._url, self._connect_args, self._dbtable, self._query_timeout)
            if self._save_mode == "errorifexists" and exists:
                msg = f"Target table {self._dbtable!r} already exists."
                raise ValueError(msg)  # noqa: TRY301
            if self._save_mode == "ignore" and exists:
                return "skip"

            if not exists:
                for field in self._schema:
                    _arrow_to_sqlalchemy_type(field.type, self._dialect)
                _create_sqlalchemy_table(
                    self._url,
                    self._connect_args,
                    self._dbtable,
                    self._schema,
                    self._create_table_options,
                    self._create_table_column_types,
                    self._case_sensitive,
                    self._table_comment,
                    self._query_timeout,
                )
            elif self._save_mode == "overwrite":
                if self._truncate:
                    _reset_sqlalchemy_table(
                        self._url,
                        self._connect_args,
                        self._dbtable,
                        truncate=True,
                        query_timeout=self._query_timeout,
                    )
                    _resolve_column_names(
                        list(self._schema.names),
                        _sqlalchemy_target_columns(self._url, self._connect_args, self._dbtable, self._query_timeout),
                        case_sensitive=self._case_sensitive,
                    )
                if not self._truncate:
                    _reset_sqlalchemy_table(
                        self._url,
                        self._connect_args,
                        self._dbtable,
                        truncate=False,
                        query_timeout=self._query_timeout,
                    )
                    for field in self._schema:
                        _arrow_to_sqlalchemy_type(field.type, self._dialect)
                    _create_sqlalchemy_table(
                        self._url,
                        self._connect_args,
                        self._dbtable,
                        self._schema,
                        self._create_table_options,
                        self._create_table_column_types,
                        self._case_sensitive,
                        self._table_comment,
                        self._query_timeout,
                    )
            else:
                _resolve_column_names(
                    list(self._schema.names),
                    _sqlalchemy_target_columns(self._url, self._connect_args, self._dbtable, self._query_timeout),
                    case_sensitive=self._case_sensitive,
                )
            return "write"  # noqa: TRY300
        except Exception as e:
            if isinstance(e, (TypeError, ValueError)):
                raise
            msg = f"JDBC write preparation failed for {self._dbtable!r}: {_safe_error(e, self._url)}"
            raise RuntimeError(msg) from e


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

    The read path does not support ``driver``, ``predicates`` list,
    ``queryTimeout``, ``isolationLevel``, ``sessionInitStatement``, or
    Kerberos. The write path supports the native driver declarations,
    ``queryTimeout``, and ``isolationLevel`` documented by its writer.
    """

    @classmethod
    def name(cls) -> str:
        return "jdbc"

    # ------------------------------------------------------------------
    # Options resolution + validation
    # ------------------------------------------------------------------

    def _resolve_options(self) -> dict:
        opts = {key.lower(): value for key, value in self.options.items()}

        # --- url ---
        url = opts.get("url")
        if not url:
            msg = "Option 'url' is required for the jdbc data source"
            raise ValueError(msg)
        if not url.startswith("jdbc:"):
            msg = f"Invalid JDBC URL: {_redact_credentials(url)!r}. Expected format: jdbc:<subprotocol>://<host>:<port>/<database>"
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
        if conn_str.startswith("postgresql://"):
            conn_str = _postgresql_dsn_with_properties(conn_str, opts)

        # --- partitioning ---
        num_partitions = int(opts.get("numpartitions", "1"))
        partition_column = opts.get("partitioncolumn") or None
        lower_bound_raw = opts.get("lowerbound") or None
        upper_bound_raw = opts.get("upperbound") or None

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

        push_down_predicate = opts.get("pushdownpredicate", "true").lower() == "true"

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

        opts = {key.lower(): value for key, value in self.options.items()}
        custom_schema_ddl = opts.get("customschema") or None
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
        * ``batchsize`` — rows per ingest call (default 1000)
        * ``truncate`` — Spark-compatible truncate request (MySQL / SQL Server)
        * ``sail.jdbc.overwriteMode`` — ``"atomic"`` or ``"truncate"``;
          PostgreSQL-only Sail extension for explicit overwrite mode

        Usage::

            df.write.format("jdbc") \\
                .option("url", "jdbc:postgresql://localhost:5432/mydb") \\
                .option("dbtable", "public.events") \\
                .mode("overwrite") \\
                .save()
        """
        opts = {key.lower(): value for key, value in self.options.items()}

        url = opts.get("url")
        if not url:
            msg = "Option 'url' is required for the jdbc data source"
            raise ValueError(msg)
        subprotocol = url[5:].split("://", 1)[0].split(":", 1)[0] if url.startswith("jdbc:") else ""
        if subprotocol == "postgresql":
            supported = {
                "url",
                "dbtable",
                "user",
                "password",
                "driver",
                "batchsize",
                "numpartitions",
                "truncate",
                "cascadetruncate",
                "isolationlevel",
                "querytimeout",
                "createtableoptions",
                "createtablecolumntypes",
                "tablecomment",
                "sail.jdbc.overwritemode",
                "applicationname",
                "connecttimeout",
                "sslmode",
                "sslcert",
                "sslkey",
                "sslrootcert",
                "options",
                "__sail_save_mode",
                "__sail_case_sensitive",
            }
            if unsupported := next((key for key in self.options if key.lower() not in supported), None):
                msg = f"PostgreSQL JDBC property {unsupported} has no equivalent in Sail's native client."
                raise ValueError(msg)
        driver = opts.get("driver")
        native_drivers = {
            "postgresql": {"org.postgresql.Driver"},
            "mysql": {"com.mysql.cj.jdbc.Driver", "com.mysql.jdbc.Driver"},
            "sqlserver": {"com.microsoft.sqlserver.jdbc.SQLServerDriver"},
        }
        if driver and driver not in native_drivers.get(subprotocol, set()):
            msg = (
                f"Option 'driver'={driver!r} cannot be loaded by Sail's Python JDBC backend; "
                f"expected one of {sorted(native_drivers.get(subprotocol, set()))!r}."
            )
            raise ValueError(msg)

        if opts.get("query"):
            msg = "Cannot write to a 'query'; specify 'dbtable' (a table name) for writes."
            raise ValueError(msg)

        dbtable = opts.get("dbtable", "").strip()
        if not dbtable:
            msg = "Option 'dbtable' is required for jdbc writes."
            raise ValueError(msg)

        user = opts.get("user") or None
        password = opts.get("password") or None
        conn_str = _jdbc_url_to_dsn(url, user, password)
        if subprotocol == "postgresql":
            conn_str = _postgresql_dsn_with_properties(conn_str, opts)
        try:
            batch_size = int(opts.get("batchsize", "1000"))
        except ValueError:
            msg = "Option 'batchsize' must be a positive integer."
            raise ValueError(msg) from None
        if batch_size <= 0:
            msg = f"Option 'batchsize' must be a positive integer, got {batch_size}."
            raise ValueError(msg)
        isolation_level = opts.get("isolationlevel", "READ_UNCOMMITTED")
        valid_isolation_levels = {
            "NONE",
            "READ_UNCOMMITTED",
            "READ_COMMITTED",
            "REPEATABLE_READ",
            "SERIALIZABLE",
        }
        if isolation_level not in valid_isolation_levels:
            msg = f"Invalid value {isolation_level!r} for option 'isolationLevel'."
            raise ValueError(msg)
        try:
            query_timeout = int(opts.get("querytimeout", "0"))
        except ValueError:
            msg = "Option 'queryTimeout' must be a non-negative integer."
            raise ValueError(msg) from None
        if query_timeout < 0:
            msg = f"Option 'queryTimeout' must be a non-negative integer, got {query_timeout}."
            raise ValueError(msg)
        if "numpartitions" in opts:
            try:
                num_partitions = int(opts["numpartitions"])
            except ValueError:
                msg = "Option 'numPartitions' must be a positive integer."
                raise ValueError(msg) from None
            if num_partitions <= 0:
                msg = f"Option 'numPartitions' must be a positive integer, got {num_partitions}."
                raise ValueError(msg)

        import uuid  # noqa: PLC0415

        run_id = uuid.uuid4().hex[:12]
        save_mode = opts.get("__sail_save_mode", "overwrite" if overwrite else "append").lower()
        if save_mode not in {"errorifexists", "ignore", "append", "overwrite"}:
            msg = f"Unsupported JDBC save mode {save_mode!r}."
            raise ValueError(msg)
        truncate_value = opts.get("truncate", "false").lower()
        if truncate_value not in {"true", "false"}:
            msg = f"Option 'truncate' must be 'true' or 'false', got {truncate_value!r}."
            raise ValueError(msg)
        truncate_requested = truncate_value == "true"
        cascade_truncate_value = opts.get("cascadetruncate", "false").lower()
        if cascade_truncate_value not in {"true", "false"}:
            msg = f"Option 'cascadeTruncate' must be 'true' or 'false', got {cascade_truncate_value!r}."
            raise ValueError(msg)
        cascade_truncate = cascade_truncate_value == "true"
        create_table_options = opts.get("createtableoptions", "")
        create_table_column_types = opts.get("createtablecolumntypes", "")
        table_comment = opts.get("tablecomment", "")
        case_sensitive = opts.get("__sail_case_sensitive", "false").lower() == "true"
        sail_overwrite_mode = opts.get("sail.jdbc.overwritemode")
        if sail_overwrite_mode is not None:
            sail_overwrite_mode = sail_overwrite_mode.lower()
            if save_mode != "overwrite":
                msg = "Option 'sail.jdbc.overwriteMode' requires mode('overwrite')."
                raise ValueError(msg)
            if subprotocol != "postgresql":
                msg = "Option 'sail.jdbc.overwriteMode' is supported only for PostgreSQL."
                raise ValueError(msg)
            if sail_overwrite_mode not in {"atomic", "truncate"}:
                msg = f"Option 'sail.jdbc.overwriteMode' must be 'atomic' or 'truncate', got {sail_overwrite_mode!r}."
                raise ValueError(msg)
            if truncate_requested:
                msg = "Options 'truncate' and 'sail.jdbc.overwriteMode' cannot be combined."
                raise ValueError(msg)

        if subprotocol == "postgresql":
            overwrite_mode = sail_overwrite_mode or (
                "truncate" if overwrite and truncate_requested else "spark" if overwrite else "append"
            )
            return JdbcDataSourceWriter(
                conn_str=conn_str,
                dbtable=dbtable,
                schema=schema,
                save_mode=save_mode,
                overwrite_mode=overwrite_mode,
                batch_size=batch_size,
                case_sensitive=case_sensitive,
                cascade_truncate=cascade_truncate,
                create_table_options=create_table_options,
                create_table_column_types=create_table_column_types,
                isolation_level=isolation_level,
                query_timeout=query_timeout,
                table_comment=table_comment,
                run_id=run_id,
            )

        if subprotocol in ("mysql", "sqlserver"):
            sa_url, connect_args = _sqlalchemy_url(conn_str)
            return SqlAlchemyDataSourceWriter(
                url=sa_url,
                dbtable=dbtable,
                schema=schema,
                dialect="mysql" if subprotocol == "mysql" else "mssql",
                save_mode=save_mode,
                truncate=overwrite and truncate_requested,
                case_sensitive=case_sensitive,
                batch_size=batch_size,
                create_table_options=create_table_options,
                create_table_column_types=create_table_column_types,
                isolation_level=isolation_level,
                query_timeout=query_timeout,
                table_comment=table_comment,
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
