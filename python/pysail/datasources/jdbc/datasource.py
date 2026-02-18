"""JDBC data source for Sail, backed by connectorX.

Supports ``spark.read.format("jdbc")`` and ``spark.read.jdbc()`` with options
consistent with the PySpark JDBC API.

Install the optional dependency before use::

    pip install pysail[jdbc]
"""

from __future__ import annotations

import datetime
from urllib.parse import quote

try:
    import connectorx as cx
except ImportError as e:
    msg = "connectorX is required for the JDBC data source. Install it with: pip install pysail[jdbc]"
    raise ImportError(msg) from e

from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from collections.abc import Iterator

try:
    from pyspark.sql.datasource import (
        DataSource,
        DataSourceReader,
        EqualTo,
        Filter,
        GreaterThan,
        GreaterThanOrEqual,
        InputPartition,
        LessThan,
        LessThanOrEqual,
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
        idx = dsn.index(sep)
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
# DataSource
# ============================================================================


class JdbcDataSource(DataSource):
    """JDBC data source backed by connectorX.

    Register and use::

        from pysail.datasources.jdbc import JdbcDataSource

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

    Not supported: ``driver``, ``predicates`` list, write operations,
    ``queryTimeout``, ``isolationLevel``, ``sessionInitStatement``, Kerberos.
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
        dbtable = opts.get("dbtable") or None
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
