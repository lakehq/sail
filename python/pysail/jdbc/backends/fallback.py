"""Fallback backend using pyodbc (row-wise, slow, for testing only)."""

from __future__ import annotations

import importlib
import logging
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from pysail.jdbc.backends.base import DatabaseBackend
from pysail.jdbc.exceptions import BackendNotAvailableError, DatabaseError
from pysail.jdbc.utils import mask_credentials

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

logger = logging.getLogger("lakesail.jdbc")


class FallbackBackend(DatabaseBackend):
    """
    Row-wise fallback using pyodbc.

    ⚠️ WARNING: This backend is intended for development and very small workloads.
    """

    ROW_LIMIT = 1_000_000

    def __init__(self) -> None:
        """Import pyodbc lazily and expose it on the instance."""
        try:
            import pyodbc  # type: ignore import-not-found
        except ImportError as err:  # pragma: no cover - optional dependency
            message = "Fallback backend requires pyodbc. " "Install with: pip install pyodbc"
            raise BackendNotAvailableError(message) from err

        self.pyodbc = pyodbc

    def read_batches(
        self,
        connection_string: str,
        query: str,
        fetch_size: int = 10000,
    ) -> list[pa.RecordBatch]:
        """
        Read via ODBC (row-wise; slow).

        Args:
            connection_string: ODBC connection string
            query: SQL query to execute
            fetch_size: Batch size for the cursor
        """
        conn_str = self._build_odbc_connection_string(connection_string)
        masked_conn = mask_credentials(conn_str)
        masked_uri = mask_credentials(connection_string)

        logger.warning(
            "Using fallback backend (slow, row-wise). Consider installing ConnectorX for better performance: %s",
            masked_conn,
        )
        logger.debug("Query: %s", query)

        try:
            with self.pyodbc.connect(conn_str) as conn, conn.cursor() as cursor:
                cursor.execute(query)
                rows = self._fetch_all(cursor, fetch_size)

                if not rows:
                    logger.warning("Query returned no rows")
                    schema = self._cursor_to_arrow_schema(cursor.description)
                    empty_batch = pa.RecordBatch.from_pydict({}, schema=schema)
                    return [empty_batch]

                schema, arrays = self._rows_to_arrow(cursor.description, rows)
                table = pa.Table.from_arrays(arrays, schema=schema)

        except DatabaseError:
            raise
        except self.pyodbc.Error as err:  # pragma: no cover - defensive logging path
            safe_err = str(err).replace(conn_str, masked_conn)
            safe_err = safe_err.replace(connection_string, masked_uri)
            logger.exception("Fallback backend error: %s", safe_err)
            message = f"Fallback backend error: {safe_err}"
            raise DatabaseError(message) from err

        logger.info(
            "Fallback backend read %s rows from %s",
            table.num_rows,
            masked_conn,
        )

        return table.to_batches()

    def _build_odbc_connection_string(self, uri: str) -> str:
        """Convert URI to ODBC connection string."""
        # This implementation currently relies on pyodbc accepting the URI as-is.
        return uri

    def _fetch_all(self, cursor, fetch_size: int) -> list[tuple[Any, ...]]:
        """Fetch rows in batches, enforcing the safety limit."""
        rows: list[tuple[Any, ...]] = []
        row_count = 0

        while True:
            batch = cursor.fetchmany(fetch_size)
            if not batch:
                break

            rows.extend(batch)
            row_count += len(batch)

            if row_count > self.ROW_LIMIT:
                message = (
                    f"Result set exceeds {self.ROW_LIMIT:,} rows. " "Use ConnectorX or ADBC backend for large tables."
                )
                raise DatabaseError(message)

        return rows

    def _cursor_to_arrow_schema(self, description: Sequence[Sequence[Any]]) -> pa.Schema:
        """Build Arrow schema from cursor description (for empty results)."""
        fields = [pa.field(col[0], pa.string()) for col in description]
        return pa.schema(fields)

    def _rows_to_arrow(
        self,
        description: Sequence[Sequence[Any]],
        rows: Iterable[tuple[Any, ...]],
    ) -> tuple[pa.Schema, list[pa.Array]]:
        """Convert rows to Arrow schema and arrays."""
        row_list = list(rows)
        if not row_list:
            schema = self._cursor_to_arrow_schema(description)
            arrays = [pa.array(()) for _ in description]
            return schema, arrays

        col_names = [col[0] for col in description]
        columns = list(zip(*row_list))

        arrays: list[pa.Array] = []
        fields: list[pa.Field] = []

        for col_name, col_data in zip(col_names, columns, strict=False):
            try:
                arrow_array = pa.array(col_data)
                arrays.append(arrow_array)
                fields.append(pa.field(col_name, arrow_array.type))
            except (
                pa.ArrowInvalid,
                TypeError,
                ValueError,
            ) as err:  # pragma: no cover - type inference fallback
                logger.warning(
                    "Could not infer type for column '%s', falling back to string: %s",
                    col_name,
                    err,
                )
                arrow_array = pa.array([str(value) if value is not None else None for value in col_data])
                arrays.append(arrow_array)
                fields.append(pa.field(col_name, pa.string()))

        schema = pa.schema(fields)
        return schema, arrays

    def get_name(self) -> str:
        """Get backend name."""
        return "Fallback (pyodbc)"

    def is_available(self) -> bool:
        """Check if pyodbc is available."""
        return importlib.util.find_spec("pyodbc") is not None
