"""ADBC (Arrow Database Connectivity) backend implementation."""

from __future__ import annotations

import importlib
import importlib.util
import logging
from typing import TYPE_CHECKING

import pyarrow as pa

from pysail.jdbc.backends.base import DatabaseBackend
from pysail.jdbc.exceptions import (
    BackendNotAvailableError,
    DatabaseError,
    UnsupportedDatabaseError,
)
from pysail.jdbc.utils import mask_credentials

if TYPE_CHECKING:
    from types import ModuleType

logger = logging.getLogger("lakesail.jdbc")


class ADBCBackend(DatabaseBackend):
    """Backend using ADBC (Arrow DB Connectivity standard)."""

    def __init__(self) -> None:
        """Ensure driver manager is present."""
        try:
            import adbc_driver_manager  # noqa: F401
        except ImportError as err:  # pragma: no cover - import error path
            message = "ADBC driver manager not installed. " "Install with: pip install adbc-driver-manager"
            raise BackendNotAvailableError(message) from err

    def read_batches(
        self,
        connection_string: str,
        query: str,
        fetch_size: int = 10000,
    ) -> list[pa.RecordBatch]:
        """
        Read query results from an ADBC-compatible database.

        Args:
            connection_string: Database connection string
            query: SQL query to execute
            fetch_size: Batch size hint for drivers that honour it

        Returns:
            Record batches containing the query results
        """
        masked_connection = mask_credentials(connection_string)
        logger.info("ADBC reading from %s", masked_connection)
        logger.debug("Query: %s", query)
        logger.debug("Requested fetch_size hint: %s", fetch_size)

        driver_module = self._get_driver_module(connection_string)

        batches: list[pa.RecordBatch] = []
        method_used: str | None = None

        try:
            with (
                driver_module.connect(connection_string) as conn,
                conn.cursor() as cursor,
            ):
                if hasattr(cursor, "arraysize"):
                    cursor.arraysize = fetch_size
                cursor.execute(query)

                if hasattr(cursor, "fetch_arrow_batches"):
                    method_used = "fetch_arrow_batches"
                    logger.debug("Using fetch_arrow_batches() (ADBC >= 1.6.0)")
                    for batch in cursor.fetch_arrow_batches():
                        if isinstance(batch, pa.RecordBatch):
                            batches.append(batch)
                        else:
                            batches.extend(batch.to_batches())
                elif hasattr(cursor, "fetch_arrow_table"):
                    method_used = "fetch_arrow_table"
                    logger.debug("Using fetch_arrow_table() (ADBC < 1.6.0)")
                    table = cursor.fetch_arrow_table()
                    batches = table.to_batches()
        except DatabaseError:
            raise
        except Exception as err:  # pragma: no cover - defensive logging path
            safe_err = str(err).replace(connection_string, masked_connection)
            logger.exception("ADBC error: %s", safe_err)
            message = f"ADBC error: {safe_err}"
            raise DatabaseError(message) from err

        if method_used is None:
            message = (
                "ADBC cursor has neither fetch_arrow_batches() nor fetch_arrow_table(). "
                "Please upgrade adbc-driver-manager."
            )
            raise DatabaseError(message)

        total_rows = sum(batch.num_rows for batch in batches)
        logger.info(
            "ADBC read %s rows in %s batches from %s",
            total_rows,
            len(batches),
            masked_connection,
        )

        return batches

    def _get_driver_module(self, connection_string: str) -> ModuleType:
        """Resolve the Python module that can handle the requested scheme."""
        scheme = connection_string.split("://")[0].lower() if "://" in connection_string else ""

        module_map = {
            "postgresql": "adbc_driver_postgresql.dbapi",
            "postgres": "adbc_driver_postgresql.dbapi",
            "sqlite": "adbc_driver_sqlite.dbapi",
            "snowflake": "adbc_driver_snowflake.dbapi",
        }

        module_name = module_map.get(scheme)
        if not module_name:
            supported = ", ".join(sorted(module_map))
            message = f"ADBC does not support scheme '{scheme}'. Supported: {supported}"
            raise UnsupportedDatabaseError(message)

        try:
            module = importlib.import_module(module_name)
        except ImportError as err:  # pragma: no cover - optional dependency
            package_name = module_name.split(".")[0].replace("_", "-")
            message = f"ADBC driver '{package_name}' not installed. " f"Install with: pip install {package_name}"
            raise BackendNotAvailableError(message) from err
        else:
            return module

    def get_name(self) -> str:
        """Return the backend name."""
        return "ADBC"

    def is_available(self) -> bool:
        """Report whether the driver manager is available."""
        return importlib.util.find_spec("adbc_driver_manager") is not None
