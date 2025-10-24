"""ConnectorX backend implementation."""

from __future__ import annotations

import importlib
import logging

import pyarrow as pa

from pysail.jdbc.backends.base import DatabaseBackend
from pysail.jdbc.exceptions import BackendNotAvailableError, DatabaseError
from pysail.jdbc.utils import mask_credentials

logger = logging.getLogger("lakesail.jdbc")


class ConnectorXBackend(DatabaseBackend):
    """Backend using ConnectorX (distributed, Rust-based)."""

    def __init__(self) -> None:
        """Import connectorx and capture basic metadata."""
        try:
            import connectorx as cx
        except ImportError as err:  # pragma: no cover - optional dependency
            message = "ConnectorX backend not installed. " "Install with: pip install 'connectorx>=0.3.0'"
            raise BackendNotAvailableError(message) from err

        self.cx = cx
        version_text = getattr(cx, "__version__", "0.0.0")
        self.version = self._parse_version(version_text)

        if self.version == (0, 0):
            logger.warning(
                "Could not parse ConnectorX version: %s",
                version_text,
            )
        elif self.version < (0, 3):
            logger.warning(
                "ConnectorX version %s is older than recommended 0.3.0. Arrow return type may not be available.",
                version_text,
            )

    @staticmethod
    def _parse_version(version_text: str) -> tuple[int, int]:
        """Parse the major/minor components of the version string."""
        try:
            return tuple(map(int, version_text.split(".")[:2]))
        except (ValueError, AttributeError):
            return (0, 0)

    def read_batches(
        self,
        connection_string: str,
        query: str,
        fetch_size: int = 10000,
    ) -> list[pa.RecordBatch]:
        """
        Read and return Arrow RecordBatches.

        Args:
            connection_string: Database connection string
            query: SQL query to execute
            fetch_size: Batch size hint (unused, retained for API parity)

        Returns:
            List of Arrow RecordBatches
        """
        masked_connection = mask_credentials(connection_string)
        logger.info("ConnectorX reading from %s", masked_connection)
        logger.debug("Query: %s", query)
        logger.debug("Requested fetch_size hint: %s", fetch_size)

        try:
            arrow_table = self._run_query(connection_string, query)
        except DatabaseError:
            raise
        except Exception as err:  # pragma: no cover - defensive logging path
            safe_err = str(err).replace(connection_string, masked_connection)
            logger.exception("ConnectorX error: %s", safe_err)
            message = f"ConnectorX error: {safe_err}"
            raise DatabaseError(message) from err

        batches = arrow_table.to_batches()
        logger.info(
            "ConnectorX read %s rows in %s batches from %s",
            arrow_table.num_rows,
            len(batches),
            masked_connection,
        )

        return batches

    def _run_query(self, connection_string: str, query: str) -> pa.Table:
        """Execute the query using ConnectorX, with compatibility fallbacks."""
        try:
            table = self.cx.read_sql(
                conn=connection_string,
                query=query,
                return_type="arrow",
            )
        except Exception as err:  # pragma: no cover - compatibility path
            if "return_type" in str(err):
                logger.warning(
                    "Falling back to 'arrow2' return type for older ConnectorX version",
                )
                table = self.cx.read_sql(
                    conn=connection_string,
                    query=query,
                    return_type="arrow2",
                )
            else:
                raise

        if not isinstance(table, pa.Table):
            message = f"ConnectorX returned unexpected type: {type(table)}. " "Expected pyarrow.Table"
            raise DatabaseError(message)

        return table

    def get_name(self) -> str:
        """Get backend name."""
        return "ConnectorX"

    def is_available(self) -> bool:
        """Check if ConnectorX is available."""
        return importlib.util.find_spec("connectorx") is not None
