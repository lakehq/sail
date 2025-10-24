"""Abstract base class for database backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa


class DatabaseBackend(ABC):
    """Abstract interface for database reading backends."""

    @abstractmethod
    def read_batches(
        self,
        connection_string: str,
        query: str,
        fetch_size: int = 10000,
    ) -> list[pa.RecordBatch]:
        """
        Read query results as Arrow RecordBatches.

        Args:
            connection_string: Database connection string (NOT JDBC URL)
            query: SQL query to execute
            fetch_size: Number of rows to fetch per batch

        Returns:
            List of Arrow RecordBatches

        Raises:
            DatabaseError: If read operation fails
            BackendNotAvailableError: If backend library is not installed
        """

    @abstractmethod
    def get_name(self) -> str:
        """
        Get backend name.

        Returns:
            Backend name (e.g., 'ConnectorX', 'ADBC')
        """

    @abstractmethod
    def is_available(self) -> bool:
        """
        Check if backend is available (library installed).

        Returns:
            True if backend can be used
        """
