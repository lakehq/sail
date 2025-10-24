"""Database backend implementations for JDBC reader.

Available backends:
- ConnectorX: Rust-based, high-performance, distributed reading
- ADBC: Arrow Database Connectivity standard
- Fallback: Row-wise fallback using pyodbc (slow, for testing only)
"""

from pysail.jdbc.backends.adbc import ADBCBackend
from pysail.jdbc.backends.base import DatabaseBackend
from pysail.jdbc.backends.connectorx import ConnectorXBackend
from pysail.jdbc.backends.fallback import FallbackBackend

__all__ = [
    "DatabaseBackend",
    "ConnectorXBackend",
    "ADBCBackend",
    "FallbackBackend",
    "get_backend",
]


def get_backend(engine: str) -> DatabaseBackend:
    """
    Get backend instance by name.

    Args:
        engine: Backend name ('connectorx', 'adbc', or 'fallback')

    Returns:
        DatabaseBackend instance

    Raises:
        ValueError: If engine name is invalid
    """
    if engine == "connectorx":
        return ConnectorXBackend()
    if engine == "adbc":
        return ADBCBackend()
    if engine == "fallback":
        return FallbackBackend()
    valid_engines = "connectorx, adbc, fallback"
    message = f"Invalid engine: {engine}. Must be one of: {valid_engines}"
    raise ValueError(message)
