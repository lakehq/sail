"""Database backend implementations for JDBC reader.

Available backends:
- ConnectorX: Rust-based, high-performance, distributed reading
- ADBC: Arrow Database Connectivity standard
- Fallback: Row-wise fallback using pyodbc (slow, for testing only)
"""

import logging

from pysail.jdbc.backends.adbc import ADBCBackend
from pysail.jdbc.backends.base import DatabaseBackend
from pysail.jdbc.backends.connectorx import ConnectorXBackend
from pysail.jdbc.backends.fallback import FallbackBackend
from pysail.jdbc.exceptions import BackendNotAvailableError

logger = logging.getLogger("lakesail.jdbc")

__all__ = [
    "DatabaseBackend",
    "ConnectorXBackend",
    "ADBCBackend",
    "FallbackBackend",
    "get_backend",
]


def get_backend(engine: str, *, auto_fallback: bool = True) -> DatabaseBackend:
    """
    Get backend instance by name with automatic fallback.

    Args:
        engine: Backend name ('connectorx', 'adbc', or 'fallback')
        auto_fallback: If True, automatically fall back to next available backend

    Returns:
        DatabaseBackend instance

    Raises:
        BackendNotAvailableError: If no backend is available
        ValueError: If engine name is invalid and auto_fallback is False
    """
    # Define backend preference order
    backend_order = {
        "connectorx": [ConnectorXBackend, ADBCBackend, FallbackBackend],
        "adbc": [ADBCBackend, ConnectorXBackend, FallbackBackend],
        "fallback": [FallbackBackend],
    }

    if engine not in backend_order:
        valid_engines = "connectorx, adbc, fallback"
        message = f"Invalid engine: {engine}. Must be one of: {valid_engines}"
        raise ValueError(message)

    backends_to_try = backend_order[engine]

    # Try backends in order
    for backend_class in backends_to_try:
        try:
            backend = backend_class()
            if backend.is_available():
                if backend_class != backends_to_try[0] and auto_fallback:
                    logger.warning(
                        "Requested backend '%s' not available, using '%s' instead",
                        engine,
                        backend.get_name(),
                    )
                return backend
        except BackendNotAvailableError:
            continue

    # No backend available
    attempted = ", ".join(backend.__name__ for backend in backends_to_try)
    message = (
        "No JDBC backend available. "
        f"Tried: {attempted}. Install at least one: "
        "pip install connectorx OR pip install adbc-driver-manager"
    )
    raise BackendNotAvailableError(message)
