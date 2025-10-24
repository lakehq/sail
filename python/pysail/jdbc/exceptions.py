"""Custom exceptions for JDBC reader module."""


class JDBCReaderError(Exception):
    """Base exception for JDBC reader errors."""


class InvalidJDBCUrlError(JDBCReaderError):
    """Raised when JDBC URL format is invalid."""


class BackendNotAvailableError(JDBCReaderError):
    """Raised when requested backend is not installed."""


class UnsupportedDatabaseError(JDBCReaderError):
    """Raised when database driver is not supported by any backend."""


class DatabaseError(JDBCReaderError):
    """Raised when database operation fails."""


class SchemaInferenceError(JDBCReaderError):
    """Raised when Arrow schema cannot be inferred."""


class InvalidOptionsError(JDBCReaderError):
    """Raised when JDBC options are invalid or inconsistent."""
