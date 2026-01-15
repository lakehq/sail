"""
Python DataSource API for Sail.

This module provides the DataSource base class and related types for implementing
custom data sources in Python. The API is 100% compatible with PySpark 4.0+.

Example usage:

    from pysail.spark.datasource import DataSource, DataSourceReader, InputPartition

    class MyDataSource(DataSource):
        @classmethod
        def name(cls):
            return "myformat"

        def schema(self):
            return "id INT, name STRING"

        def reader(self, schema):
            return MyReader(self.options)

    class MyReader(DataSourceReader):
        def read(self, partition):
            yield (1, "Alice")
            yield (2, "Bob")
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

try:
    import pyarrow as pa

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False
    pa = None


# Type alias for column paths (nested columns)
ColumnPath = Tuple[str, ...]


class CaseInsensitiveDict(dict):
    """A dictionary with case-insensitive keys."""

    def __getitem__(self, key):
        return super().__getitem__(key.lower())

    def __setitem__(self, key, value):
        super().__setitem__(key.lower(), value)

    def __contains__(self, key):
        return super().__contains__(key.lower())

    def get(self, key, default=None):
        return super().get(key.lower(), default)


# ============================================================================
# Filter Classes (PySpark-compatible)
# ============================================================================


@dataclass(frozen=True)
class EqualTo:
    """Filter: column == value"""

    column: ColumnPath
    value: Any


@dataclass(frozen=True)
class EqualNullSafe:
    """Filter: column <=> value (null-safe equals)"""

    column: ColumnPath
    value: Any


@dataclass(frozen=True)
class GreaterThan:
    """Filter: column > value"""

    column: ColumnPath
    value: Any


@dataclass(frozen=True)
class GreaterThanOrEqual:
    """Filter: column >= value"""

    column: ColumnPath
    value: Any


@dataclass(frozen=True)
class LessThan:
    """Filter: column < value"""

    column: ColumnPath
    value: Any


@dataclass(frozen=True)
class LessThanOrEqual:
    """Filter: column <= value"""

    column: ColumnPath
    value: Any


@dataclass(frozen=True)
class In:
    """Filter: column IN (values)"""

    column: ColumnPath
    values: Tuple[Any, ...]


@dataclass(frozen=True)
class IsNull:
    """Filter: column IS NULL"""

    column: ColumnPath


@dataclass(frozen=True)
class IsNotNull:
    """Filter: column IS NOT NULL"""

    column: ColumnPath


@dataclass(frozen=True)
class Not:
    """Filter: NOT child"""

    child: Any  # Another filter


@dataclass(frozen=True)
class And:
    """Filter: left AND right"""

    left: Any  # Another filter
    right: Any  # Another filter


@dataclass(frozen=True)
class Or:
    """Filter: left OR right"""

    left: Any  # Another filter
    right: Any  # Another filter


@dataclass(frozen=True)
class StringStartsWith:
    """Filter: column LIKE 'value%'"""

    column: ColumnPath
    value: str


@dataclass(frozen=True)
class StringEndsWith:
    """Filter: column LIKE '%value'"""

    column: ColumnPath
    value: str


@dataclass(frozen=True)
class StringContains:
    """Filter: column LIKE '%value%'"""

    column: ColumnPath
    value: str


# All filter types for convenience
Filter = Union[
    EqualTo,
    EqualNullSafe,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    In,
    IsNull,
    IsNotNull,
    Not,
    And,
    Or,
    StringStartsWith,
    StringEndsWith,
    StringContains,
]


# ============================================================================
# Core Classes
# ============================================================================


class InputPartition:
    """
    Represents a partition of data to be read.

    Users can subclass this to add partition-specific data (e.g., file paths,
    row ranges, etc.). The partition must be picklable for distributed execution.
    """

    def __init__(self, partition_id: int = 0):
        self.partition_id = partition_id

    def __repr__(self):
        return f"InputPartition({self.partition_id})"


class DataSourceReader(ABC):
    """
    Base class for reading data from a partition.

    Subclass this and implement the required methods to define how data
    is read from a Python datasource.
    """

    def pushFilters(self, filters: List[Filter]) -> Iterator[Filter]:
        """
        Push filters to the reader.

        Called by Sail to push filters down to the datasource. The reader
        should accept filters it can handle and return (yield) filters it cannot.

        Args:
            filters: List of filter objects to push down

        Yields:
            Filters that cannot be handled by this reader (will be applied by Sail)
        """
        # Default: reject all filters (Sail will apply them)
        for f in filters:
            yield f

    def partitions(self) -> List[InputPartition]:
        """
        Return the list of partitions for parallel reading.

        Default implementation returns a single partition.
        Override to enable parallel reading across multiple partitions.

        Returns:
            List of InputPartition objects
        """
        return [InputPartition(0)]

    @abstractmethod
    def read(self, partition: InputPartition) -> Iterator[Union[Tuple, "pa.RecordBatch"]]:
        """
        Read data from a partition.

        Args:
            partition: The partition to read from

        Yields:
            Either tuples (rows) or PyArrow RecordBatches
        """
        pass


class DataSource(ABC):
    """
    Base class for user-defined data sources.

    Subclass this to implement custom data sources in Python.
    The API is 100% compatible with PySpark's DataSource API.

    Example:

        class MyDataSource(DataSource):
            @classmethod
            def name(cls):
                return "myformat"

            def schema(self):
                return "id INT, name STRING"

            def reader(self, schema):
                return MyReader(self.options)
    """

    def __init__(self, options: Dict[str, str]):
        """
        Initialize the datasource with options.

        Args:
            options: Case-insensitive dictionary of options from the user
        """
        self.options = CaseInsensitiveDict(options or {})

    @classmethod
    @abstractmethod
    def name(cls) -> str:
        """
        Return the short name of this data source.

        This is the name used in `spark.read.format("name")`.
        """
        pass

    @abstractmethod
    def schema(self) -> str:
        """
        Return the schema of the data source.

        Returns:
            PyArrow Schema object (preferred), OR a DDL-formatted schema string.
            
            DDL strings support: INT, BIGINT, FLOAT, DOUBLE, BOOLEAN, STRING,
            DATE, TIMESTAMP, DECIMAL(p,s). For complex types (arrays, structs),
            use PyArrow Schema.
        """
        pass

    @abstractmethod
    def reader(self, schema) -> DataSourceReader:
        """
        Create a reader for this data source.

        Args:
            schema: The schema to use for reading (PyArrow Schema)

        Returns:
            DataSourceReader instance
        """
        pass


# ============================================================================
# Registration
# ============================================================================

_REGISTERED_DATASOURCES: Dict[str, type] = {}


def register(datasource_class: type) -> type:
    """
    Register a Python datasource class.

    Can be used as a decorator:

        @register
        class MyDataSource(DataSource):
            ...

    Or called directly:

        register(MyDataSource)

    Args:
        datasource_class: A DataSource subclass

    Returns:
        The datasource class (for decorator use)
    """
    if not issubclass(datasource_class, DataSource):
        raise TypeError(f"{datasource_class} must be a subclass of DataSource")

    name = datasource_class.name()
    _REGISTERED_DATASOURCES[name.lower()] = datasource_class
    return datasource_class


def get_registered_datasource(name: str) -> Optional[type]:
    """Get a registered datasource by name."""
    return _REGISTERED_DATASOURCES.get(name.lower())


def list_registered_datasources() -> List[str]:
    """List all registered datasource names."""
    return list(_REGISTERED_DATASOURCES.keys())


def discover_entry_points(group: str = "sail.datasources") -> List[Tuple[str, type]]:
    """
    Discover datasources from Python entry points.

    Compatible with Python 3.9+ (handles API differences between 3.9 and 3.10+).
    """
    from importlib.metadata import entry_points

    try:
        # Python 3.10+: entry_points(group=...) returns iterable
        eps = entry_points(group=group)
    except TypeError:
        # Python 3.9: entry_points() returns SelectableGroups (dict-like)
        all_eps = entry_points()
        eps = getattr(all_eps, "get", lambda g, d: d)(group, [])

    result = []
    for ep in eps:
        try:
            cls = ep.load()
            result.append((ep.name, cls))
        except Exception:
            pass
    return result


# ============================================================================
# Re-exports for convenience
# ============================================================================

# Import PySpark compatibility functions from compat module
from .compat import install_pyspark_shim, is_shim_installed  # noqa: E402

# Legacy alias for backward compatibility
setup_pyspark_compat_shim = install_pyspark_shim

# NOTE: The shim is NOT auto-installed on import. This is intentional.
# - Server-side: unpickle_datasource_class() installs the shim automatically
# - Client-side: Users who need the shim (e.g., for testing without PySpark)
#   should call install_pyspark_shim() explicitly


__all__ = [
    # Core classes
    "DataSource",
    "DataSourceReader",
    "InputPartition",
    "CaseInsensitiveDict",
    # Filters
    "EqualTo",
    "EqualNullSafe",
    "GreaterThan",
    "GreaterThanOrEqual",
    "LessThan",
    "LessThanOrEqual",
    "In",
    "IsNull",
    "IsNotNull",
    "Not",
    "And",
    "Or",
    "StringStartsWith",
    "StringEndsWith",
    "StringContains",
    "Filter",
    "ColumnPath",
    # Registration
    "register",
    "get_registered_datasource",
    "list_registered_datasources",
    "discover_entry_points",
    # PySpark compatibility
    "setup_pyspark_compat_shim",
    "install_pyspark_shim",
    "is_shim_installed",
]
