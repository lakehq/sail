"""Vortex data source for Sail, backed by the ``vortex`` Python library.

Supports ``spark.read.format("vortex")`` with filter pushdown and
zero-copy Arrow RecordBatch yield.

Install the optional dependency before use::

    pip install vortex

Usage::

    from pysail.spark.datasource.vortex import VortexDataSource

    spark.dataSource.register(VortexDataSource)
    df = spark.read.format("vortex").option("path", "/data/file.vtx").load()
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

try:
    import vortex
    from vortex.expr import and_, column, not_
except ImportError as e:
    msg = "vortex is required for the Vortex data source. Install it with: pip install vortex"
    raise ImportError(msg) from e

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    InputPartition,
    IsNotNull,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    StringContains,
    StringEndsWith,
    StringStartsWith,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pyspark.sql.datasource import Filter

# ============================================================================
# Filter helpers
# ============================================================================

# PySpark comparison filters -> Python dunder method name
_COMPARISON_OPS: dict[type, str] = {
    EqualTo: "__eq__",
    GreaterThan: "__gt__",
    GreaterThanOrEqual: "__ge__",
    LessThan: "__lt__",
    LessThanOrEqual: "__le__",
}


def _col_name(f: Filter) -> str:
    """Extract the column name from a PySpark Filter."""
    return f.attribute[0] if isinstance(f.attribute, tuple) else f.attribute


def _filter_to_tuple(f: Filter) -> tuple | None:
    """Convert a PySpark Filter to a pickle-safe tuple.

    Returns None if the filter type is not supported.
    Tuple formats:
      - Comparison: (col, op, value)     e.g. ("id", "__gt__", 5)
      - IsNull:     (col, "is_null")
      - IsNotNull:  (col, "is_not_null")
      - In:         (col, "in", (v1, v2, ...))
      - Not:        ("not", child_tuple)
      - String:     (col, "starts_with"|"ends_with"|"contains", value)
    """
    op = _COMPARISON_OPS.get(type(f))
    if op is not None:
        return (_col_name(f), op, f.value)

    if isinstance(f, IsNull):
        return (_col_name(f), "is_null")
    if isinstance(f, IsNotNull):
        return (_col_name(f), "is_not_null")
    if isinstance(f, In):
        return (_col_name(f), "in", f.value)
    if isinstance(f, Not):
        child = _filter_to_tuple(f.child)
        return ("not", child) if child is not None else None
    if isinstance(f, StringStartsWith):
        return (_col_name(f), "starts_with", f.value)
    if isinstance(f, StringEndsWith):
        return (_col_name(f), "ends_with", f.value)
    if isinstance(f, StringContains):
        return (_col_name(f), "contains", f.value)
    return None


def _tuple_to_vortex_expr(t: tuple):
    """Convert a pickle-safe filter tuple to a vortex.expr expression.

    Returns None if the filter cannot be expressed in Vortex
    (e.g. string predicates).
    """
    # Not: ("not", child_tuple)
    if t[0] == "not":
        child = _tuple_to_vortex_expr(t[1])
        return not_(child) if child is not None else None

    col_name = t[0]
    op = t[1]
    col = column(col_name)

    # Comparison: (col, "__eq__", value)
    if op.startswith("__"):
        return getattr(col, op)(t[2])

    # Null checks
    if op == "is_null":
        return col == None  # noqa: E711  (vortex uses == None for is_null)
    if op == "is_not_null":
        return not_(col == None)  # noqa: E711

    # In: (col, "in", (v1, v2, ...))
    if op == "in":
        expr = None
        for v in t[2]:
            eq = col == v
            expr = eq if expr is None else expr | eq
        return expr

    # String predicates — Vortex doesn't have native string functions,
    # so we reject these (return None) and let Sail post-filter.
    return None


# ============================================================================
# Arrow helpers
# ============================================================================


def _cast_string_views(table: pa.Table) -> pa.Table:
    """Cast Utf8View columns to Utf8 (Vortex returns string_view, Sail expects string)."""
    columns = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if field.type == pa.string_view():
            col = col.cast(pa.string())
        columns.append(col)
    if all(table.column(i) is columns[i] for i in range(len(columns))):
        return table
    return pa.table({field.name: col for field, col in zip(table.schema, columns, strict=False)})


def _read_schema(path: str) -> pa.Schema:
    """Read the Arrow schema from a Vortex file without loading all data."""
    try:
        vf = vortex.open(path)
    except Exception as e:
        msg = f"Failed to open Vortex file: {path!r}. Error: {e}"
        raise RuntimeError(msg) from e
    first = next(iter(vf.scan()), None)
    if first is None:
        msg = f"Cannot infer schema from empty Vortex file: {path!r}"
        raise ValueError(msg)
    return first.to_arrow_table().schema


# ============================================================================
# InputPartition
# ============================================================================


class VortexPartition(InputPartition):
    """A single Vortex file partition."""

    def __init__(self, path: str) -> None:
        super().__init__(0)
        self.path = path


# ============================================================================
# DataSourceReader
# ============================================================================


class VortexReader(DataSourceReader):
    """Reader for :class:`VortexDataSource`."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._filters: list[tuple] = []
        # Detect once whether we need to cast string_view columns.
        schema = _read_schema(path)
        self._needs_string_view_cast = any(f.type == pa.string_view() for f in schema)

    def pushFilters(self, filters: list[Filter]) -> Iterator[Filter]:  # noqa: N802
        accepted: list[tuple] = []
        for f in filters:
            t = _filter_to_tuple(f)
            if t is not None and _tuple_to_vortex_expr(t) is not None:
                accepted.append(t)
            else:
                yield f
        self._filters = accepted

    def partitions(self) -> list[InputPartition]:
        return [VortexPartition(self.path)]

    def read(self, partition: InputPartition) -> Iterator[pa.RecordBatch]:
        if not isinstance(partition, VortexPartition):
            msg = f"Expected VortexPartition, got {type(partition)}"
            raise TypeError(msg)

        try:
            vf = vortex.open(partition.path)
        except Exception as e:
            msg = f"Failed to open Vortex file: {partition.path!r}. Error: {e}"
            raise RuntimeError(msg) from e

        scan_expr = None
        for t in self._filters:
            expr = _tuple_to_vortex_expr(t)
            scan_expr = expr if scan_expr is None else and_(scan_expr, expr)

        for batch in vf.scan(expr=scan_expr):
            table = batch.to_arrow_table()
            if self._needs_string_view_cast:
                table = _cast_string_views(table)
            yield from table.to_batches()


# ============================================================================
# DataSource
# ============================================================================


class VortexDataSource(DataSource):
    """Vortex columnar data source backed by the ``vortex`` library.

    Register and use::

        from pysail.spark.datasource.vortex import VortexDataSource

        spark.dataSource.register(VortexDataSource)
        df = spark.read.format("vortex").option("path", "/data/file.vtx").load()

    Supported options:

    +--------+----------+------------------------------------------+
    | Option | Required | Description                              |
    +========+==========+==========================================+
    | path   | Yes      | Path to the Vortex file (.vtx)           |
    +--------+----------+------------------------------------------+

    Supported filter pushdown: EqualTo, GreaterThan, GreaterThanOrEqual,
    LessThan, LessThanOrEqual, IsNull, IsNotNull, In, Not.
    String predicates (StartsWith, EndsWith, Contains) are rejected and
    applied by Sail post-read.
    """

    @classmethod
    def name(cls) -> str:
        return "vortex"

    def _validate_options(self) -> str:
        """Validate options and return the file path."""
        path = self.options.get("path")
        if not path:
            msg = "Option 'path' is required for the vortex data source"
            raise ValueError(msg)
        return path

    def schema(self) -> pa.Schema:
        path = self._validate_options()
        return _read_schema(path)

    def reader(self, schema: pa.Schema) -> VortexReader:  # noqa: ARG002
        path = self._validate_options()
        return VortexReader(path)
