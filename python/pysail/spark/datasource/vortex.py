"""Vortex data source for Sail, backed by the ``vortex-data`` Python library.

Supports ``spark.read.format("vortex")`` with filter pushdown and
zero-copy Arrow RecordBatch yield.

Install the optional dependency before use::

    pip install pysail[vortex]

Usage::

    from pysail.spark.datasource.vortex import VortexDataSource

    spark.dataSource.register(VortexDataSource)
    df = spark.read.format("vortex").option("path", "/data/file.vortex").load()
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

try:
    import vortex
    from vortex.expr import and_, column, not_
except ImportError as e:
    msg = "vortex-data is required for the Vortex data source. Install it with: pip install pysail[vortex]"
    raise ImportError(msg) from e

try:
    from pyspark.sql.datasource import (
        DataSource,
        DataSourceReader,
        EqualTo,
        GreaterThan,
        GreaterThanOrEqual,
        In,
        InputPartition,
        LessThan,
        LessThanOrEqual,
        Not,
        StringContains,
        StringEndsWith,
        StringStartsWith,
    )
except ImportError as e:
    msg = (
        "The Vortex data source requires PySpark 4.1+ with filter pushdown support "
        "(pyspark.sql.datasource). Please upgrade PySpark or "
        "disable this data source."
    )
    raise ImportError(msg) from e

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


def _col_name(f: Filter) -> str | None:
    """Extract the column name from a PySpark Filter.

    Returns None for nested columns (multi-part attributes) since Vortex
    does not support nested column pushdown.
    """
    attr = f.attribute
    if isinstance(attr, tuple):
        if len(attr) != 1:
            return None  # Reject nested columns
        return attr[0]
    return attr


def _filter_to_tuple(f: Filter) -> tuple | None:
    """Convert a PySpark Filter to a pickle-safe tuple.

    Returns None if the filter type is not supported.
    Tuple formats:
      - Comparison: (col, op, value)     e.g. ("id", "__gt__", 5)
      - In:         (col, "in", (v1, v2, ...))
      - Not:        ("not", child_tuple)
      - String:     (col, "starts_with"|"ends_with"|"contains", value)
    """
    op = _COMPARISON_OPS.get(type(f))
    if op is not None:
        col = _col_name(f)
        return (col, op, f.value) if col is not None else None

    if isinstance(f, In):
        col = _col_name(f)
        return (col, "in", f.value) if col is not None else None
    if isinstance(f, Not):
        child = _filter_to_tuple(f.child)
        return ("not", child) if child is not None else None
    if isinstance(f, StringStartsWith):
        col = _col_name(f)
        return (col, "starts_with", f.value) if col is not None else None
    if isinstance(f, StringEndsWith):
        col = _col_name(f)
        return (col, "ends_with", f.value) if col is not None else None
    if isinstance(f, StringContains):
        col = _col_name(f)
        return (col, "contains", f.value) if col is not None else None
    return None


def _tuple_to_vortex_expr(t: tuple) -> object | None:
    """Convert a pickle-safe filter tuple to a vortex.expr expression.

    Returns None if the filter cannot be expressed in Vortex
    (e.g. string predicates or empty ``In`` lists).
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

    # In: (col, "in", (v1, v2, ...))
    if op == "in":
        values = t[2]
        if not values:
            return None  # Empty In list — let Sail post-filter
        expr = None
        for v in values:
            eq = col == v
            expr = eq if expr is None else expr | eq
        return expr

    # String predicates — Vortex doesn't have native string functions,
    # so we reject these (return None) and let Sail post-filter.
    return None


# ============================================================================
# Arrow helpers
# ============================================================================


def _normalize_schema(schema: pa.Schema) -> pa.Schema:
    """Normalize Arrow schema types for Sail compatibility.

    Vortex may report Utf8View in the schema but produce Utf8/BinaryView
    as Binary in batches. Normalize view types to their non-view equivalents
    to avoid schema mismatch errors. Uses ``field.with_type()`` to preserve
    field-level metadata.

    .. todo:: Normalize recursively for struct fields.
    """
    fields = []
    for field in schema:
        if field.type == pa.string_view():
            fields.append(field.with_type(pa.utf8()))
        elif field.type == pa.binary_view():
            fields.append(field.with_type(pa.binary()))
        else:
            fields.append(field)
    return pa.schema(fields, metadata=schema.metadata)


def _cast_view_types(table: pa.Table) -> pa.Table:
    """Cast view types (Utf8View, BinaryView) to their non-view equivalents."""
    needs_cast = False
    for field in table.schema:
        if field.type in (pa.string_view(), pa.binary_view()):
            needs_cast = True
            break
    if not needs_cast:
        return table
    target = _normalize_schema(table.schema)
    return table.cast(target)


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
    return _normalize_schema(first.to_arrow_table().schema)


# ============================================================================
# InputPartition
# ============================================================================


class VortexPartition(InputPartition):
    """A single Vortex file partition."""

    def __init__(self, path: str) -> None:
        super().__init__(0)  # partition index; single-file source always uses 0
        self.path = path


# ============================================================================
# DataSourceReader
# ============================================================================


class VortexReader(DataSourceReader):
    """Reader for :class:`VortexDataSource`."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._filters: list[tuple] = []

    def pushFilters(self, filters: list[Filter]) -> Iterator[Filter]:  # noqa: N802
        accepted: list[tuple] = []
        rejected: list[Filter] = []
        for f in filters:
            t = _filter_to_tuple(f)
            if t is not None and _tuple_to_vortex_expr(t) is not None:
                accepted.append(t)
            else:
                rejected.append(f)
        self._filters = accepted
        return iter(rejected)

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
            if expr is None:
                continue  # Skip filters that couldn't be converted
            scan_expr = expr if scan_expr is None else and_(scan_expr, expr)

        for batch in vf.scan(expr=scan_expr):
            table = _cast_view_types(batch.to_arrow_table())
            yield from table.to_batches()


# ============================================================================
# DataSource
# ============================================================================


class VortexDataSource(DataSource):
    """Vortex columnar data source backed by the ``vortex-data`` library.

    Register and use::

        from pysail.spark.datasource.vortex import VortexDataSource

        spark.dataSource.register(VortexDataSource)
        df = spark.read.format("vortex").option("path", "/data/file.vortex").load()

    Supported options:

    +--------+----------+------------------------------------------+
    | Option | Required | Description                              |
    +========+==========+==========================================+
    | path   | Yes      | Path to the Vortex file (.vortex)        |
    +--------+----------+------------------------------------------+

    Supported filter pushdown: EqualTo, GreaterThan, GreaterThanOrEqual,
    LessThan, LessThanOrEqual, In, Not.
    String predicates (StartsWith, EndsWith, Contains) and null predicates
    (IsNull, IsNotNull) are rejected and applied by Sail post-read.
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
