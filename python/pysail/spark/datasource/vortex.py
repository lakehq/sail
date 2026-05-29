"""Vortex data source for Sail, backed by the ``vortex-data`` Python library.

Supports ``spark.read.format("vortex")`` with filter pushdown and
zero-copy Arrow RecordBatch yield, and ``spark.write.format("vortex")``
with append, overwrite, error, and ignore modes.

Install the optional dependency before use::

    pip install pysail[vortex]

Usage::

    from pysail.spark.datasource.vortex import VortexDataSource

    spark.dataSource.register(VortexDataSource)

    # Read a single file
    df = spark.read.format("vortex").option("path", "/data/file.vortex").load()

    # Read a directory recursively
    df = spark.read.format("vortex").option("path", "/data/").load()

    # Read with a glob pattern
    df = spark.read.format("vortex").option("path", "/data/*.vortex").load()
    df = (
        spark.read.format("vortex").option("path", "/data/**/*.vortex").load()
    )  # recursive

    # Write with error mode (default)
    df.write.format("vortex").option("path", "/data/").save()

    # Write with overwrite mode
    df.write.mode("overwrite").format("vortex").option("path", "/data/").save()
"""

from __future__ import annotations

import glob
import os
import secrets
import shutil
from dataclasses import dataclass
from pathlib import Path
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
        DataSourceArrowWriter,
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
        WriterCommitMessage,
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
# File handling helpers
# ============================================================================


def _get_vortex_files(path: str) -> list[str]:
    """Resolve a path into a sorted list of Vortex files.

    Path handling:
    - File:         Returns [path] directly if it's a Vortex file.
    - Directory:    Recursively collects all files; raises ValueError
                    if any non-Vortex file is found, otherwise returns all.
    - Glob pattern: If pattern targets Vortex (ends with ``.vortex``), expands
                    the pattern, collects matching Vortex files and returns them;
                    non-Vortex files are silently skipped.
    """
    vortex_files: list[str] = []

    if os.path.isfile(path):
        if not path.endswith(".vortex"):
            msg = f"Path is not a Vortex file: '{path}'"
            raise ValueError(msg)
        vortex_files.append(path)
    elif os.path.isdir(path):
        for p in Path(path).rglob("*"):
            # skip hidden files and files inside hidden directories
            if any(part.startswith(("_", ".")) for part in p.parts[len(Path(path).parts) :]):
                continue
            if p.is_file():
                f = str(p)
                if not f.endswith(".vortex"):
                    msg = f"Path contains non-Vortex file(s): '{path}'"
                    raise ValueError(msg)
                vortex_files.append(f)
    else:
        is_vortex_pattern = path.endswith(".vortex")
        if is_vortex_pattern:
            matched = glob.glob(path, recursive=True)
            vortex_files.extend([m for m in matched if os.path.isfile(m) and m.endswith(".vortex")])

    if not vortex_files:
        msg = f"No Vortex files found in the specified path: '{path}'"
        raise ValueError(msg)

    return sorted(set(vortex_files))


def _generate_unique_vortex_path(base_path: str) -> str:
    """Generates a unique .vortex file path."""
    while True:
        file_name = secrets.token_hex(8) + ".vortex"
        path = os.path.join(base_path, file_name)
        if not os.path.exists(path):
            return path


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
    """Read the Arrow schema from the first Vortex file without loading all data."""
    vortex_files = _get_vortex_files(path)
    vf_path = vortex_files[0]
    try:
        vf = vortex.open(vf_path)
    except Exception as e:
        msg = f"Failed to open Vortex file: {vf_path!r}. Error: {e}"
        raise RuntimeError(msg) from e
    first = next(iter(vf.scan()), None)
    if first is None:
        msg = f"Cannot infer schema from empty Vortex file: {vf_path!r}"
        raise ValueError(msg)
    return _normalize_schema(first.to_arrow_table().schema)


# ============================================================================
# InputPartition
# ============================================================================


class VortexPartition(InputPartition):
    """Partition, one per Vortex file."""

    def __init__(self, index: int, path: str) -> None:
        super().__init__(index)  # partition index
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
        vortex_files = _get_vortex_files(self.path)
        return [VortexPartition(index, vf_path) for index, vf_path in enumerate(vortex_files)]

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
# DataSourceWriter
# ============================================================================


@dataclass
class VortexCommitMessage(WriterCommitMessage):
    """Commit message returned by the VortexWriter.write"""

    staged_path: str


class VortexWriter(DataSourceArrowWriter):
    """Writer for :class:`VortexDataSource`."""

    def __init__(self, path: str, schema: pa.Schema, mode: str) -> None:
        self.path = path
        self.schema = schema
        self.mode = mode
        self._init_staging_dir()

    def _init_staging_dir(self) -> None:
        staging_name = f"._staging_{secrets.token_hex(8)}"
        self._staging_dir = os.path.join(self.path, staging_name)
        os.makedirs(self._staging_dir, exist_ok=True)

    def _remove_staging_dir(self) -> None:
        if self._staging_dir:
            shutil.rmtree(self._staging_dir, ignore_errors=True)

    def write(self, iterator: Iterator[pa.RecordBatch]) -> WriterCommitMessage:
        table = pa.Table.from_batches(iterator, self.schema)
        if table.num_rows == 0:
            return VortexCommitMessage("")

        staged_path = _generate_unique_vortex_path(self._staging_dir)
        vortex.io.write(table, staged_path)
        return VortexCommitMessage(staged_path)

    def commit(self, messages: list[WriterCommitMessage | None]) -> None:
        staged_files = [m.staged_path for m in messages if m and m.staged_path]

        try:
            if not staged_files:
                return
            if self.mode == "overwrite" and os.path.isdir(self.path):
                for p in Path(self.path).rglob("*.vortex"):
                    if p.is_file() and os.path.basename(self._staging_dir) not in p.parts:
                        p.unlink(missing_ok=True)

            os.makedirs(self.path, exist_ok=True)

            for src in staged_files:
                shutil.move(src, os.path.join(self.path, os.path.basename(src)))
        except Exception as e:
            msg = f"Failed to commit to '{self.path}': {e}"
            raise RuntimeError(msg) from e
        finally:
            self._remove_staging_dir()

    def abort(self, messages: list[WriterCommitMessage | None]) -> None:  # noqa: ARG002
        self._remove_staging_dir()


class VortexIgnoreWriter(VortexWriter):
    """Writer that no-ops when the path already exists (mode='ignore')."""

    def _init_staging_dir(self) -> None:
        self._staging_dir = ""

    def write(self, iterator: Iterator[pa.RecordBatch]) -> WriterCommitMessage:  # noqa: ARG002
        return VortexCommitMessage("")


# ============================================================================
# DataSource
# ============================================================================


class VortexDataSource(DataSource):
    """Vortex columnar data source backed by the ``vortex-data`` library.

    Register and use::

        from pysail.spark.datasource.vortex import VortexDataSource

        spark.dataSource.register(VortexDataSource)

        # Read a single file
        df = spark.read.format("vortex").option("path", "/data/file.vortex").load()

        # Read a directory recursively
        df = spark.read.format("vortex").option("path", "/data/").load()

        # Read with a glob pattern
        df = spark.read.format("vortex").option("path", "/data/*.vortex").load()
        df = (
            spark.read.format("vortex").option("path", "/data/**/*.vortex").load()
        )  # recursive

        # Write with error mode (default)
        df.write.format("vortex").option("path", "/data/").save()

        # Write with overwrite mode
        df.write.mode("overwrite").format("vortex").option("path", "/data/").save()

    Read Mode
    -------
    Path handling:

    - **File**: Must be a Vortex file. Raises ``ValueError`` otherwise.
    - **Directory**: Recursively collects all files beneath the directory.
      Raises ``ValueError`` if any non-Vortex file is encountered.
    - **Glob pattern**: Only patterns ending with ``.vortex`` are expanded;
    non-Vortex matches are silently skipped.

    Supported Options:

    +--------+----------+------------------------------------------+
    | Option | Required | Description                              |
    +========+==========+==========================================+
    | path   | Yes      | Path to read: file, directory, or        |
    |        |          | glob pattern ending with ``.vortex``     |
    +--------+----------+------------------------------------------+

    Supported filter pushdown: EqualTo, GreaterThan, GreaterThanOrEqual,
    LessThan, LessThanOrEqual, In, Not.
    String predicates (StartsWith, EndsWith, Contains) and null predicates
    (IsNull, IsNotNull) are rejected and applied by Sail post-read.

    Write Mode
    ----------
    Writes one ``.vortex`` file per Spark partition into the target directory.
    Uses a two-phase commit: partitions write to a staging directory first,
    the driver moves files to the final path only after all partitions succeed.

    Supported Write Options:

    +--------+----------+-----------------------------------------------+
    | Option | Required | Description                                   |
    +========+==========+===============================================+
    | path   | Yes      | Target directory to write ``.vortex`` files   |
    +--------+----------+-----------------------------------------------+
    | mode   | No       | Write mode (see below). Defaults to           |
    |        |          | ``error`` if not set.                        |
    +--------+----------+-----------------------------------------------+

    Supported write modes:

    - **append**: Adds new ``.vortex`` files to the target directory.
    - **overwrite**: Deletes existing ``.vortex`` files in the target
      directory before writing new ones.
    - **error**: Raises ``RuntimeError`` if the target directory already exists.
    - **ignore**: Skips the write entirely if the target directory already exists.
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

    def writer(self, schema: pa.Schema, overwrite: bool) -> VortexWriter:  # noqa: FBT001
        path = self._validate_options()
        mode = self.options.get("mode") or ("overwrite" if overwrite else "error")

        path_exists = os.path.isdir(path)
        if mode == "error" and path_exists:
            msg = f"Path '{path}' already exists. Use mode='overwrite' to replace or mode='append' to add to it."
            raise RuntimeError(msg)
        if mode == "ignore" and path_exists:
            return VortexIgnoreWriter(path, schema, mode)

        return VortexWriter(path, schema, mode)
