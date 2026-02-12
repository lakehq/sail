"""
Vortex Python DataSource for Sail (PySpark DataSource API v2).

Supports:
  - Filter pushdown via pushFilters() -> vortex scan(expr=...)
  - RecordBatch yield (columnar, zero-copy) instead of row-by-row tuples

Usage:
    spark.dataSource.register(VortexDataSource)
    df = spark.read.format("vortex").option("path", "/data/file.vtx").load()
"""

import pyarrow as pa
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

import vortex

# Arrow type -> Spark DDL mapping
ARROW_TO_SPARK = {
    "int8": "TINYINT",
    "int16": "SMALLINT",
    "int32": "INT",
    "int64": "BIGINT",
    "uint8": "TINYINT",
    "uint16": "SMALLINT",
    "uint32": "INT",
    "uint64": "BIGINT",
    "float": "FLOAT",
    "float16": "FLOAT",
    "float32": "FLOAT",
    "double": "DOUBLE",
    "float64": "DOUBLE",
    "bool": "BOOLEAN",
    "string": "STRING",
    "large_string": "STRING",
    "string_view": "STRING",
    "utf8": "STRING",
    "large_utf8": "STRING",
    "binary": "BINARY",
    "date32": "DATE",
}

# PySpark comparison filters -> Python dunder method name
COMPARISON_OPS = {
    EqualTo: "__eq__",
    GreaterThan: "__gt__",
    GreaterThanOrEqual: "__ge__",
    LessThan: "__lt__",
    LessThanOrEqual: "__le__",
}


def arrow_to_spark_ddl(arrow_schema):
    """Convert an Arrow schema to Spark DDL string."""
    fields = []
    for field in arrow_schema:
        spark_type = ARROW_TO_SPARK.get(str(field.type), "STRING")
        fields.append(f"{field.name} {spark_type}")
    return ", ".join(fields)


def filter_to_tuple(f):
    """Convert a PySpark Filter to a pickle-safe tuple.

    Returns None if the filter type is not supported by Vortex.
    Tuple formats:
      - Comparison: (col, op, value)     e.g. ("id", "__gt__", 5)
      - IsNull:     (col, "is_null")
      - IsNotNull:  (col, "is_not_null")
      - In:         (col, "in", (v1, v2, ...))
      - Not:        ("not", child_tuple)
      - StringStartsWith/EndsWith/Contains: (col, "starts_with"|"ends_with"|"contains", value)
    """

    def _col_name(f):
        return f.attribute[0] if isinstance(f.attribute, tuple) else f.attribute

    # Comparison: EqualTo, GreaterThan, etc.
    op = COMPARISON_OPS.get(type(f))
    if op is not None:
        return (_col_name(f), op, f.value)

    if isinstance(f, IsNull):
        return (_col_name(f), "is_null")
    if isinstance(f, IsNotNull):
        return (_col_name(f), "is_not_null")
    if isinstance(f, In):
        return (_col_name(f), "in", f.value)
    if isinstance(f, Not):
        child = filter_to_tuple(f.child)
        if child is not None:
            return ("not", child)
        return None
    if isinstance(f, StringStartsWith):
        return (_col_name(f), "starts_with", f.value)
    if isinstance(f, StringEndsWith):
        return (_col_name(f), "ends_with", f.value)
    if isinstance(f, StringContains):
        return (_col_name(f), "contains", f.value)
    return None


def tuple_to_vortex_expr(t):
    """Convert a pickle-safe filter tuple to a vortex.expr expression."""
    from vortex.expr import column, not_

    # Not: ("not", child_tuple)
    if t[0] == "not":
        return not_(tuple_to_vortex_expr(t[1]))

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


def cast_string_views(table):
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


class VortexPartition(InputPartition):
    def __init__(self, path):
        super().__init__(0)
        self.path = path


class VortexReader(DataSourceReader):
    def __init__(self, path):
        self.path = path
        self._filters = []  # pickle-safe filter tuples

    def pushFilters(self, filters):  # noqa: N802
        for f in filters:
            t = filter_to_tuple(f)
            if t is not None:
                vexpr = tuple_to_vortex_expr(t)
                if vexpr is not None:
                    self._filters.append(t)
                else:
                    yield f  # Supported format but no vortex equivalent (e.g. string ops)
            else:
                yield f  # Unsupported filter type

    def partitions(self):
        return [VortexPartition(self.path)]

    def read(self, partition):
        from vortex.expr import and_

        vf = vortex.open(partition.path)
        scan_expr = None
        for t in self._filters:
            expr = tuple_to_vortex_expr(t)
            scan_expr = expr if scan_expr is None else and_(scan_expr, expr)

        for batch in vf.scan(expr=scan_expr):
            table = cast_string_views(batch.to_arrow_table())
            yield from table.to_batches()


class VortexDataSource(DataSource):
    @classmethod
    def name(cls):
        return "vortex"

    def schema(self):
        vf = vortex.open(self.options["path"])
        arrow_schema = next(iter(vf.scan())).to_arrow_table().schema
        return arrow_to_spark_ddl(arrow_schema)

    def reader(self, schema):  # noqa: ARG002
        return VortexReader(self.options["path"])
