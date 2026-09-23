import pytest
from pyspark.sql.types import ArrayType, IntegerType, MapType, StructField, StructType

from pysail.testing.spark.utils.common import is_jvm_spark

# The nullability a set operation reports for a column, at every level it nests, for each operation
# and each pair of inputs where the level under test can be NULL on neither side, on one or on both.
# Spark derives it per operation: a union can hold a NULL wherever either input can, at every level
# (`Union.mergeChildOutputs`, `StructType.unionLikeMerge`); an intersection only where both inputs
# can, and only at the top level (`Intersect.mergeChildOutputs`); a difference keeps the first input
# (`Except.mergeChildOutputs`). The inputs are built from an explicit schema, so that both engines
# start from the same nullability. Every expected value was measured on the Spark JVM.
#
# A column is written as `n`/`N` for NOT NULL/nullable, followed by `[..]` for the element of an
# array, `{..}` for the value of a map and `(x:..)` for the fields of a struct, and the number of
# rows the operation returns.


def _inner(nullable):
    return StructType([StructField("x", IntegerType(), nullable)])


# Each shape gives the type of column `a`, and whether it can be NULL, from a flag that says whether
# the level under test can be NULL, and a value that fits both.
_SHAPES = {
    "int": (lambda n: (IntegerType(), n), 1),
    "array": (lambda n: (ArrayType(IntegerType(), n), False), [1]),
    "map": (lambda n: (MapType(IntegerType(), IntegerType(), n), False), {1: 1}),
    "struct field": (lambda n: (_inner(n), False), (1,)),
    "struct": (lambda n: (_inner(False), n), (1,)),
    "array of struct field": (lambda n: (ArrayType(_inner(n), False), False), [(1,)]),
}


def _frame(spark, column, nullable):
    build, value = _SHAPES[column]
    data_type, top = build(nullable)
    schema = StructType([StructField("a", data_type, top), StructField("k", IntegerType(), False)])
    return spark.createDataFrame([(value, 1)], schema)


def _flag(nullable):
    return "N" if nullable else "n"


def _shape(data_type):
    if isinstance(data_type, ArrayType):
        return f"[{_flag(data_type.containsNull)}{_shape(data_type.elementType)}]"
    if isinstance(data_type, MapType):
        return f"{{{_flag(data_type.valueContainsNull)}{_shape(data_type.valueType)}}}"
    if isinstance(data_type, StructType):
        return "(" + ",".join(f"{f.name}:{_flag(f.nullable)}{_shape(f.dataType)}" for f in data_type.fields) + ")"
    return ""


def _outcome(spark, op, column, sides):
    left, right = (_frame(spark, column, side == "N") for side in sides)
    try:
        df = getattr(left, op)(right)
        df._cached_schema = None  # noqa: SLF001
        field = df.schema.fields[0]
        return f"{_flag(field.nullable)}{_shape(field.dataType)} rows={len(df.collect())}"
    except Exception as e:  # noqa: BLE001
        text = str(e)
        start = text.find("[")
        return "ERR " + text[start + 1 : text.find("]", start)]


_EXPECTED = {
    ("exceptAll", "array of struct field", "NN"): "n[n(x:N)] rows=0",
    ("exceptAll", "array of struct field", "Nn"): "n[n(x:N)] rows=0",
    ("exceptAll", "array of struct field", "nN"): "n[n(x:n)] rows=0",
    ("exceptAll", "array of struct field", "nn"): "n[n(x:n)] rows=0",
    ("exceptAll", "array", "NN"): "n[N] rows=0",
    ("exceptAll", "array", "Nn"): "n[N] rows=0",
    ("exceptAll", "array", "nN"): "n[n] rows=0",
    ("exceptAll", "array", "nn"): "n[n] rows=0",
    ("exceptAll", "int", "NN"): "N rows=0",
    ("exceptAll", "int", "Nn"): "N rows=0",
    ("exceptAll", "int", "nN"): "n rows=0",
    ("exceptAll", "int", "nn"): "n rows=0",
    ("exceptAll", "map", "NN"): "ERR UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
    ("exceptAll", "map", "Nn"): "ERR UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
    ("exceptAll", "map", "nN"): "ERR UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
    ("exceptAll", "map", "nn"): "ERR UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
    ("exceptAll", "struct field", "NN"): "n(x:N) rows=0",
    ("exceptAll", "struct field", "Nn"): "n(x:N) rows=0",
    ("exceptAll", "struct field", "nN"): "n(x:n) rows=0",
    ("exceptAll", "struct field", "nn"): "n(x:n) rows=0",
    ("exceptAll", "struct", "NN"): "N(x:n) rows=0",
    ("exceptAll", "struct", "Nn"): "N(x:n) rows=0",
    ("exceptAll", "struct", "nN"): "n(x:n) rows=0",
    ("exceptAll", "struct", "nn"): "n(x:n) rows=0",
    ("intersectAll", "array of struct field", "NN"): "n[n(x:N)] rows=1",
    ("intersectAll", "array of struct field", "Nn"): "n[n(x:N)] rows=1",
    ("intersectAll", "array of struct field", "nN"): "n[n(x:n)] rows=1",
    ("intersectAll", "array of struct field", "nn"): "n[n(x:n)] rows=1",
    ("intersectAll", "array", "NN"): "n[N] rows=1",
    ("intersectAll", "array", "Nn"): "n[N] rows=1",
    ("intersectAll", "array", "nN"): "n[n] rows=1",
    ("intersectAll", "array", "nn"): "n[n] rows=1",
    ("intersectAll", "int", "NN"): "N rows=1",
    ("intersectAll", "int", "Nn"): "n rows=1",
    ("intersectAll", "int", "nN"): "n rows=1",
    ("intersectAll", "int", "nn"): "n rows=1",
    ("intersectAll", "map", "NN"): "ERR UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
    ("intersectAll", "map", "Nn"): "ERR UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
    ("intersectAll", "map", "nN"): "ERR UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
    ("intersectAll", "map", "nn"): "ERR UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
    ("intersectAll", "struct field", "NN"): "n(x:N) rows=1",
    ("intersectAll", "struct field", "Nn"): "n(x:N) rows=1",
    ("intersectAll", "struct field", "nN"): "n(x:n) rows=1",
    ("intersectAll", "struct field", "nn"): "n(x:n) rows=1",
    ("intersectAll", "struct", "NN"): "N(x:n) rows=1",
    ("intersectAll", "struct", "Nn"): "n(x:n) rows=1",
    ("intersectAll", "struct", "nN"): "n(x:n) rows=1",
    ("intersectAll", "struct", "nn"): "n(x:n) rows=1",
    ("intersect", "array of struct field", "NN"): "n[n(x:N)] rows=1",
    ("intersect", "array of struct field", "Nn"): "n[n(x:N)] rows=1",
    ("intersect", "array of struct field", "nN"): "n[n(x:n)] rows=1",
    ("intersect", "array of struct field", "nn"): "n[n(x:n)] rows=1",
    ("intersect", "array", "NN"): "n[N] rows=1",
    ("intersect", "array", "Nn"): "n[N] rows=1",
    ("intersect", "array", "nN"): "n[n] rows=1",
    ("intersect", "array", "nn"): "n[n] rows=1",
    ("intersect", "int", "NN"): "N rows=1",
    ("intersect", "int", "Nn"): "n rows=1",
    ("intersect", "int", "nN"): "n rows=1",
    ("intersect", "int", "nn"): "n rows=1",
    ("intersect", "map", "NN"): "ERR UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
    ("intersect", "map", "Nn"): "ERR UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
    ("intersect", "map", "nN"): "ERR UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
    ("intersect", "map", "nn"): "ERR UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
    ("intersect", "struct field", "NN"): "n(x:N) rows=1",
    ("intersect", "struct field", "Nn"): "n(x:N) rows=1",
    ("intersect", "struct field", "nN"): "n(x:n) rows=1",
    ("intersect", "struct field", "nn"): "n(x:n) rows=1",
    ("intersect", "struct", "NN"): "N(x:n) rows=1",
    ("intersect", "struct", "Nn"): "n(x:n) rows=1",
    ("intersect", "struct", "nN"): "n(x:n) rows=1",
    ("intersect", "struct", "nn"): "n(x:n) rows=1",
    ("subtract", "array of struct field", "NN"): "n[n(x:N)] rows=0",
    ("subtract", "array of struct field", "Nn"): "n[n(x:N)] rows=0",
    ("subtract", "array of struct field", "nN"): "n[n(x:n)] rows=0",
    ("subtract", "array of struct field", "nn"): "n[n(x:n)] rows=0",
    ("subtract", "array", "NN"): "n[N] rows=0",
    ("subtract", "array", "Nn"): "n[N] rows=0",
    ("subtract", "array", "nN"): "n[n] rows=0",
    ("subtract", "array", "nn"): "n[n] rows=0",
    ("subtract", "int", "NN"): "N rows=0",
    ("subtract", "int", "Nn"): "N rows=0",
    ("subtract", "int", "nN"): "n rows=0",
    ("subtract", "int", "nn"): "n rows=0",
    ("subtract", "map", "NN"): "ERR UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
    ("subtract", "map", "Nn"): "ERR UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
    ("subtract", "map", "nN"): "ERR UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
    ("subtract", "map", "nn"): "ERR UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
    ("subtract", "struct field", "NN"): "n(x:N) rows=0",
    ("subtract", "struct field", "Nn"): "n(x:N) rows=0",
    ("subtract", "struct field", "nN"): "n(x:n) rows=0",
    ("subtract", "struct field", "nn"): "n(x:n) rows=0",
    ("subtract", "struct", "NN"): "N(x:n) rows=0",
    ("subtract", "struct", "Nn"): "N(x:n) rows=0",
    ("subtract", "struct", "nN"): "n(x:n) rows=0",
    ("subtract", "struct", "nn"): "n(x:n) rows=0",
    ("unionAll", "array of struct field", "NN"): "n[n(x:N)] rows=2",
    ("unionAll", "array of struct field", "Nn"): "n[n(x:N)] rows=2",
    ("unionAll", "array of struct field", "nN"): "n[n(x:N)] rows=2",
    ("unionAll", "array of struct field", "nn"): "n[n(x:n)] rows=2",
    ("unionAll", "array", "NN"): "n[N] rows=2",
    ("unionAll", "array", "Nn"): "n[N] rows=2",
    ("unionAll", "array", "nN"): "n[N] rows=2",
    ("unionAll", "array", "nn"): "n[n] rows=2",
    ("unionAll", "int", "NN"): "N rows=2",
    ("unionAll", "int", "Nn"): "N rows=2",
    ("unionAll", "int", "nN"): "N rows=2",
    ("unionAll", "int", "nn"): "n rows=2",
    ("unionAll", "map", "NN"): "n{N} rows=2",
    ("unionAll", "map", "Nn"): "n{N} rows=2",
    ("unionAll", "map", "nN"): "n{N} rows=2",
    ("unionAll", "map", "nn"): "n{n} rows=2",
    ("unionAll", "struct field", "NN"): "n(x:N) rows=2",
    ("unionAll", "struct field", "Nn"): "n(x:N) rows=2",
    ("unionAll", "struct field", "nN"): "n(x:N) rows=2",
    ("unionAll", "struct field", "nn"): "n(x:n) rows=2",
    ("unionAll", "struct", "NN"): "N(x:n) rows=2",
    ("unionAll", "struct", "Nn"): "N(x:n) rows=2",
    ("unionAll", "struct", "nN"): "N(x:n) rows=2",
    ("unionAll", "struct", "nn"): "n(x:n) rows=2",
    ("unionByName", "array of struct field", "NN"): "n[n(x:N)] rows=2",
    ("unionByName", "array of struct field", "Nn"): "n[n(x:N)] rows=2",
    ("unionByName", "array of struct field", "nN"): "n[n(x:N)] rows=2",
    ("unionByName", "array of struct field", "nn"): "n[n(x:n)] rows=2",
    ("unionByName", "array", "NN"): "n[N] rows=2",
    ("unionByName", "array", "Nn"): "n[N] rows=2",
    ("unionByName", "array", "nN"): "n[N] rows=2",
    ("unionByName", "array", "nn"): "n[n] rows=2",
    ("unionByName", "int", "NN"): "N rows=2",
    ("unionByName", "int", "Nn"): "N rows=2",
    ("unionByName", "int", "nN"): "N rows=2",
    ("unionByName", "int", "nn"): "n rows=2",
    ("unionByName", "map", "NN"): "n{N} rows=2",
    ("unionByName", "map", "Nn"): "n{N} rows=2",
    ("unionByName", "map", "nN"): "n{N} rows=2",
    ("unionByName", "map", "nn"): "n{n} rows=2",
    ("unionByName", "struct field", "NN"): "n(x:N) rows=2",
    ("unionByName", "struct field", "Nn"): "n(x:N) rows=2",
    ("unionByName", "struct field", "nN"): "n(x:N) rows=2",
    ("unionByName", "struct field", "nn"): "n(x:n) rows=2",
    ("unionByName", "struct", "NN"): "N(x:n) rows=2",
    ("unionByName", "struct", "Nn"): "N(x:n) rows=2",
    ("unionByName", "struct", "nN"): "N(x:n) rows=2",
    ("unionByName", "struct", "nn"): "n(x:n) rows=2",
    ("union", "array of struct field", "NN"): "n[n(x:N)] rows=2",
    ("union", "array of struct field", "Nn"): "n[n(x:N)] rows=2",
    ("union", "array of struct field", "nN"): "n[n(x:N)] rows=2",
    ("union", "array of struct field", "nn"): "n[n(x:n)] rows=2",
    ("union", "array", "NN"): "n[N] rows=2",
    ("union", "array", "Nn"): "n[N] rows=2",
    ("union", "array", "nN"): "n[N] rows=2",
    ("union", "array", "nn"): "n[n] rows=2",
    ("union", "int", "NN"): "N rows=2",
    ("union", "int", "Nn"): "N rows=2",
    ("union", "int", "nN"): "N rows=2",
    ("union", "int", "nn"): "n rows=2",
    ("union", "map", "NN"): "n{N} rows=2",
    ("union", "map", "Nn"): "n{N} rows=2",
    ("union", "map", "nN"): "n{N} rows=2",
    ("union", "map", "nn"): "n{n} rows=2",
    ("union", "struct field", "NN"): "n(x:N) rows=2",
    ("union", "struct field", "Nn"): "n(x:N) rows=2",
    ("union", "struct field", "nN"): "n(x:N) rows=2",
    ("union", "struct field", "nn"): "n(x:n) rows=2",
    ("union", "struct", "NN"): "N(x:n) rows=2",
    ("union", "struct", "Nn"): "N(x:n) rows=2",
    ("union", "struct", "nN"): "N(x:n) rows=2",
    ("union", "struct", "nn"): "n(x:n) rows=2",
}


# An intersection reports the first input's nullability at the top level instead of narrowing it to
# where both inputs can hold NULL. See the TODO in `set_op.rs`.
_SAIL_BUGS = {(op, column, "Nn") for op in ("intersect", "intersectAll") for column in ("int", "struct")}


def _cases():
    for key in _EXPECTED:
        marks = (
            [pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)] if key in _SAIL_BUGS else []
        )
        yield pytest.param(*key, id="-".join(key), marks=marks)


@pytest.mark.parametrize(("op", "column", "sides"), _cases())
def test_the_nullability_a_set_operation_reports(spark, op, column, sides):
    assert _outcome(spark, op, column, sides) == _EXPECTED[(op, column, sides)]
