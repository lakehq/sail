import json
import re

import pytest
from pyspark.sql import functions as sf
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StructField,
    StructType,
)

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

# The values were measured on Spark 4.2, and the coercion rules and messages of set operations
# differ across minor versions; `COLLATE` and VARIANT need Spark 4 as well.
pytestmark = pytest.mark.skipif(
    pyspark_version() < (4, 2),
    reason="expected values measured on Spark 4.2",
)

# Every leaf of the branch map of Spark 4.2.0 set operations (UNION / UNION ALL / unionByName with
# and without allowMissingColumns / INTERSECT [ALL] / EXCEPT [ALL] / subtract / exceptAll), one row
# per leaf. The rule underneath is spread over `WidenSetOperationTypes` (TypeCoercionBase.scala),
# `findTightestCommonType` / `findWiderTypeForTwo` (TypeCoercion.scala, AnsiTypeCoercion.scala),
# `findTypeForComplex` / `findWiderTypeForDecimal` / `findWiderDateTimeType`
# (TypeCoercionHelper.scala), `ResolveUnion`, the `Union | SetOperation` case of `CheckAnalysis`
# with `TypeCoercionValidation.getDataTypesAreCompatibleFn` / `getHintForOperatorCoercion`, the
# map / variant rejection of `CheckAnalysis`, and the output of `Union.mergeChildOutputs`
# (`StructType.unionLikeMerge`), `Intersect.mergeChildOutputs` and `Except.mergeChildOutputs`.
# Every expected value below was measured on the Spark JVM over Spark Connect in UTC, and is
# frozen here; nothing is derived. Each leaf is `(id, input, ansi, caseSensitive, expected)`.
#
# The input is one of a few shapes: two `SELECT` lists combined by a DataFrame method or a SQL
# operator, a chain of three unions, a pair with metadata on the column, a struct whose nested
# field carries metadata, columns read back from Parquet, VectorUDT frames, or frames built from
# an explicit schema. The expected value is either the output schema (type, nullability at every
# level, metadata) with the sorted rows, or the error class with its message.
#
# Leaves the JVM cannot answer over Spark Connect are left out (TIME values, and a TIME(3) vs
# TIME(6) union, since the JVM refuses to ship a TIME column): see the branch map.
#
# TODO: the leaves still marked as Sail bugs, each group with one cause outside the set operation,
#   where it is explained:
#   - B23 B25 B27 C-*-dt / C-*-ym F01 F03: an interval type loses its fields in Arrow, so it
#     renders as `INTERVAL DAY TO SECOND` where Spark writes `INTERVAL DAY`.
#   - J23-J29: a column read from Parquet keeps the nullability the file declares, where Spark reads
#     every column as nullable (`listing/source.rs`).
#   - B15 B36: a cast is not declared nullable where it can turn a value into NULL (`cast.rs`).
#   - C-abc-*: a string that is not a number fails with DataFusion's message rather than
#     `CAST_INVALID_INPUT` (`cast.rs`).
#   - J13 J33b: an intersection is not narrowed to where both inputs can hold NULL (`set_op.rs`).
#   - H17: a struct inside an array is not rebuilt by name for `unionByName` (`set_op.rs`).
#   - K01-K04: Sail does not parse `COLLATE`.
#   The intersection and the difference of a string and a day-time interval with ANSI mode are not
#   leaves at all: Spark fails there with `INTERNAL_ERROR` ("Found the unresolved operator"), which
#   is not a behavior to match.
#   - B28: Spark refuses the TIME type by default.

_CLASS = re.compile(r"\[([A-Z][A-Z0-9_]*(?:\.[A-Z0-9_]+)*)\]")

_SQL_OPS = {"UNION ALL", "UNION", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL", "MINUS"}


def _apply(op, left, right):
    return {
        "union": lambda: left.union(right),
        "unionByName": lambda: left.unionByName(right),
        "unionByNameMissing": lambda: left.unionByName(right, allowMissingColumns=True),
        "intersect": lambda: left.intersect(right),
        "intersectAll": lambda: left.intersectAll(right),
        "subtract": lambda: left.subtract(right),
        "exceptAll": lambda: left.exceptAll(right),
        "union.distinct": lambda: left.union(right).distinct(),
    }[op]()


def _two(spark, _tmp, lsel, rsel, op="union"):
    if op in _SQL_OPS:
        return spark.sql(f"SELECT {lsel} {op} SELECT {rsel}")
    return _apply(op, spark.sql(f"SELECT {lsel}"), spark.sql(f"SELECT {rsel}"))


def _chain(spark, _tmp, sels, op="union"):
    df = spark.sql(f"SELECT {sels[0]}")
    for x in sels[1:]:
        df = _apply(op, df, spark.sql(f"SELECT {x}"))
    return df


def _meta_pair(spark, _tmp, lsel, lmeta, rsel, rmeta, op="union"):
    left, right = spark.sql(f"SELECT {lsel}"), spark.sql(f"SELECT {rsel}")
    if lmeta is not None:
        left = left.select(sf.col("a").alias("a", metadata=lmeta))
    if rmeta is not None:
        right = right.select(sf.col("a").alias("a", metadata=rmeta))
    return _apply(op, left, right)


def _meta_chain(spark, _tmp, sels, meta):
    dfs = [spark.sql(f"SELECT {x}") for x in sels]
    df = dfs[0].select(sf.col("a").alias("a", metadata=meta))
    for d in dfs[1:]:
        df = df.union(d)
    return df


def _nested_meta(spark, _tmp, left_inner, right_inner, op="union", *, wrap_array=False):
    def frame(inner, meta, value):
        st = StructType([StructField("x", inner, True, meta)])
        dt = ArrayType(st, True) if wrap_array else st
        v = [(value,)] if wrap_array else (value,)
        return spark.createDataFrame([(v,)], StructType([StructField("s", dt, True)]))

    return _apply(op, frame(left_inner, {"k": "L"}, 1), frame(right_inner, {"k": "R"}, 2))


def _parquet(spark, tmp):
    path = str(tmp / "pq")
    spark.sql(
        "SELECT X'6162' AS b, 'ab' AS s, TIMESTAMP'2024-01-01 00:00:00' AS ts, "
        "TIMESTAMP_NTZ'2024-01-01 00:00:00' AS ntz, DATE'2024-01-01' AS d, "
        "array('x') AS arr, 1 AS i"
    ).write.mode("overwrite").parquet(path)
    return spark.read.parquet(path)


def _pq_pair(spark, tmp, lcols, rsel, op, *, pq_right=False):
    pq = _parquet(spark, tmp)
    right = pq.selectExpr(*rsel) if pq_right else spark.sql(f"SELECT {rsel}")
    return _apply(op, pq.selectExpr(*lcols), right)


def _vector_pair(spark, _tmp, right_kind, op):
    from pyspark.ml.linalg import Vectors

    left = spark.createDataFrame([(Vectors.dense([1.0, 2.0]),)], ["v"])
    if right_kind == "vector":
        right = spark.createDataFrame([(Vectors.dense([1.0, 2.0]),), (Vectors.sparse(2, [0], [3.0]),)], ["v"])
    else:
        right = spark.sql("SELECT 1 AS v")
    return _apply(op, left, right)


def _schema_pair(spark, _tmp, lschema, ldata, rschema, rdata, op):
    return _apply(op, spark.createDataFrame(ldata, lschema), spark.createDataFrame(rdata, rschema))


_BUILDERS = {
    "two": _two,
    "chain": _chain,
    "meta_pair": _meta_pair,
    "meta_chain": _meta_chain,
    "nested_meta": _nested_meta,
    "pq_pair": _pq_pair,
    "vector_pair": _vector_pair,
    "schema_pair": _schema_pair,
}


def _message(e):
    text = str(e)
    m = _CLASS.search(text)
    body = text[m.start() :] if m else text
    body = body.split(" SQLSTATE")[0].split("\n\nJVM stacktrace")[0]
    return (m.group(1) if m else type(e).__name__), re.sub(r"\s+", " ", body.strip()).rstrip(".")


def _build_and_collect(spark, tmp, name, args, kwargs):
    df = _BUILDERS[name](spark, tmp, *args, **kwargs)
    _ = df.schema
    return df.collect()


def _bug(*values):
    return pytest.param(*values, marks=pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True))


# Left out as UNREACHABLE: B29-time3-time6, C-str-time/ansi
_MATRIX = [
    (
        "A01-union-2v3",
        ("two", ("1 a, 2 b", "1 a, 2 b, 3 c", "union"), {}),
        False,
        False,
        {
            "error": "NUM_COLUMNS_MISMATCH",
            "message": "[NUM_COLUMNS_MISMATCH] UNION can only be performed on inputs with the same number of columns, but the first input has 2 columns and the second input has 3 columns",
        },
    ),
    (
        "A02-intersect-3v2",
        ("two", ("1 a, 2 b, 3 c", "1 a, 2 b", "intersect"), {}),
        False,
        False,
        {
            "error": "NUM_COLUMNS_MISMATCH",
            "message": "[NUM_COLUMNS_MISMATCH] INTERSECT can only be performed on inputs with the same number of columns, but the first input has 3 columns and the second input has 2 columns",
        },
    ),
    (
        "A03-exceptAll-1v2",
        ("two", ("1 a", "1 a, 2 b", "exceptAll"), {}),
        False,
        False,
        {
            "error": "NUM_COLUMNS_MISMATCH",
            "message": "[NUM_COLUMNS_MISMATCH] EXCEPT ALL can only be performed on inputs with the same number of columns, but the first input has 1 columns and the second input has 2 columns",
        },
    ),
    (
        "A04-intersectAll-2v1",
        ("two", ("1 a, 2 b", "1 a", "intersectAll"), {}),
        False,
        False,
        {
            "error": "NUM_COLUMNS_MISMATCH",
            "message": "[NUM_COLUMNS_MISMATCH] INTERSECT ALL can only be performed on inputs with the same number of columns, but the first input has 2 columns and the second input has 1 columns",
        },
    ),
    (
        "A05-subtract-1v2",
        ("two", ("1 a", "1 a, 2 b", "subtract"), {}),
        False,
        False,
        {
            "error": "NUM_COLUMNS_MISMATCH",
            "message": "[NUM_COLUMNS_MISMATCH] EXCEPT can only be performed on inputs with the same number of columns, but the first input has 1 columns and the second input has 2 columns",
        },
    ),
    (
        "A06-sql-union-distinct-1v2",
        ("two", ("1 a", "1 a, 2 b", "UNION"), {}),
        False,
        False,
        {
            "error": "NUM_COLUMNS_MISMATCH",
            "message": "[NUM_COLUMNS_MISMATCH] UNION can only be performed on inputs with the same number of columns, but the first input has 1 columns and the second input has 2 columns",
        },
    ),
    (
        "A07-sql-minus-1v2",
        ("two", ("1 a", "1 a, 2 b", "MINUS"), {}),
        False,
        False,
        {
            "error": "NUM_COLUMNS_MISMATCH",
            "message": "[NUM_COLUMNS_MISMATCH] EXCEPT can only be performed on inputs with the same number of columns, but the first input has 1 columns and the second input has 2 columns",
        },
    ),
    (
        "A08-byName-right-extra",
        ("two", ("1 a", "1 a, 2 b", "unionByName"), {}),
        False,
        False,
        {
            "error": "NUM_COLUMNS_MISMATCH",
            "message": "[NUM_COLUMNS_MISMATCH] UNION can only be performed on inputs with the same number of columns, but the first input has 1 columns and the second input has 2 columns",
        },
    ),
    (
        "A09-chain3-third-count",
        ("chain", (["1 a", "2 a", "3 a, 4 b"],), {}),
        False,
        False,
        {
            "error": "NUM_COLUMNS_MISMATCH",
            "message": "[NUM_COLUMNS_MISMATCH] UNION can only be performed on inputs with the same number of columns, but the first input has 1 columns and the second input has 2 columns",
        },
    ),
    (
        "A10-byNameMissing-appends",
        ("two", ("1 a, 2 b", "3 c, 4 a", "unionByNameMissing"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}, {"metadata": {}, "name": "b", "nullable": true, "type": "integer"}, {"metadata": {}, "name": "c", "nullable": true, "type": "integer"}]',
            "rows": ["(1, 2, None)", "(4, None, 3)"],
        },
    ),
    (
        "B01-int-int",
        ("two", ("1 a", "2 a"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}]', "rows": ["(1,)", "(2,)"]},
    ),
    (
        "B02-null-int",
        ("two", ("NULL a", "2 a"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "integer"}]', "rows": ["(2,)", "(None,)"]},
    ),  # pre-existing
    (
        "B03-int-null",
        ("two", ("1 a", "NULL a"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "integer"}]', "rows": ["(1,)", "(None,)"]},
    ),
    (
        "B04-null-null",
        ("two", ("NULL a", "NULL a"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "void"}]', "rows": ["(None,)", "(None,)"]},
    ),
    (
        "B05-tinyint-smallint",
        ("two", ("CAST(1 AS TINYINT) a", "CAST(2 AS SMALLINT) a"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "short"}]', "rows": ["(1,)", "(2,)"]},
    ),  # pre-existing
    (
        "B06-int-bigint",
        ("two", ("1 a", "2L a"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "long"}]', "rows": ["(1,)", "(2,)"]},
    ),  # pre-existing
    (
        "B07-bigint-int",
        ("two", ("2L a", "1 a"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "long"}]', "rows": ["(1,)", "(2,)"]},
    ),
    (
        "B08-int-float/noansi",
        ("two", ("16777217 a", "CAST(2.5 AS FLOAT) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "float"}]',
            "rows": ["(16777216.0,)", "(2.5,)"],
        },
    ),  # pre-existing
    (
        "B08-int-float/ansi",
        ("two", ("16777217 a", "CAST(2.5 AS FLOAT) a"), {}),
        True,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "double"}]',
            "rows": ["(16777217.0,)", "(2.5,)"],
        },
    ),  # pre-existing
    (
        "B09-bigint-float/noansi",
        ("two", ("9007199254740993L a", "CAST(1.5 AS FLOAT) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "float"}]',
            "rows": ["(1.5,)", "(9007199254740992.0,)"],
        },
    ),  # pre-existing
    (
        "B09-bigint-float/ansi",
        ("two", ("9007199254740993L a", "CAST(1.5 AS FLOAT) a"), {}),
        True,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "double"}]',
            "rows": ["(1.5,)", "(9007199254740992.0,)"],
        },
    ),  # pre-existing
    (
        "B09b-float-int/noansi",
        ("two", ("CAST(2.5 AS FLOAT) a", "16777217 a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "float"}]',
            "rows": ["(16777216.0,)", "(2.5,)"],
        },
    ),
    (
        "B09b-float-int/ansi",
        ("two", ("CAST(2.5 AS FLOAT) a", "16777217 a"), {}),
        True,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "double"}]',
            "rows": ["(16777217.0,)", "(2.5,)"],
        },
    ),  # pre-existing
    (
        "B10-float-double",
        ("two", ("CAST(2.5 AS FLOAT) a", "1.5D a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "double"}]',
            "rows": ["(1.5,)", "(2.5,)"],
        },
    ),  # pre-existing
    (
        "B11-int-double",
        ("two", ("1 a", "1.5D a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "double"}]',
            "rows": ["(1.0,)", "(1.5,)"],
        },
    ),  # pre-existing
    (
        "B12-int-dec5_2",
        ("two", ("1 a", "CAST(2.25 AS DECIMAL(5,2)) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "decimal(12,2)"}]',
            "rows": ["(Decimal('1.00'),)", "(Decimal('2.25'),)"],
        },
    ),  # pre-existing
    (
        "B13-int-dec12_2",
        ("two", ("1 a", "CAST(2.25 AS DECIMAL(12,2)) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "decimal(12,2)"}]',
            "rows": ["(Decimal('1.00'),)", "(Decimal('2.25'),)"],
        },
    ),  # pre-existing
    (
        "B14-dec38_10-bigint",
        ("two", ("CAST(1.5 AS DECIMAL(38,10)) a", "9223372036854775807L a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "decimal(38,10)"}]',
            "rows": ["(Decimal('1.5000000000'),)", "(Decimal('9223372036854775807.0000000000'),)"],
        },
    ),
    _bug(
        "B15-dec38_20-bigint-cap/noansi",
        ("two", ("CAST('1.12345678901234567891' AS DECIMAL(38,20)) a", "9223372036854775807L a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "decimal(38,18)"}]',
            "rows": ["(Decimal('1.123456789012345679'),)", "(Decimal('9223372036854775807.000000000000000000'),)"],
        },
    ),  # pre-existing
    _bug(
        "B15-dec38_20-bigint-cap/ansi",
        ("two", ("CAST('1.12345678901234567891' AS DECIMAL(38,20)) a", "9223372036854775807L a"), {}),
        True,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "decimal(38,18)"}]',
            "rows": ["(Decimal('1.123456789012345679'),)", "(Decimal('9223372036854775807.000000000000000000'),)"],
        },
    ),  # pre-existing
    (
        "B16-dec38_0-dec38_38-cap/noansi",
        ("two", ("CAST(12345 AS DECIMAL(38,0)) a", "CAST(0.5 AS DECIMAL(38,38)) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "decimal(38,0)"}]',
            "rows": ["(Decimal('1'),)", "(Decimal('12345'),)"],
        },
    ),  # pre-existing
    (
        "B16-dec38_0-dec38_38-cap/ansi",
        ("two", ("CAST(12345 AS DECIMAL(38,0)) a", "CAST(0.5 AS DECIMAL(38,38)) a"), {}),
        True,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "decimal(38,0)"}]',
            "rows": ["(Decimal('1'),)", "(Decimal('12345'),)"],
        },
    ),  # pre-existing
    (
        "B16b-dec38_38-dec38_0",
        ("two", ("CAST(0.5 AS DECIMAL(38,38)) a", "CAST(12345 AS DECIMAL(38,0)) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "decimal(38,0)"}]',
            "rows": ["(Decimal('1'),)", "(Decimal('12345'),)"],
        },
    ),  # pre-existing
    (
        "B17-dec5_2-dec7_1",
        ("two", ("CAST(1.25 AS DECIMAL(5,2)) a", "CAST(123456.5 AS DECIMAL(7,1)) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "decimal(8,2)"}]',
            "rows": ["(Decimal('1.25'),)", "(Decimal('123456.50'),)"],
        },
    ),  # pre-existing
    (
        "B18-dec-float",
        ("two", ("CAST(1.25 AS DECIMAL(5,2)) a", "CAST(2.5 AS FLOAT) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "double"}]',
            "rows": ["(1.25,)", "(2.5,)"],
        },
    ),  # pre-existing
    (
        "B18b-double-dec",
        ("two", ("1.5D a", "CAST(1.25 AS DECIMAL(5,2)) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "double"}]',
            "rows": ["(1.25,)", "(1.5,)"],
        },
    ),  # pre-existing
    (
        "B19-date-ts",
        ("two", ("DATE'2024-01-02' a", "TIMESTAMP'2024-01-01 10:00:00' a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "timestamp"}]',
            "rows": ["(datetime.datetime(2024, 1, 1, 10, 0),)", "(datetime.datetime(2024, 1, 2, 0, 0),)"],
        },
    ),  # pre-existing
    (
        "B20-ntz-ts",
        ("two", ("TIMESTAMP_NTZ'2024-01-02 03:04:05' a", "TIMESTAMP'2024-01-01 10:00:00' a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "timestamp"}]',
            "rows": ["(datetime.datetime(2024, 1, 1, 10, 0),)", "(datetime.datetime(2024, 1, 2, 3, 4, 5),)"],
        },
    ),  # pre-existing
    (
        "B21-date-ntz",
        ("two", ("DATE'2024-01-02' a", "TIMESTAMP_NTZ'2024-01-01 10:00:00' a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "timestamp_ntz"}]',
            "rows": ["(datetime.datetime(2024, 1, 1, 10, 0),)", "(datetime.datetime(2024, 1, 2, 0, 0),)"],
        },
    ),  # pre-existing
    (
        "B22-ts-date",
        ("two", ("TIMESTAMP'2024-01-01 10:00:00' a", "DATE'2024-01-02' a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "timestamp"}]',
            "rows": ["(datetime.datetime(2024, 1, 1, 10, 0),)", "(datetime.datetime(2024, 1, 2, 0, 0),)"],
        },
    ),  # pre-existing
    _bug(
        "B23-dt-day-hour",
        ("two", ("INTERVAL '1' DAY a", "INTERVAL '2' HOUR a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "interval day to hour"}]',
            "rows": ["(datetime.timedelta(days=1),)", "(datetime.timedelta(seconds=7200),)"],
        },
    ),  # pre-existing
    (
        "B24-ym-year-month",
        ("two", ("INTERVAL '1' YEAR a", "INTERVAL '2' MONTH a"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "interval year to month"}]'},
    ),
    _bug(
        "B25-ym-dt/noansi",
        ("two", ("INTERVAL '1' YEAR a", "INTERVAL '2' DAY a"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "INTERVAL DAY" type which is not compatible with "INTERVAL YEAR" at the same column of the first table',
        },
    ),  # won(partial)
    _bug(
        "B25-ym-dt/ansi",
        ("two", ("INTERVAL '1' YEAR a", "INTERVAL '2' DAY a"), {}),
        True,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "INTERVAL DAY" type which is not compatible with "INTERVAL YEAR" at the same column of the first table',
        },
    ),  # won(partial)
    (
        "B26-cal-cal",
        ("two", ("make_interval(1, 2) a", "make_interval(0, 0, 0, 3) a"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "interval"}]'},
    ),
    _bug(
        "B27-cal-dt",
        ("two", ("make_interval(1, 2) a", "INTERVAL '2' DAY a"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "INTERVAL DAY" type which is not compatible with "INTERVAL" at the same column of the first table',
        },
    ),  # pre-existing
    (
        "B28-time-time",
        ("two", ("TIME'12:34:56' a", "TIME'01:02:03' a"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "time(6)"}]'},
    ),
    (
        "B30-time-ts",
        ("two", ("TIME'12:34:56' a", "TIMESTAMP'2024-01-01 10:00:00' a"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "TIMESTAMP" type which is not compatible with "TIME(6)" at the same column of the first table',
        },
    ),
    (
        "B31-bool-int/noansi",
        ("two", ("true a", "1 a"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "INT" type which is not compatible with "BOOLEAN" at the same column of the first table',
        },
    ),
    (
        "B31-bool-int/ansi",
        ("two", ("true a", "1 a"), {}),
        True,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "INT" type which is not compatible with "BOOLEAN" at the same column of the first table',
        },
    ),
    (
        "B32-date-int",
        ("two", ("DATE'2024-01-02' a", "1 a"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "INT" type which is not compatible with "DATE" at the same column of the first table',
        },
    ),
    (
        "B33-ts-bigint",
        ("two", ("TIMESTAMP'2024-01-01 10:00:00' a", "1L a"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "BIGINT" type which is not compatible with "TIMESTAMP" at the same column of the first table',
        },
    ),
    (
        "B34-binary-int",
        ("two", ("X'61' a", "1 a"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "INT" type which is not compatible with "BINARY" at the same column of the first table',
        },
    ),
    (
        "B35-bigint-dec20_0",
        ("two", ("1L a", "CAST(2 AS DECIMAL(20,0)) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "decimal(20,0)"}]',
            "rows": ["(Decimal('1'),)", "(Decimal('2'),)"],
        },
    ),  # pre-existing
    _bug(
        "B36-tinyint-dec2_0",
        ("two", ("CAST(1 AS TINYINT) a", "CAST(2 AS DECIMAL(2,0)) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "decimal(3,0)"}]',
            "rows": ["(Decimal('1'),)", "(Decimal('2'),)"],
        },
    ),  # pre-existing
    (
        "B37-binary-binary",
        ("two", ("X'61' a", "X'6263' a", "intersect"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "binary"}]', "rows": []},
    ),
    (
        "C-str-int/noansi",
        ("two", ("'7' a", "1 a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["('1',)", "('7',)"],
        },
    ),
    (
        "C-str-int/ansi",
        ("two", ("'7' a", "1 a"), {}),
        True,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "long"}]', "rows": ["(1,)", "(7,)"]},
    ),  # pre-existing
    (
        "C-str-tinyint/noansi",
        ("two", ("'7' a", "CAST(1 AS TINYINT) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["('1',)", "('7',)"],
        },
    ),
    (
        "C-str-tinyint/ansi",
        ("two", ("'7' a", "CAST(1 AS TINYINT) a"), {}),
        True,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "long"}]', "rows": ["(1,)", "(7,)"]},
    ),  # pre-existing
    (
        "C-str-bigint/noansi",
        ("two", ("'7' a", "1L a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["('1',)", "('7',)"],
        },
    ),
    (
        "C-str-bigint/ansi",
        ("two", ("'7' a", "1L a"), {}),
        True,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "long"}]', "rows": ["(1,)", "(7,)"]},
    ),  # pre-existing
    (
        "C-str-double/noansi",
        ("two", ("'7.5' a", "1.5D a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["('1.5',)", "('7.5',)"],
        },
    ),
    (
        "C-str-double/ansi",
        ("two", ("'7.5' a", "1.5D a"), {}),
        True,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "double"}]', "rows": ["(1.5,)", "(7.5,)"]},
    ),  # pre-existing
    (
        "C-str-float/noansi",
        ("two", ("'7.5' a", "CAST(1.5 AS FLOAT) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["('1.5',)", "('7.5',)"],
        },
    ),
    (
        "C-str-float/ansi",
        ("two", ("'7.5' a", "CAST(1.5 AS FLOAT) a"), {}),
        True,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "double"}]', "rows": ["(1.5,)", "(7.5,)"]},
    ),  # pre-existing
    (
        "C-str-dec10_2/noansi",
        ("two", ("'7.5' a", "CAST(1.25 AS DECIMAL(10,2)) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["('1.25',)", "('7.5',)"],
        },
    ),
    (
        "C-str-dec10_2/ansi",
        ("two", ("'7.5' a", "CAST(1.25 AS DECIMAL(10,2)) a"), {}),
        True,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "double"}]',
            "rows": ["(1.25,)", "(7.5,)"],
        },
    ),  # pre-existing
    (
        "C-str-dec10_0/noansi",
        ("two", ("'7' a", "CAST(1 AS DECIMAL(10,0)) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["('1',)", "('7',)"],
        },
    ),
    (
        "C-str-dec10_0/ansi",
        ("two", ("'7' a", "CAST(1 AS DECIMAL(10,0)) a"), {}),
        True,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "double"}]', "rows": ["(1.0,)", "(7.0,)"]},
    ),  # pre-existing
    (
        "C-str-date/noansi",
        ("two", ("'2024-01-02' a", "DATE'2024-01-01' a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["('2024-01-01',)", "('2024-01-02',)"],
        },
    ),  # pre-existing
    (
        "C-str-date/ansi",
        ("two", ("'2024-01-02' a", "DATE'2024-01-01' a"), {}),
        True,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "date"}]',
            "rows": ["(datetime.date(2024, 1, 1),)", "(datetime.date(2024, 1, 2),)"],
        },
    ),  # pre-existing
    (
        "C-str-ts/noansi",
        ("two", ("'2024-01-02 03:04:05' a", "TIMESTAMP'2024-01-01 10:00:00' a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["('2024-01-01 10:00:00',)", "('2024-01-02 03:04:05',)"],
        },
    ),  # pre-existing
    (
        "C-str-ts/ansi",
        ("two", ("'2024-01-02 03:04:05' a", "TIMESTAMP'2024-01-01 10:00:00' a"), {}),
        True,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "timestamp"}]',
            "rows": ["(datetime.datetime(2024, 1, 1, 10, 0),)", "(datetime.datetime(2024, 1, 2, 3, 4, 5),)"],
        },
    ),  # pre-existing
    (
        "C-str-ntz/noansi",
        ("two", ("'2024-01-02 03:04:05' a", "TIMESTAMP_NTZ'2024-01-01 10:00:00' a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["('2024-01-01 10:00:00',)", "('2024-01-02 03:04:05',)"],
        },
    ),  # pre-existing
    (
        "C-str-ntz/ansi",
        ("two", ("'2024-01-02 03:04:05' a", "TIMESTAMP_NTZ'2024-01-01 10:00:00' a"), {}),
        True,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "timestamp_ntz"}]',
            "rows": ["(datetime.datetime(2024, 1, 1, 10, 0),)", "(datetime.datetime(2024, 1, 2, 3, 4, 5),)"],
        },
    ),  # pre-existing
    (
        "C-str-bool/noansi",
        ("two", ("'true' a", "false a"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "BOOLEAN" type which is not compatible with "STRING" at the same column of the first table',
        },
    ),
    (
        "C-str-bool/ansi",
        ("two", ("'true' a", "false a"), {}),
        True,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "boolean"}]',
            "rows": ["(False,)", "(True,)"],
        },
    ),  # pre-existing
    (
        "C-str-binary/noansi",
        ("two", ("'ab' a", "X'6364' a"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "BINARY" type which is not compatible with "STRING" at the same column of the first table',
        },
    ),
    (
        "C-str-binary/ansi",
        ("two", ("'ab' a", "X'6364' a"), {}),
        True,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "binary"}]',
            "rows": ["(b'ab',)", "(b'cd',)"],
        },
    ),  # pre-existing
    _bug(
        "C-str-dt/noansi",
        ("two", ("'1' a", "INTERVAL '1' DAY a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["(\"INTERVAL '1' DAY\",)", "('1',)"],
        },
    ),  # pre-existing
    _bug(
        "C-str-dt/ansi",
        ("two", ("'1' a", "INTERVAL '1' DAY a"), {}),
        True,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "INTERVAL DAY" type which is not compatible with "STRING" at the same column of the first table. To fix the error, you might need to add explicit type casts. If necessary set spark.sql.ansi.enabled to false to bypass this error',
        },
    ),  # won(partial)
    _bug(
        "C-str-ym/noansi",
        ("two", ("'1' a", "INTERVAL '1' YEAR a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["(\"INTERVAL '1' YEAR\",)", "('1',)"],
        },
    ),  # pre-existing
    _bug(
        "C-str-ym/ansi",
        ("two", ("'1' a", "INTERVAL '1' YEAR a"), {}),
        True,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "INTERVAL YEAR" type which is not compatible with "STRING" at the same column of the first table. To fix the error, you might need to add explicit type casts. If necessary set spark.sql.ansi.enabled to false to bypass this error',
        },
    ),  # won(partial)
    (
        "C-str-time/noansi",
        ("two", ("'12:00:00' a", "TIME'01:02:03' a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["('01:02:03',)", "('12:00:00',)"],
        },
    ),  # pre-existing
    (
        "C-int-str/noansi",
        ("two", ("1 a", "'7' a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["('1',)", "('7',)"],
        },
    ),  # pre-existing
    (
        "C-int-str/ansi",
        ("two", ("1 a", "'7' a"), {}),
        True,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "long"}]', "rows": ["(1,)", "(7,)"]},
    ),  # pre-existing
    (
        "C-double-str/noansi",
        ("two", ("1.5D a", "'7.5' a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["('1.5',)", "('7.5',)"],
        },
    ),  # pre-existing
    (
        "C-double-str/ansi",
        ("two", ("1.5D a", "'7.5' a"), {}),
        True,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "double"}]', "rows": ["(1.5,)", "(7.5,)"]},
    ),  # pre-existing
    (
        "C-date-str/noansi",
        ("two", ("DATE'2024-01-01' a", "'2024-01-02' a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["('2024-01-01',)", "('2024-01-02',)"],
        },
    ),  # pre-existing
    (
        "C-date-str/ansi",
        ("two", ("DATE'2024-01-01' a", "'2024-01-02' a"), {}),
        True,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "date"}]',
            "rows": ["(datetime.date(2024, 1, 1),)", "(datetime.date(2024, 1, 2),)"],
        },
    ),  # pre-existing
    (
        "C-bool-str/noansi",
        ("two", ("false a", "'true' a"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "STRING" type which is not compatible with "BOOLEAN" at the same column of the first table',
        },
    ),
    (
        "C-bool-str/ansi",
        ("two", ("false a", "'true' a"), {}),
        True,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "boolean"}]',
            "rows": ["(False,)", "(True,)"],
        },
    ),  # pre-existing
    (
        "C-binary-str/noansi",
        ("two", ("X'6364' a", "'ab' a"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "STRING" type which is not compatible with "BINARY" at the same column of the first table',
        },
    ),
    (
        "C-binary-str/ansi",
        ("two", ("X'6364' a", "'ab' a"), {}),
        True,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "binary"}]',
            "rows": ["(b'ab',)", "(b'cd',)"],
        },
    ),
    _bug(
        "C-dt-str/noansi",
        ("two", ("INTERVAL '1' DAY a", "'1' a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["(\"INTERVAL '1' DAY\",)", "('1',)"],
        },
    ),  # pre-existing
    _bug(
        "C-dt-str/ansi",
        ("two", ("INTERVAL '1' DAY a", "'1' a"), {}),
        True,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "STRING" type which is not compatible with "INTERVAL DAY" at the same column of the first table. To fix the error, you might need to add explicit type casts. If necessary set spark.sql.ansi.enabled to false to bypass this error',
        },
    ),  # won(partial)
    (
        "C-abc-int/noansi",
        ("two", ("'abc' a", "1 a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["('1',)", "('abc',)"],
        },
    ),
    _bug(
        "C-abc-int/ansi",
        ("two", ("'abc' a", "1 a"), {}),
        True,
        False,
        {
            "error": "CAST_INVALID_INPUT",
            "message": '[CAST_INVALID_INPUT] The value \'abc\' of the type "STRING" cannot be cast to "BIGINT" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead',
        },
    ),  # pre-existing
    (
        "C-abc-int-emptyleft/noansi",
        ("two", ("'abc' a WHERE false", "1 a"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]', "rows": ["('1',)"]},
    ),
    _bug(
        "C-abc-int-emptyleft/ansi",
        ("two", ("'abc' a WHERE false", "1 a"), {}),
        True,
        False,
        {
            "error": "CAST_INVALID_INPUT",
            "message": '[CAST_INVALID_INPUT] The value \'abc\' of the type "STRING" cannot be cast to "BIGINT" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead',
        },
    ),  # pre-existing
    (
        "C-str-int-intersect/noansi",
        ("two", ("'1' a", "1 a", "intersect"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]', "rows": ["('1',)"]},
    ),
    (
        "C-str-int-intersect/ansi",
        ("two", ("'1' a", "1 a", "intersect"), {}),
        True,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "long"}]', "rows": ["(1,)"]},
    ),  # pre-existing
    (
        "C-str-bool-intersect/noansi",
        ("two", ("'true' a", "true a", "intersect"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] INTERSECT can only be performed on tables with compatible column types. The first column of the second table is "BOOLEAN" type which is not compatible with "STRING" at the same column of the first table',
        },
    ),
    (
        "C-str-bool-intersect/ansi",
        ("two", ("'true' a", "true a", "intersect"), {}),
        True,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "boolean"}]', "rows": ["(True,)"]},
    ),  # pre-existing
    (
        "C-str-binary-subtract/noansi",
        ("two", ("'ab' a", "X'6162' a", "subtract"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] EXCEPT can only be performed on tables with compatible column types. The first column of the second table is "BINARY" type which is not compatible with "STRING" at the same column of the first table',
        },
    ),
    (
        "C-str-binary-subtract/ansi",
        ("two", ("'ab' a", "X'6162' a", "subtract"), {}),
        True,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "binary"}]', "rows": []},
    ),  # pre-existing
    (
        "C-str-dt-intersect/noansi",
        ("two", ("'1' a", "INTERVAL '1' DAY a", "intersect"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]', "rows": []},
    ),  # pre-existing
    (
        "C-str-dt-exceptAll/noansi",
        ("two", ("'1' a", "INTERVAL '1' DAY a", "exceptAll"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]', "rows": ["('1',)"]},
    ),  # pre-existing
    (
        "C-abc-bool-intersect/noansi",
        ("two", ("'x' a", "true a", "intersect"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] INTERSECT can only be performed on tables with compatible column types. The first column of the second table is "BOOLEAN" type which is not compatible with "STRING" at the same column of the first table',
        },
    ),
    _bug(
        "C-abc-bool-intersect/ansi",
        ("two", ("'x' a", "true a", "intersect"), {}),
        True,
        False,
        {
            "error": "CAST_INVALID_INPUT",
            "message": '[CAST_INVALID_INPUT] The value \'x\' of the type "STRING" cannot be cast to "BOOLEAN" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead',
        },
    ),  # pre-existing
    (
        "D01-arr-int-bigint",
        ("two", ("array(1) a", "array(2L) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": {"containsNull": false, "elementType": "long", "type": "array"}}]',
            "rows": ["([1],)", "([2],)"],
        },
    ),  # pre-existing
    (
        "D02-arr-int-str/noansi",
        ("two", ("array(1) a", "array('2') a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": {"containsNull": false, "elementType": "string", "type": "array"}}]',
            "rows": ["(['1'],)", "(['2'],)"],
        },
    ),  # pre-existing
    (
        "D02-arr-int-str/ansi",
        ("two", ("array(1) a", "array('2') a"), {}),
        True,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": {"containsNull": true, "elementType": "long", "type": "array"}}]',
            "rows": ["([1],)", "([2],)"],
        },
    ),  # pre-existing
    (
        "D03-arr-int-bool",
        ("two", ("array(1) a", "array(true) a"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "ARRAY<BOOLEAN>" type which is not compatible with "ARRAY<INT>" at the same column of the first table',
        },
    ),
    (
        "D04-arr-containsNull",
        ("two", ("array(1) a", "array(1, NULL) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": {"containsNull": true, "elementType": "integer", "type": "array"}}]',
            "rows": ["([1, None],)", "([1],)"],
        },
    ),
    (
        "D05-map-val-widen",
        ("two", ("map('k', 1) a", "map('k', 2L) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": {"keyType": "string", "type": "map", "valueContainsNull": false, "valueType": "long"}}]',
            "rows": ["({'k': 1},)", "({'k': 2},)"],
        },
    ),  # pre-existing
    (
        "D06-map-key-widen",
        ("two", ("map(1, 1) a", "map(2L, 1) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": {"keyType": "long", "type": "map", "valueContainsNull": false, "valueType": "integer"}}]',
            "rows": ["({1: 1},)", "({2: 1},)"],
        },
    ),  # pre-existing
    (
        "D07-map-key-str-int/noansi",
        ("two", ("map('1', 1) a", "map(2, 1) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": {"keyType": "string", "type": "map", "valueContainsNull": false, "valueType": "integer"}}]',
            "rows": ["({'1': 1},)", "({'2': 1},)"],
        },
    ),  # pre-existing
    (
        "D07-map-key-str-int/ansi",
        ("two", ("map('1', 1) a", "map(2, 1) a"), {}),
        True,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "MAP<INT, INT>" type which is not compatible with "MAP<STRING, INT>" at the same column of the first table. To fix the error, you might need to add explicit type casts. If necessary set spark.sql.ansi.enabled to false to bypass this error',
        },
    ),
    (
        "D08-map-key-dec-cap/noansi",
        ("two", ("map(1L, 1) a", "map(CAST(1 AS DECIMAL(38,20)), 1) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": {"keyType": "decimal(38,18)", "type": "map", "valueContainsNull": false, "valueType": "integer"}}]',
            "rows": ["({Decimal('1.000000000000000000'): 1},)", "({Decimal('1.000000000000000000'): 1},)"],
        },
    ),  # pre-existing
    (
        "D08-map-key-dec-cap/ansi",
        ("two", ("map(1L, 1) a", "map(CAST(1 AS DECIMAL(38,20)), 1) a"), {}),
        True,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": {"keyType": "decimal(38,18)", "type": "map", "valueContainsNull": false, "valueType": "integer"}}]',
            "rows": ["({Decimal('1.000000000000000000'): 1},)", "({Decimal('1.000000000000000000'): 1},)"],
        },
    ),  # pre-existing
    (
        "D09-map-key-date-ts",
        ("two", ("map(DATE'2024-01-01', 1) a", "map(TIMESTAMP'2024-01-01 00:00:00', 1) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": {"keyType": "timestamp", "type": "map", "valueContainsNull": false, "valueType": "integer"}}]',
            "rows": ["({datetime.datetime(2024, 1, 1, 0, 0): 1},)", "({datetime.datetime(2024, 1, 1, 0, 0): 1},)"],
        },
    ),  # pre-existing
    (
        "D10-map-key-int-double",
        ("two", ("map(1, 1) a", "map(1.5D, 1) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": {"keyType": "double", "type": "map", "valueContainsNull": false, "valueType": "integer"}}]',
            "rows": ["({1.0: 1},)", "({1.5: 1},)"],
        },
    ),  # pre-existing
    (
        "D11-struct-widen",
        ("two", ("named_struct('a', 1) s", "named_struct('a', 2L) s"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": false, "type": {"fields": [{"metadata": {}, "name": "a", "nullable": false, "type": "long"}], "type": "struct"}}]',
            "rows": ["(Row(a=1),)", "(Row(a=2),)"],
        },
    ),  # pre-existing
    (
        "D12-struct-names-union",
        ("two", ("named_struct('a', 1) s", "named_struct('b', 2) s"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": false, "type": {"fields": [{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}], "type": "struct"}}]',
            "rows": ["(Row(a=1),)", "(Row(a=2),)"],
        },
    ),  # pre-existing
    (
        "D13-struct-names-widen",
        ("two", ("named_struct('a', 1) s", "named_struct('b', 2L) s"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "STRUCT<b: BIGINT NOT NULL>" type which is not compatible with "STRUCT<a: INT NOT NULL>" at the same column of the first table',
        },
    ),  # won(partial)
    (
        "D14-struct-names-intersect",
        ("two", ("named_struct('a', 1) s", "named_struct('b', 1) s", "intersect"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] INTERSECT can only be performed on tables with compatible column types. The first column of the second table is "STRUCT<b: INT NOT NULL>" type which is not compatible with "STRUCT<a: INT NOT NULL>" at the same column of the first table',
        },
    ),  # won(partial)
    (
        "D15-struct-case-widen/cs-off",
        ("two", ("named_struct('a', 1) s", "named_struct('A', 2L) s"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": false, "type": {"fields": [{"metadata": {}, "name": "a", "nullable": false, "type": "long"}], "type": "struct"}}]',
            "rows": ["(Row(a=1),)", "(Row(a=2),)"],
        },
    ),  # pre-existing
    (
        "D15-struct-case-widen/cs-on",
        ("two", ("named_struct('a', 1) s", "named_struct('A', 2L) s"), {}),
        False,
        True,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "STRUCT<A: BIGINT NOT NULL>" type which is not compatible with "STRUCT<a: INT NOT NULL>" at the same column of the first table',
        },
    ),  # won(partial)
    (
        "D16-struct-case-intersect/cs-off",
        ("two", ("named_struct('a', 1) s", "named_struct('A', 1) s", "intersect"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": false, "type": {"fields": [{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}], "type": "struct"}}]',
            "rows": ["(Row(a=1),)"],
        },
    ),  # pre-existing
    (
        "D16-struct-case-intersect/cs-on",
        ("two", ("named_struct('a', 1) s", "named_struct('A', 1) s", "intersect"), {}),
        False,
        True,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] INTERSECT can only be performed on tables with compatible column types. The first column of the second table is "STRUCT<A: INT NOT NULL>" type which is not compatible with "STRUCT<a: INT NOT NULL>" at the same column of the first table',
        },
    ),  # won(partial)
    (
        "D16b-struct-case-union/cs-on",
        ("two", ("named_struct('a', 1) s", "named_struct('A', 1) s"), {}),
        False,
        True,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": false, "type": {"fields": [{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}], "type": "struct"}}]',
            "rows": ["(Row(a=1),)", "(Row(a=1),)"],
        },
    ),  # pre-existing
    (
        "D17-struct-count",
        ("two", ("named_struct('a', 1, 'b', 2) s", "named_struct('a', 1) s"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "STRUCT<a: INT NOT NULL>" type which is not compatible with "STRUCT<a: INT NOT NULL, b: INT NOT NULL>" at the same column of the first table',
        },
    ),  # won(partial)
    (
        "D18-struct-null-union",
        ("two", ("named_struct('a', 1) s", "named_struct('a', CAST(NULL AS INT)) s"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": false, "type": {"fields": [{"metadata": {}, "name": "a", "nullable": true, "type": "integer"}], "type": "struct"}}]',
            "rows": ["(Row(a=1),)", "(Row(a=None),)"],
        },
    ),  # pre-existing
    (
        "D18-struct-null-intersect",
        ("two", ("named_struct('a', 1) s", "named_struct('a', CAST(NULL AS INT)) s", "intersect"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": false, "type": {"fields": [{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}], "type": "struct"}}]',
            "rows": [],
        },
    ),  # pre-existing
    (
        "D18-struct-null-except",
        ("two", ("named_struct('a', CAST(NULL AS INT)) s", "named_struct('a', 1) s", "subtract"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": false, "type": {"fields": [{"metadata": {}, "name": "a", "nullable": true, "type": "integer"}], "type": "struct"}}]',
            "rows": ["(Row(a=None),)"],
        },
    ),  # pre-existing
    (
        "D19-struct-swapped",
        ("two", ("named_struct('a', 1, 'b', 'x') s", "named_struct('b', 'y', 'a', 2) s"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "STRUCT<b: STRING NOT NULL, a: INT NOT NULL>" type which is not compatible with "STRUCT<a: INT NOT NULL, b: STRING NOT NULL>" at the same column of the first table',
        },
    ),  # won(partial)
    (
        "D20-3lvl-union",
        ("two", ("array(named_struct('m', map('k', 1))) a", "array(named_struct('m', map('k', 2L))) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": {"containsNull": false, "elementType": {"fields": [{"metadata": {}, "name": "m", "nullable": false, "type": {"keyType": "string", "type": "map", "valueContainsNull": false, "valueType": "long"}}], "type": "struct"}, "type": "array"}}]',
            "rows": ["([Row(m={'k': 1})],)", "([Row(m={'k': 2})],)"],
        },
    ),  # pre-existing
    (
        "D20-3lvl-intersect",
        (
            "two",
            ("array(named_struct('m', map('k', 1))) a", "array(named_struct('m', map('k', 2L))) a", "intersect"),
            {},
        ),
        False,
        False,
        {
            "error": "UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
            "message": '[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE] The feature is not supported: Cannot have MAP type columns in DataFrame which calls set operations (INTERSECT, EXCEPT, etc.), but the type of column `a` is "ARRAY<STRUCT<m: MAP<STRING, BIGINT> NOT NULL>>"',
        },
    ),  # won(partial)
    (
        "D21-arr-struct-names",
        ("two", ("array(named_struct('a', 1)) a", "array(named_struct('b', 2)) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": {"containsNull": false, "elementType": {"fields": [{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}], "type": "struct"}, "type": "array"}}]',
            "rows": ["([Row(a=1)],)", "([Row(a=2)],)"],
        },
    ),  # pre-existing
    (
        "D22-arr-int",
        ("two", ("array(1) a", "1 a"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "INT" type which is not compatible with "ARRAY<INT>" at the same column of the first table',
        },
    ),
    (
        "D22b-arr-str/noansi",
        ("two", ("array(1) a", "'x' a"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "STRING" type which is not compatible with "ARRAY<INT>" at the same column of the first table',
        },
    ),
    (
        "D22b-arr-str/ansi",
        ("two", ("array(1) a", "'x' a"), {}),
        True,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "STRING" type which is not compatible with "ARRAY<INT>" at the same column of the first table',
        },
    ),
    (
        "D23-map-valueContainsNull",
        ("two", ("map('k', 1) a", "map('k', CAST(NULL AS INT)) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": {"keyType": "string", "type": "map", "valueContainsNull": true, "valueType": "integer"}}]',
            "rows": ["({'k': 1},)", "({'k': None},)"],
        },
    ),  # pre-existing
    (
        "D24-struct-str",
        ("two", ("named_struct('a', 1) s", "'x' s"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "STRING" type which is not compatible with "STRUCT<a: INT NOT NULL>" at the same column of the first table',
        },
    ),  # won(partial)
    (
        "D25-3lvl-case/cs-off",
        ("two", ("array(named_struct('M', map('k', 1))) a", "array(named_struct('m', map('k', 2L))) a"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": {"containsNull": false, "elementType": {"fields": [{"metadata": {}, "name": "M", "nullable": false, "type": {"keyType": "string", "type": "map", "valueContainsNull": false, "valueType": "long"}}], "type": "struct"}, "type": "array"}}]',
            "rows": ["([Row(M={'k': 1})],)", "([Row(M={'k': 2})],)"],
        },
    ),  # pre-existing
    (
        "D25-3lvl-case/cs-on",
        ("two", ("array(named_struct('M', map('k', 1))) a", "array(named_struct('m', map('k', 2L))) a"), {}),
        False,
        True,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "ARRAY<STRUCT<m: MAP<STRING, BIGINT> NOT NULL>>" type which is not compatible with "ARRAY<STRUCT<M: MAP<STRING, INT> NOT NULL>>" at the same column of the first table',
        },
    ),  # won(partial)
    (
        "D26-arr-struct-widen-intersect",
        ("two", ("array(named_struct('a', 1)) a", "array(named_struct('a', 1L)) a", "intersect"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": {"containsNull": false, "elementType": {"fields": [{"metadata": {}, "name": "a", "nullable": false, "type": "long"}], "type": "struct"}, "type": "array"}}]',
            "rows": ["([Row(a=1)],)"],
        },
    ),  # pre-existing
    (
        "E01-intersect-int-bigint",
        ("two", ("1 a", "1L a", "intersect"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "long"}]', "rows": ["(1,)"]},
    ),  # pre-existing
    (
        "E02-exceptAll-bool-int",
        ("two", ("true a", "1 a", "exceptAll"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] EXCEPT ALL can only be performed on tables with compatible column types. The first column of the second table is "INT" type which is not compatible with "BOOLEAN" at the same column of the first table',
        },
    ),
    (
        "E03-intersectAll-bool-int",
        ("two", ("true a", "1 a", "intersectAll"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] INTERSECT ALL can only be performed on tables with compatible column types. The first column of the second table is "INT" type which is not compatible with "BOOLEAN" at the same column of the first table',
        },
    ),
    (
        "E04-minus-bool-int",
        ("two", ("true a", "1 a", "MINUS"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] EXCEPT can only be performed on tables with compatible column types. The first column of the second table is "INT" type which is not compatible with "BOOLEAN" at the same column of the first table',
        },
    ),
    (
        "E05-sql-intersect-bool-int",
        ("two", ("true a", "1 a", "INTERSECT"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] INTERSECT can only be performed on tables with compatible column types. The first column of the second table is "INT" type which is not compatible with "BOOLEAN" at the same column of the first table',
        },
    ),
    (
        "E06-sql-except-all-bool-int",
        ("two", ("true a", "1 a", "EXCEPT ALL"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] EXCEPT ALL can only be performed on tables with compatible column types. The first column of the second table is "INT" type which is not compatible with "BOOLEAN" at the same column of the first table',
        },
    ),
    (
        "E07-sql-union-bool-int",
        ("two", ("true a", "1 a", "UNION"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "INT" type which is not compatible with "BOOLEAN" at the same column of the first table',
        },
    ),
    (
        "E08-intersect-float-int",
        ("two", ("CAST(1 AS FLOAT) a", "1 a", "intersect"), {}),
        True,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "double"}]', "rows": ["(1.0,)"]},
    ),  # pre-existing
    _bug(
        "F01-hint-fixable-first-unfixable-second",
        ("two", ("'1' a, true b", "INTERVAL '1' DAY a, 1 b"), {}),
        True,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "INTERVAL DAY" type which is not compatible with "STRING" at the same column of the first table',
        },
    ),  # won(partial)
    (
        "F02-hint-unfixable-first-fixable-second",
        ("two", ("true a, '1' b", "1 a, INTERVAL '1' DAY b"), {}),
        True,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "INT" type which is not compatible with "BOOLEAN" at the same column of the first table',
        },
    ),
    _bug(
        "F03-hint-two-fixable",
        ("two", ("'1' a, '1' b", "INTERVAL '1' DAY a, INTERVAL '1' YEAR b"), {}),
        True,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "INTERVAL DAY" type which is not compatible with "STRING" at the same column of the first table. To fix the error, you might need to add explicit type casts. If necessary set spark.sql.ansi.enabled to false to bypass this error',
        },
    ),  # won(partial)
    (
        "F04-hint-intersect-dt-plus-bool",
        ("two", ("'1' a, true b", "INTERVAL '1' DAY a, 1 b", "intersect"), {}),
        True,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] INTERSECT can only be performed on tables with compatible column types. The second column of the second table is "INT" type which is not compatible with "BOOLEAN" at the same column of the first table',
        },
    ),
    (
        "G01-5cols-third",
        ("two", ("1 a, 2 b, true c, 4 d, 5 e", "1 a, 2 b, 3 c, 4 d, 5 e"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The third column of the second table is "INT" type which is not compatible with "BOOLEAN" at the same column of the first table',
        },
    ),
    (
        "G02-5cols-5th",
        ("two", ("1 a, 2 b, 3 c, 4 d, true e", "1 a, 2 b, 3 c, 4 d, 5 e"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The 5th column of the second table is "INT" type which is not compatible with "BOOLEAN" at the same column of the first table',
        },
    ),
    (
        "G03-5cols-second-and-4th",
        ("two", ("1 a, true b, 3 c, true d, 5 e", "1 a, 2 b, 3 c, 4 d, 5 e"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The second column of the second table is "INT" type which is not compatible with "BOOLEAN" at the same column of the first table',
        },
    ),
    (
        "G04-5cols-5th-intersect",
        ("two", ("1 a, 2 b, 3 c, 4 d, DATE'2024-01-01' e", "1 a, 2 b, 3 c, 4 d, 5 e", "intersect"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] INTERSECT can only be performed on tables with compatible column types. The 5th column of the second table is "INT" type which is not compatible with "DATE" at the same column of the first table',
        },
    ),
    (
        "G05-chain3-third-type",
        ("chain", (["1 a, 2 b, 3 c, 4 d", "1 a, 2 b, 3 c, 4 d", "1 a, 2 b, 3 c, true d"],), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The 4th column of the second table is "BOOLEAN" type which is not compatible with "INT" at the same column of the first table',
        },
    ),
    (
        "G06-6cols-4th-except",
        ("two", ("1 a, 2 b, 3 c, X'61' d, 5 e, 6 f", "1 a, 2 b, 3 c, 4 d, 5 e, 6 f", "subtract"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] EXCEPT can only be performed on tables with compatible column types. The 4th column of the second table is "INT" type which is not compatible with "BINARY" at the same column of the first table',
        },
    ),
    (
        "H01-byName-reorder",
        ("two", ("1 a, 'x' b", "'y' b, 2 a", "unionByName"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}, {"metadata": {}, "name": "b", "nullable": false, "type": "string"}]',
            "rows": ["(1, 'x')", "(2, 'y')"],
        },
    ),
    (
        "H02-byName-case/cs-off",
        ("two", ("1 a", "2 A", "unionByName"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}]', "rows": ["(1,)", "(2,)"]},
    ),
    (
        "H02-byName-case/cs-on",
        ("two", ("1 a", "2 A", "unionByName"), {}),
        False,
        True,
        {
            "error": "UNRESOLVED_COLUMN_AMONG_FIELD_NAMES",
            "message": '[UNRESOLVED_COLUMN_AMONG_FIELD_NAMES] Cannot resolve column name "a" among (A)',
        },
    ),
    (
        "H03-byName-missing",
        ("two", ("1 a, 2 b", "1 a, 3 c", "unionByName"), {}),
        False,
        False,
        {
            "error": "UNRESOLVED_COLUMN_AMONG_FIELD_NAMES",
            "message": '[UNRESOLVED_COLUMN_AMONG_FIELD_NAMES] Cannot resolve column name "b" among (a, c)',
        },
    ),
    (
        "H04-byName-left-dup",
        ("two", ("1 a, 2 a", "1 a, 2 b", "unionByName"), {}),
        False,
        False,
        {
            "error": "COLUMN_ALREADY_EXISTS",
            "message": "[COLUMN_ALREADY_EXISTS] The column `a` already exists. Choose another name or rename the existing column",
        },
    ),  # pre-existing
    (
        "H05-byName-right-dup",
        ("two", ("1 a, 2 b", "1 b, 2 b", "unionByName"), {}),
        False,
        False,
        {
            "error": "COLUMN_ALREADY_EXISTS",
            "message": "[COLUMN_ALREADY_EXISTS] The column `b` already exists. Choose another name or rename the existing column",
        },
    ),  # pre-existing
    (
        "H06-byName-case-dup/cs-off",
        ("two", ("1 a, 2 A", "1 a, 2 b", "unionByName"), {}),
        False,
        False,
        {
            "error": "COLUMN_ALREADY_EXISTS",
            "message": "[COLUMN_ALREADY_EXISTS] The column `a` already exists. Choose another name or rename the existing column",
        },
    ),  # pre-existing
    (
        "H06-byName-case-dup/cs-on",
        ("two", ("1 a, 2 A", "1 a, 2 b", "unionByName"), {}),
        False,
        True,
        {
            "error": "UNRESOLVED_COLUMN_AMONG_FIELD_NAMES",
            "message": '[UNRESOLVED_COLUMN_AMONG_FIELD_NAMES] Cannot resolve column name "A" among (a, b)',
        },
    ),
    (
        "H07-byName-nested-reorder",
        ("two", ("named_struct('a', 1, 'b', 'x') s", "named_struct('b', 'y', 'a', 2) s", "unionByName"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": false, "type": {"fields": [{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}, {"metadata": {}, "name": "b", "nullable": false, "type": "string"}], "type": "struct"}}]',
            "rows": ["(Row(a=1, b='x'),)", "(Row(a=2, b='y'),)"],
        },
    ),  # pre-existing
    (
        "H08-byName-nested-missing",
        ("two", ("named_struct('a', 1, 'b', 2) s", "named_struct('a', 3) s", "unionByName"), {}),
        False,
        False,
        {"error": "FIELD_NOT_FOUND", "message": "[FIELD_NOT_FOUND] No such struct field `b` in `a`"},
    ),  # won(partial)
    (
        "H09-byName-nested-extra",
        ("two", ("named_struct('a', 1) s", "named_struct('a', 3, 'b', 4) s", "unionByName"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "STRUCT<a: INT NOT NULL, b: INT NOT NULL>" type which is not compatible with "STRUCT<a: INT NOT NULL>" at the same column of the first table',
        },
    ),  # won(partial)
    (
        "H10-byNameMissing-nested",
        ("two", ("named_struct('a', 1, 'b', 2) s", "named_struct('b', 3, 'c', 4) s", "unionByNameMissing"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": false, "type": {"fields": [{"metadata": {}, "name": "a", "nullable": true, "type": "integer"}, {"metadata": {}, "name": "b", "nullable": false, "type": "integer"}, {"metadata": {}, "name": "c", "nullable": true, "type": "integer"}], "type": "struct"}}]',
            "rows": ["(Row(a=1, b=2, c=None),)", "(Row(a=None, b=3, c=4),)"],
        },
    ),  # pre-existing
    (
        "H11-byName-array-struct-reorder",
        (
            "two",
            ("array(named_struct('a', 1, 'b', 'x')) s", "array(named_struct('b', 'y', 'a', 2)) s", "unionByName"),
            {},
        ),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": false, "type": {"containsNull": false, "elementType": {"fields": [{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}, {"metadata": {}, "name": "b", "nullable": false, "type": "string"}], "type": "struct"}, "type": "array"}}]',
            "rows": ["([Row(a=1, b='x')],)", "([Row(a=2, b='y')],)"],
        },
    ),  # pre-existing
    (
        "H12-byName-map-struct-positional",
        (
            "two",
            ("map('k', named_struct('a', 1, 'b', 2)) s", "map('k', named_struct('b', 3, 'a', 4)) s", "unionByName"),
            {},
        ),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": false, "type": {"keyType": "string", "type": "map", "valueContainsNull": false, "valueType": {"fields": [{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}, {"metadata": {}, "name": "b", "nullable": false, "type": "integer"}], "type": "struct"}}}]',
            "rows": ["({'k': Row(a=1, b=2)},)", "({'k': Row(a=3, b=4)},)"],
        },
    ),  # pre-existing
    (
        "H13-byName-widen",
        ("two", ("1 a, 'x' b", "'y' b, 2L a", "unionByName"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "long"}, {"metadata": {}, "name": "b", "nullable": false, "type": "string"}]',
            "rows": ["(1, 'x')", "(2, 'y')"],
        },
    ),  # pre-existing
    (
        "H14-byNameMissing-top",
        ("two", ("1 a, 2 b", "3 c, 4 a", "unionByNameMissing"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}, {"metadata": {}, "name": "b", "nullable": true, "type": "integer"}, {"metadata": {}, "name": "c", "nullable": true, "type": "integer"}]',
            "rows": ["(1, 2, None)", "(4, None, 3)"],
        },
    ),
    (
        "H15-byName-nested-case/cs-off",
        ("two", ("named_struct('a', 1) s", "named_struct('A', 2) s", "unionByName"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": false, "type": {"fields": [{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}], "type": "struct"}}]',
            "rows": ["(Row(a=1),)", "(Row(a=2),)"],
        },
    ),  # pre-existing
    (
        "H15-byName-nested-case/cs-on",
        ("two", ("named_struct('a', 1) s", "named_struct('A', 2) s", "unionByName"), {}),
        False,
        True,
        {"error": "FIELD_NOT_FOUND", "message": "[FIELD_NOT_FOUND] No such struct field `a` in `A`"},
    ),  # pre-existing
    (
        "H16-byName-meta",
        ("meta_pair", ("1 a", {"k": "L"}, "2 a", {"k": "R"}, "unionByName"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {"k": "L"}, "name": "a", "nullable": false, "type": "integer"}]',
            "rows": ["(1,)", "(2,)"],
        },
    ),
    _bug(
        "H17-byNameMissing-arr-struct",
        ("two", ("array(named_struct('a', 1)) s", "array(named_struct('b', 2)) s", "unionByNameMissing"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": false, "type": {"containsNull": false, "elementType": {"fields": [{"metadata": {}, "name": "a", "nullable": true, "type": "integer"}, {"metadata": {}, "name": "b", "nullable": true, "type": "integer"}], "type": "struct"}, "type": "array"}}]',
            "rows": ["([Row(a=1, b=None)],)", "([Row(a=None, b=2)],)"],
        },
    ),  # pre-existing
    (
        "H18-byName-nested-missing-2lvl",
        (
            "two",
            (
                "named_struct('x', named_struct('a', 1, 'b', 2)) s",
                "named_struct('x', named_struct('a', 3)) s",
                "unionByName",
            ),
            {},
        ),
        False,
        False,
        {"error": "FIELD_NOT_FOUND", "message": "[FIELD_NOT_FOUND] No such struct field `b` in `a`"},
    ),  # won(partial)
    (
        "H19-byName-nested-types-widen",
        ("two", ("named_struct('a', 1, 'b', 2) s", "named_struct('b', 3L, 'a', 4) s", "unionByName"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": false, "type": {"fields": [{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}, {"metadata": {}, "name": "b", "nullable": false, "type": "long"}], "type": "struct"}}]',
            "rows": ["(Row(a=1, b=2),)", "(Row(a=4, b=3),)"],
        },
    ),  # pre-existing
    (
        "I01-intersect-map",
        ("two", ("map('k', 1) m", "map('k', 1) m", "intersect"), {}),
        False,
        False,
        {
            "error": "UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
            "message": '[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE] The feature is not supported: Cannot have MAP type columns in DataFrame which calls set operations (INTERSECT, EXCEPT, etc.), but the type of column `m` is "MAP<STRING, INT>"',
        },
    ),
    (
        "I02-subtract-struct-map",
        ("two", ("named_struct('x', map('k', 1)) m", "named_struct('x', map('k', 1)) m", "subtract"), {}),
        False,
        False,
        {
            "error": "UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
            "message": '[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE] The feature is not supported: Cannot have MAP type columns in DataFrame which calls set operations (INTERSECT, EXCEPT, etc.), but the type of column `m` is "STRUCT<x: MAP<STRING, INT> NOT NULL>"',
        },
    ),  # won(partial)
    (
        "I03-unionAll-map",
        ("two", ("map('k', 1) m", "map('k', 2) m"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "m", "nullable": false, "type": {"keyType": "string", "type": "map", "valueContainsNull": false, "valueType": "integer"}}]',
            "rows": ["({'k': 1},)", "({'k': 2},)"],
        },
    ),  # pre-existing
    (
        "I04-sql-union-map",
        ("two", ("map('k', 1) m", "map('k', 2) m", "UNION"), {}),
        False,
        False,
        {
            "error": "UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
            "message": '[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE] The feature is not supported: Cannot have MAP type columns in DataFrame which calls set operations (INTERSECT, EXCEPT, etc.), but the type of column `m` is "MAP<STRING, INT>"',
        },
    ),
    (
        "I05-null-intersect-map",
        ("two", ("NULL m", "map('k', 1) m", "intersect"), {}),
        False,
        False,
        {
            "error": "UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
            "message": '[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE] The feature is not supported: Cannot have MAP type columns in DataFrame which calls set operations (INTERSECT, EXCEPT, etc.), but the type of column `m` is "MAP<STRING, INT>"',
        },
    ),
    (
        "I06-intersect-variant",
        ("two", ("parse_json('1') v", "parse_json('1') v", "intersect"), {}),
        False,
        False,
        {
            "error": "UNSUPPORTED_FEATURE.SET_OPERATION_ON_VARIANT_TYPE",
            "message": '[UNSUPPORTED_FEATURE.SET_OPERATION_ON_VARIANT_TYPE] The feature is not supported: Cannot have VARIANT type columns in DataFrame which calls set operations (INTERSECT, EXCEPT, etc.), but the type of column `v` is "VARIANT"',
        },
    ),  # pre-existing
    (
        "I07-sql-union-variant",
        ("two", ("parse_json('1') v", "parse_json('2') v", "UNION"), {}),
        False,
        False,
        {
            "error": "UNSUPPORTED_FEATURE.SET_OPERATION_ON_VARIANT_TYPE",
            "message": '[UNSUPPORTED_FEATURE.SET_OPERATION_ON_VARIANT_TYPE] The feature is not supported: Cannot have VARIANT type columns in DataFrame which calls set operations (INTERSECT, EXCEPT, etc.), but the type of column `v` is "VARIANT"',
        },
    ),  # pre-existing
    (
        "I08-unionAll-variant",
        ("two", ("parse_json('1') v", "parse_json('2') v"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "v", "nullable": false, "type": "variant"}]',
            "rows": [
                "(VariantVal(b'\\x0c\\x01', b'\\x01\\x00\\x00'),)",
                "(VariantVal(b'\\x0c\\x02', b'\\x01\\x00\\x00'),)",
            ],
        },
    ),  # pre-existing
    (
        "I09-exceptAll-arr-variant",
        ("two", ("array(parse_json('1')) v", "array(parse_json('1')) v", "exceptAll"), {}),
        False,
        False,
        {
            "error": "UNSUPPORTED_FEATURE.SET_OPERATION_ON_VARIANT_TYPE",
            "message": '[UNSUPPORTED_FEATURE.SET_OPERATION_ON_VARIANT_TYPE] The feature is not supported: Cannot have VARIANT type columns in DataFrame which calls set operations (INTERSECT, EXCEPT, etc.), but the type of column `v` is "ARRAY<VARIANT>"',
        },
    ),  # pre-existing
    (
        "I10-int-intersect-map",
        ("two", ("1 m", "map('k', 1) m", "intersect"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] INTERSECT can only be performed on tables with compatible column types. The first column of the second table is "MAP<STRING, INT>" type which is not compatible with "INT" at the same column of the first table',
        },
    ),
    (
        "I11-union-distinct-map",
        ("two", ("map('k', 1) m", "map('k', 2) m", "union.distinct"), {}),
        False,
        False,
        {
            "error": "UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
            "message": '[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE] The feature is not supported: Cannot have MAP type columns in DataFrame which calls set operations (INTERSECT, EXCEPT, etc.), but the type of column `m` is "MAP<STRING, INT>"',
        },
    ),
    (
        "I12-exceptAll-map",
        ("two", ("map('k', 1) m", "map('k', 1) m", "exceptAll"), {}),
        False,
        False,
        {
            "error": "UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
            "message": '[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE] The feature is not supported: Cannot have MAP type columns in DataFrame which calls set operations (INTERSECT, EXCEPT, etc.), but the type of column `m` is "MAP<STRING, INT>"',
        },
    ),
    (
        "I13-variant-str/noansi",
        ("two", ("parse_json('1') v", "'x' v"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "v", "nullable": true, "type": "string"}]', "rows": ["('1',)", "('x',)"]},
    ),  # pre-existing
    (
        "I13-variant-str/ansi",
        ("two", ("parse_json('1') v", "'x' v"), {}),
        True,
        False,
        {
            "schema": '[{"metadata": {}, "name": "v", "nullable": true, "type": "variant"}]',
            "rows": [
                "(VariantVal(b'\\x05x', b'\\x01\\x00\\x00'),)",
                "(VariantVal(b'\\x0c\\x01', b'\\x01\\x00\\x00'),)",
            ],
        },
    ),  # pre-existing
    (
        "I14-variant-int",
        ("two", ("parse_json('1') v", "1 v"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "INT" type which is not compatible with "VARIANT" at the same column of the first table',
        },
    ),  # pre-existing
    (
        "I15-map-intersect-null",
        ("two", ("map('k', 1) m", "NULL m", "intersect"), {}),
        False,
        False,
        {
            "error": "UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
            "message": '[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE] The feature is not supported: Cannot have MAP type columns in DataFrame which calls set operations (INTERSECT, EXCEPT, etc.), but the type of column `m` is "MAP<STRING, INT>"',
        },
    ),
    (
        "I16-intersect-map-renamed",
        ("two", ("map('k', 1) m", "map('k', 1) n", "intersect"), {}),
        False,
        False,
        {
            "error": "UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
            "message": '[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE] The feature is not supported: Cannot have MAP type columns in DataFrame which calls set operations (INTERSECT, EXCEPT, etc.), but the type of column `m` is "MAP<STRING, INT>"',
        },
    ),
    (
        "J01-meta-kept",
        ("meta_pair", ("1 a", {"k": "L"}, "2 a", {"k": "R"}), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {"k": "L"}, "name": "a", "nullable": false, "type": "integer"}]',
            "rows": ["(1,)", "(2,)"],
        },
    ),
    (
        "J02-meta-left-cast",
        ("meta_pair", ("1 a", {"k": "L"}, "2L a", {"k": "R"}), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "long"}]', "rows": ["(1,)", "(2,)"]},
    ),  # pre-existing
    (
        "J03-meta-left-none",
        ("meta_pair", ("1 a", None, "2 a", {"k": "R"}), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}]', "rows": ["(1,)", "(2,)"]},
    ),
    (
        "J04-meta-right-cast",
        ("meta_pair", ("1L a", {"k": "L"}, "2 a", {"k": "R"}), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {"k": "L"}, "name": "a", "nullable": false, "type": "long"}]',
            "rows": ["(1,)", "(2,)"],
        },
    ),
    (
        "J05-meta-intersect-kept",
        ("meta_pair", ("1 a", {"k": "L"}, "1 a", {"k": "R"}, "intersect"), {}),
        False,
        False,
        {"schema": '[{"metadata": {"k": "L"}, "name": "a", "nullable": false, "type": "integer"}]', "rows": ["(1,)"]},
    ),
    (
        "J06-meta-intersect-cast",
        ("meta_pair", ("1 a", {"k": "L"}, "1L a", {"k": "R"}, "intersect"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "long"}]', "rows": ["(1,)"]},
    ),  # pre-existing
    (
        "J07-meta-exceptAll",
        ("meta_pair", ("1 a", {"k": "L"}, "2 a", {"k": "R"}, "exceptAll"), {}),
        False,
        False,
        {"schema": '[{"metadata": {"k": "L"}, "name": "a", "nullable": false, "type": "integer"}]', "rows": ["(1,)"]},
    ),
    (
        "J07b-meta-subtract-cast",
        ("meta_pair", ("1 a", {"k": "L"}, "2L a", {"k": "R"}, "subtract"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "long"}]', "rows": ["(1,)"]},
    ),  # pre-existing
    (
        "J08-nested-meta-kept",
        ("nested_meta", (IntegerType(), IntegerType()), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": true, "type": {"fields": [{"metadata": {"k": "L"}, "name": "x", "nullable": true, "type": "integer"}], "type": "struct"}}]',
            "rows": ["(Row(x=1),)", "(Row(x=2),)"],
        },
    ),
    (
        "J09-nested-meta-left-cast",
        ("nested_meta", (IntegerType(), LongType()), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": true, "type": {"fields": [{"metadata": {}, "name": "x", "nullable": true, "type": "long"}], "type": "struct"}}]',
            "rows": ["(Row(x=1),)", "(Row(x=2),)"],
        },
    ),  # pre-existing
    (
        "J10-nested-meta-array-kept",
        ("nested_meta", (IntegerType(), IntegerType()), {"wrap_array": True}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": true, "type": {"containsNull": true, "elementType": {"fields": [{"metadata": {"k": "L"}, "name": "x", "nullable": true, "type": "integer"}], "type": "struct"}, "type": "array"}}]',
            "rows": ["([Row(x=1)],)", "([Row(x=2)],)"],
        },
    ),
    (
        "J10b-nested-meta-intersect",
        ("nested_meta", (IntegerType(), IntegerType()), {"op": "intersect"}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": true, "type": {"fields": [{"metadata": {"k": "L"}, "name": "x", "nullable": true, "type": "integer"}], "type": "struct"}}]',
            "rows": [],
        },
    ),
    (
        "J10c-nested-meta-byName",
        ("nested_meta", (IntegerType(), IntegerType()), {"op": "unionByName"}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": true, "type": {"fields": [{"metadata": {"k": "L"}, "name": "x", "nullable": true, "type": "integer"}], "type": "struct"}}]',
            "rows": ["(Row(x=1),)", "(Row(x=2),)"],
        },
    ),
    (
        "J11-null-union-nonnull-nullable",
        ("two", ("1 a", "a FROM VALUES (2), (NULL) t(a)"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "integer"}]',
            "rows": ["(1,)", "(2,)", "(None,)"],
        },
    ),
    (
        "J12-null-union-nonnull-nonnull",
        ("two", ("1 a", "2 a"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}]', "rows": ["(1,)", "(2,)"]},
    ),
    _bug(
        "J13-null-intersect-nullable-nonnull",
        ("two", ("a FROM VALUES (1), (NULL) t(a)", "1 a", "intersect"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}]', "rows": ["(1,)"]},
    ),
    (
        "J13b-null-intersect-nonnull-nullable",
        ("two", ("1 a", "a FROM VALUES (1), (NULL) t(a)", "intersect"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}]', "rows": ["(1,)"]},
    ),
    (
        "J13c-null-intersect-both",
        ("two", ("a FROM VALUES (1), (NULL) t(a)", "a FROM VALUES (NULL) t(a)", "intersect"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "integer"}]', "rows": ["(None,)"]},
    ),
    (
        "J14-null-except-nullable-left",
        ("two", ("a FROM VALUES (1), (NULL) t(a)", "1 a", "subtract"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "integer"}]', "rows": ["(None,)"]},
    ),
    (
        "J14b-null-except-nonnull-left",
        ("two", ("1 a", "a FROM VALUES (2), (NULL) t(a)", "exceptAll"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}]', "rows": ["(1,)"]},
    ),
    (
        "J15-empty-left-widen",
        ("two", ("1 a WHERE false", "2L a"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "long"}]', "rows": ["(2,)"]},
    ),  # pre-existing
    (
        "J16-empty-intersect",
        ("two", ("1 a", "1L a WHERE false", "intersect"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "long"}]', "rows": []},
    ),  # pre-existing
    (
        "J17-chain3-int-bigint-str/noansi",
        ("chain", (["1 a", "2L a", "'3' a"],), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["('1',)", "('2',)", "('3',)"],
        },
    ),  # pre-existing
    (
        "J17-chain3-int-bigint-str/ansi",
        ("chain", (["1 a", "2L a", "'3' a"],), {}),
        True,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "long"}]',
            "rows": ["(1,)", "(2,)", "(3,)"],
        },
    ),  # pre-existing
    (
        "J17b-chain3-str-int-date/noansi",
        ("chain", (["'1' a", "2 a", "DATE'2024-01-01' a"],), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "string"}]',
            "rows": ["('1',)", "('2',)", "('2024-01-01',)"],
        },
    ),  # pre-existing
    (
        "J17b-chain3-str-int-date/ansi",
        ("chain", (["'1' a", "2 a", "DATE'2024-01-01' a"],), {}),
        True,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "DATE" type which is not compatible with "BIGINT" at the same column of the first table',
        },
    ),  # pre-existing
    (
        "J18-chain3-nullability",
        ("chain", (["1 a", "2 a", "a FROM VALUES (NULL) t(a)"],), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "integer"}]',
            "rows": ["(1,)", "(2,)", "(None,)"],
        },
    ),
    (
        "J19-chain3-meta-cast",
        ("meta_chain", (["1 a", "2L a", "3 a"], {"k": "L"}), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "long"}]',
            "rows": ["(1,)", "(2,)", "(3,)"],
        },
    ),  # pre-existing
    (
        "J19b-chain3-meta-kept",
        ("meta_chain", (["1L a", "2 a", "3 a"], {"k": "L"}), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {"k": "L"}, "name": "a", "nullable": false, "type": "long"}]',
            "rows": ["(1,)", "(2,)", "(3,)"],
        },
    ),
    (
        "J20-vector-union",
        ("vector_pair", ("vector", "union"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "v", "nullable": true, "type": {"class": "org.apache.spark.ml.linalg.VectorUDT", "pyClass": "pyspark.ml.linalg.VectorUDT", "sqlType": {"fields": [{"metadata": {}, "name": "type", "nullable": false, "type": "byte"}, {"metadata": {}, "name": "size", "nullable": true, "type": "integer"}, {"metadata": {}, "name": "indices", "nullable": true, "type": {"containsNull": false, "elementType": "integer", "type": "array"}}, {"metadata": {}, "name": "values", "nullable": true, "type": {"containsNull": false, "elementType": "double", "type": "array"}}], "type": "struct"}, "type": "udt"}}]',
            "rows": ["(DenseVector([1.0, 2.0]),)", "(DenseVector([1.0, 2.0]),)", "(SparseVector(2, {0: 3.0}),)"],
        },
    ),
    (
        "J21-vector-int",
        ("vector_pair", ("int", "union"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "INT" type which is not compatible with UDT("STRUCT<type: TINYINT NOT NULL, size: INT, indices: ARRAY<INT>, values: ARRAY<DOUBLE>>") at the same column of the first table',
        },
    ),  # pre-existing
    (
        "J22-vector-intersect",
        ("vector_pair", ("vector", "intersect"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "v", "nullable": true, "type": {"class": "org.apache.spark.ml.linalg.VectorUDT", "pyClass": "pyspark.ml.linalg.VectorUDT", "sqlType": {"fields": [{"metadata": {}, "name": "type", "nullable": false, "type": "byte"}, {"metadata": {}, "name": "size", "nullable": true, "type": "integer"}, {"metadata": {}, "name": "indices", "nullable": true, "type": {"containsNull": false, "elementType": "integer", "type": "array"}}, {"metadata": {}, "name": "values", "nullable": true, "type": {"containsNull": false, "elementType": "double", "type": "array"}}], "type": "struct"}, "type": "udt"}}]',
            "rows": ["(DenseVector([1.0, 2.0]),)"],
        },
    ),
    _bug(
        "J23-pq-binary-intersect",
        ("pq_pair", (["b"], ["b"], "intersect"), {"pq_right": True}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "b", "nullable": true, "type": "binary"}]', "rows": ["(b'ab',)"]},
    ),  # pre-existing
    (
        "J23b-pq-binary-lit",
        ("pq_pair", (["b"], "X'6162' b", "intersect"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "b", "nullable": false, "type": "binary"}]', "rows": ["(b'ab',)"]},
    ),
    _bug(
        "J24-pq-string-subtract-lit",
        ("pq_pair", (["s"], "'zz' s", "subtract"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "s", "nullable": true, "type": "string"}]', "rows": ["('ab',)"]},
    ),  # pre-existing
    _bug(
        "J25-pq-ts-union-ntz",
        ("pq_pair", (["ts"], "TIMESTAMP_NTZ'2024-06-01 00:00:00' ts", "union"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "ts", "nullable": true, "type": "timestamp"}]',
            "rows": ["(datetime.datetime(2024, 1, 1, 0, 0),)", "(datetime.datetime(2024, 6, 1, 0, 0),)"],
        },
    ),  # pre-existing
    _bug(
        "J25b-pq-ntz-union-ts",
        ("pq_pair", (["ntz"], "TIMESTAMP'2024-06-01 00:00:00' ntz", "union"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "ntz", "nullable": true, "type": "timestamp"}]',
            "rows": ["(datetime.datetime(2024, 1, 1, 0, 0),)", "(datetime.datetime(2024, 6, 1, 0, 0),)"],
        },
    ),  # pre-existing
    _bug(
        "J26-pq-string-union-int/noansi",
        ("pq_pair", (["s"], "1 s", "union"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": true, "type": "string"}]',
            "rows": ["('1',)", "('ab',)"],
        },
    ),  # pre-existing
    _bug(
        "J26-pq-string-union-int/ansi",
        ("pq_pair", (["s"], "1 s", "union"), {}),
        True,
        False,
        {
            "error": "CAST_INVALID_INPUT",
            "message": '[CAST_INVALID_INPUT] The value \'ab\' of the type "STRING" cannot be cast to "BIGINT" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead',
        },
    ),  # pre-existing
    _bug(
        "J27-pq-ts-union-date",
        ("pq_pair", (["ts"], "DATE'2024-06-01' ts", "union"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "ts", "nullable": true, "type": "timestamp"}]',
            "rows": ["(datetime.datetime(2024, 1, 1, 0, 0),)", "(datetime.datetime(2024, 6, 1, 0, 0),)"],
        },
    ),  # pre-existing
    _bug(
        "J28-pq-arr-intersect",
        ("pq_pair", (["arr"], "array('x') arr", "intersect"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "arr", "nullable": false, "type": {"containsNull": true, "elementType": "string", "type": "array"}}]',
            "rows": ["(['x'],)"],
        },
    ),  # pre-existing
    _bug(
        "J29-pq-binary-union-str",
        ("pq_pair", (["b"], "'ab' b", "union"), {}),
        True,
        False,
        {
            "schema": '[{"metadata": {}, "name": "b", "nullable": true, "type": "binary"}]',
            "rows": ["(b'ab',)", "(b'ab',)"],
        },
    ),  # pre-existing
    (
        "J30-arr-nested-null-meta",
        ("meta_pair", ("array(1) a", {"k": "L"}, "array(1, NULL) a", {"k": "R"}), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {"k": "L"}, "name": "a", "nullable": false, "type": {"containsNull": true, "elementType": "integer", "type": "array"}}]',
            "rows": ["([1, None],)", "([1],)"],
        },
    ),
    (
        "J31-names-left",
        ("two", ("1 a", "2 b"), {}),
        False,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": "integer"}]', "rows": ["(1,)", "(2,)"]},
    ),
    (
        "J32-intersect-arr-containsNull",
        ("two", ("array(1) a", "a FROM VALUES (array(1)), (array(CAST(NULL AS INT))) t(a)", "intersect"), {}),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "a", "nullable": false, "type": {"containsNull": false, "elementType": "integer", "type": "array"}}]',
            "rows": ["([1],)"],
        },
    ),
    (
        "J33-createDF-nullable-struct",
        (
            "schema_pair",
            (
                StructType([StructField("s", StructType([StructField("x", IntegerType(), False)]), False)]),
                [((1,),)],
                StructType([StructField("s", StructType([StructField("x", IntegerType(), True)]), True)]),
                [((None,),)],
                "union",
            ),
            {},
        ),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": true, "type": {"fields": [{"metadata": {}, "name": "x", "nullable": true, "type": "integer"}], "type": "struct"}}]',
            "rows": ["(Row(x=1),)", "(Row(x=None),)"],
        },
    ),
    _bug(
        "J33b-createDF-nullable-struct-intersect",
        (
            "schema_pair",
            (
                StructType([StructField("s", StructType([StructField("x", IntegerType(), True)]), True)]),
                [((1,),)],
                StructType([StructField("s", StructType([StructField("x", IntegerType(), False)]), False)]),
                [((1,),)],
                "intersect",
            ),
            {},
        ),
        False,
        False,
        {
            "schema": '[{"metadata": {}, "name": "s", "nullable": false, "type": {"fields": [{"metadata": {}, "name": "x", "nullable": true, "type": "integer"}], "type": "struct"}}]',
            "rows": ["(Row(x=1),)"],
        },
    ),
    _bug(
        "K01-collation-lcase-vs-default",
        ("two", ("'a' COLLATE UTF8_LCASE a", "'b' a"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "STRING" type which is not compatible with "STRING COLLATE UTF8_LCASE" at the same column of the first table',
        },
    ),  # pre-existing
    _bug(
        "K02-collation-lcase-vs-unicode",
        ("two", ("'a' COLLATE UTF8_LCASE a", "'b' COLLATE UNICODE a"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "STRING COLLATE UNICODE" type which is not compatible with "STRING COLLATE UTF8_LCASE" at the same column of the first table',
        },
    ),  # pre-existing
    _bug(
        "K03-collation-intersect",
        ("two", ("'a' COLLATE UTF8_LCASE a", "'A' COLLATE UNICODE a", "intersect"), {}),
        False,
        False,
        {
            "error": "INCOMPATIBLE_COLUMN_TYPE",
            "message": '[INCOMPATIBLE_COLUMN_TYPE] INTERSECT can only be performed on tables with compatible column types. The first column of the second table is "STRING COLLATE UNICODE" type which is not compatible with "STRING COLLATE UTF8_LCASE" at the same column of the first table',
        },
    ),  # pre-existing
    _bug(
        "K04-collation-vs-int/ansi",
        ("two", ("'1' COLLATE UTF8_LCASE a", "1 a"), {}),
        True,
        False,
        {"schema": '[{"metadata": {}, "name": "a", "nullable": true, "type": "long"}]', "rows": ["(1,)", "(1,)"]},
    ),  # pre-existing
]


@pytest.fixture
def configured(spark):
    keys = ["spark.sql.ansi.enabled", "spark.sql.caseSensitive", "spark.sql.session.timeZone"]
    yield spark
    for key in keys:
        spark.conf.unset(key)


@pytest.mark.parametrize("local_timezone", ["UTC"], indirect=True)
@pytest.mark.parametrize(
    ("leaf", "shape", "ansi", "case_sensitive", "expected"), _MATRIX, ids=lambda x: x if isinstance(x, str) else ""
)
def test_set_operation_branch(configured, local_timezone, tmp_path, leaf, shape, ansi, case_sensitive, expected):  # noqa: ARG001
    spark = configured
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    spark.conf.set("spark.sql.caseSensitive", str(case_sensitive).lower())
    name, args, kwargs = shape
    if "error" in expected:
        # A SQL query is analyzed as soon as it is built, so the build is inside the block too.
        with pytest.raises(Exception) as info:  # noqa: PT011
            _build_and_collect(spark, tmp_path, name, args, kwargs)
        assert _message(info.value) == (expected["error"], expected["message"])
        return
    df = _BUILDERS[name](spark, tmp_path, *args, **kwargs)
    assert df.schema.jsonValue()["fields"] == json.loads(expected["schema"])
    if "rows" in expected:
        assert sorted(repr(tuple(r)) for r in df.collect()) == expected["rows"]
