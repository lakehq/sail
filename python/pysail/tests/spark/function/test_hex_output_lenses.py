"""`hex` seen through every way a client reads a result.

The SQL `.feature` suite reads rows with `collect()`. A PySpark client has other paths that do not
share it: `toPandas` and `toArrow` convert the Arrow batches the server sends against the schema the
server DECLARED, so a result that declares `nullable=false` but carries a NULL, or a string type the
client does not expect, fails there and nowhere else; a write to Parquet and a read back goes through
the file schema. The values are asserted, not only that nothing raised.

Inputs are built from an explicit `StructType` so that the input nullability is exactly what is
declared, with a NULL row where the column allows it.
"""

import datetime
from decimal import Decimal

import pandas as pd
import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as T  # noqa: N812

# (id, Spark type, values with a NULL, expected hex of each value, forced nullable by the cast to BIGINT)
FAMILIES = [
    ("tinyint", T.ByteType(), [5, None, -1], ["5", None, "FFFFFFFFFFFFFFFF"], False),
    ("int", T.IntegerType(), [17, None, -1], ["11", None, "FFFFFFFFFFFFFFFF"], False),
    ("bigint", T.LongType(), [255, None, -1], ["FF", None, "FFFFFFFFFFFFFFFF"], False),
    ("double", T.DoubleType(), [255.9, None, -1.5], ["FF", None, "FFFFFFFFFFFFFFFF"], True),
    (
        "decimal",
        T.DecimalType(10, 2),
        [Decimal("255.99"), None, Decimal("-1.50")],
        ["FF", None, "FFFFFFFFFFFFFFFF"],
        True,
    ),
    ("string", T.StringType(), ["ab", None, "niño"], ["6162", None, "6E69C3B16F"], False),
    ("binary", T.BinaryType(), [b"\x00\xff", None, b""], ["00FF", None, ""], False),
    ("boolean", T.BooleanType(), [True, None, False], ["74727565", None, "66616C7365"], False),
    (
        "date",
        T.DateType(),
        [datetime.date(2024, 1, 2), None, datetime.date(1, 1, 1)],
        ["323032342D30312D3032", None, "303030312D30312D3031"],
        False,
    ),
]
IDS = [f[0] for f in FAMILIES]


@pytest.fixture(params=["true", "false"], ids=["ansi_on", "ansi_off"])
def ansi(spark, request):
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", request.param)
    yield request.param
    spark.conf.set("spark.sql.ansi.enabled", previous)


def _frame(spark, data_type, values, nullable):
    schema = T.StructType(
        [T.StructField("id", T.IntegerType(), nullable=False), T.StructField("c", data_type, nullable=nullable)]
    )
    rows = [(i, v) for i, v in enumerate(values) if nullable or v is not None]
    return spark.createDataFrame(rows, schema).select("id", F.hex("c").alias("h")).orderBy("id")


def _expected(values, expected, nullable):
    return [e for v, e in zip(values, expected, strict=False) if nullable or v is not None]


def _none(values):
    """pandas reports a NULL string as None or NaN depending on the version."""
    return [None if (v is None or (isinstance(v, float) and pd.isna(v))) else v for v in values]


@pytest.mark.parametrize(("_", "data_type", "values", "expected", "forced"), FAMILIES, ids=IDS)
def test_collect(spark, ansi, _, data_type, values, expected, forced):  # noqa: ARG001
    df = _frame(spark, data_type, values, nullable=True)
    assert [r["h"] for r in df.collect()] == expected


@pytest.mark.parametrize(("_", "data_type", "values", "expected", "forced"), FAMILIES, ids=IDS)
def test_to_pandas(spark, ansi, _, data_type, values, expected, forced):  # noqa: ARG001
    df = _frame(spark, data_type, values, nullable=True)
    assert _none(df.toPandas()["h"].tolist()) == expected


@pytest.mark.parametrize(("_", "data_type", "values", "expected", "forced"), FAMILIES, ids=IDS)
def test_to_arrow(spark, ansi, _, data_type, values, expected, forced):  # noqa: ARG001
    df = _frame(spark, data_type, values, nullable=True)
    if not hasattr(df, "toArrow"):
        pytest.skip("DataFrame.toArrow needs PySpark 4.0+")
    table = df.toArrow()
    assert table.column("h").to_pylist() == expected


@pytest.mark.parametrize(("_", "data_type", "values", "expected", "forced"), FAMILIES, ids=IDS)
def test_non_null_input_to_pandas_and_schema(spark, ansi, _, data_type, values, expected, forced):  # noqa: ARG001
    """A column declared non-nullable has no NULL row. Its result follows Spark: non-nullable, except
    where the cast to BIGINT can add a NULL of its own (a fractional or decimal input)."""
    df = _frame(spark, data_type, values, nullable=False)
    assert df.schema["h"].nullable is forced
    assert df.toPandas()["h"].tolist() == _expected(values, expected, nullable=False)


@pytest.mark.parametrize(("_", "data_type", "values", "expected", "forced"), FAMILIES, ids=IDS)
def test_write_parquet_and_read_back(spark, tmp_path, ansi, _, data_type, values, expected, forced):  # noqa: ARG001
    df = _frame(spark, data_type, values, nullable=True)
    path = str(tmp_path / "out")
    df.write.parquet(path)
    back = spark.read.parquet(path).orderBy("id")
    assert [r["h"] for r in back.collect()] == expected


def test_local_iterator_and_head(spark, ansi):  # noqa: ARG001
    df = _frame(spark, T.DoubleType(), [255.9, None, -1.5], nullable=True)
    assert [r["h"] for r in df.toLocalIterator()] == ["FF", None, "FFFFFFFFFFFFFFFF"]
    assert df.head()["h"] == "FF"
    assert df.tail(1)[0]["h"] == "FFFFFFFFFFFFFFFF"
