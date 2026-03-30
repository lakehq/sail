import pytest
from pyspark.sql import types as T  # noqa: N812

if not hasattr(T.StructType, "toDDL"):
    pytest.skip("`toDDL` is not available in this Spark version", allow_module_level=True)


def test_null(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("null_col1", T.NullType()),
            T.StructField("null_col1", T.NullType()),
        ]
    )
    assert schema.toDDL() == "null_col1 NULL, null_col1 NULL"


def test_binary(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("col", T.BinaryType()),
        ]
    )
    assert schema.toDDL() == "col BINARY"


def test_boolean(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("col", T.BooleanType()),
        ]
    )
    assert schema.toDDL() == "col BOOLEAN"


def test_byte(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("col", T.ByteType()),
        ]
    )
    assert schema.toDDL() == "col TINYINT"


def test_short(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("col", T.ShortType()),
        ]
    )
    assert schema.toDDL() == "col SHORT"


def test_integer(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("col", T.IntegerType()),
        ]
    )
    assert schema.toDDL() == "col INT"


def test_long(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("col", T.LongType()),
        ]
    )
    assert schema.toDDL() == "col BIGINT"


def test_float(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("col", T.FloatType()),
        ]
    )
    assert schema.toDDL() == "col FLOAT"


def test_double(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("col", T.DoubleType()),
        ]
    )
    assert schema.toDDL() == "col DOUBLE"


def test_decimal(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("a", T.DecimalType(10, 2)),
            T.StructField("b", T.DecimalType(15, 3)),
            T.StructField("c", T.DecimalType(20, 4)),
        ]
    )
    assert schema.toDDL() == "a DECIMAL(10,2), b DECIMAL(15,3), c DECIMAL(20,4)"


def test_string(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("col", T.StringType()),
        ]
    )
    assert schema.toDDL() == "col STRING"


def test_char(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("a", T.CharType(1)),
            T.StructField("b", T.CharType(2)),
            T.StructField("c", T.CharType(3)),
        ]
    )
    assert schema.toDDL() == "a CHAR(1), b CHAR(2), c CHAR(3)"


def test_varchar(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("a", T.VarcharType(1)),
            T.StructField("b", T.VarcharType(2)),
            T.StructField("c", T.VarcharType(3)),
        ]
    )
    assert schema.toDDL() == "a VARCHAR(1), b VARCHAR(2), c VARCHAR(3)"


def test_date(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("col", T.DateType()),
        ]
    )
    assert schema.toDDL() == "col DATE"


def test_timestamp(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("col", T.TimestampType()),
        ]
    )
    assert schema.toDDL() == "col TIMESTAMP"


def test_timestampntz(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("col", T.TimestampNTZType()),
        ]
    )
    assert schema.toDDL() == "col TIMESTAMP_NTZ"


def test_year_month_interval(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("a", T.YearMonthIntervalType(0, 0)),
            T.StructField("b", T.YearMonthIntervalType(0, 1)),
            T.StructField("c", T.YearMonthIntervalType(1, 0)),
        ]
    )
    assert schema.toDDL() == "a INTERVAL YEAR, b INTERVAL YEAR TO MONTH, c INTERVAL MONTH TO YEAR"

    schema = T.StructType(
        [
            T.StructField("d", T.YearMonthIntervalType(1, 1)),
            T.StructField("e", T.YearMonthIntervalType(startField=0)),
            T.StructField("f", T.YearMonthIntervalType(startField=1)),
            T.StructField("g", T.YearMonthIntervalType()),
        ]
    )

    assert schema.toDDL() == "d INTERVAL MONTH, e INTERVAL YEAR, f INTERVAL MONTH, g INTERVAL YEAR TO MONTH"


def test_day_time_interval(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("a", T.DayTimeIntervalType(0, 0)),
            T.StructField("b", T.DayTimeIntervalType(0, 1)),
            T.StructField("c", T.DayTimeIntervalType(0, 2)),
        ]
    )
    assert schema.toDDL() == "a INTERVAL DAY, b INTERVAL DAY TO HOUR, c INTERVAL DAY TO MINUTE"

    schema = T.StructType(
        [
            T.StructField("d", T.DayTimeIntervalType(0, 3)),
            T.StructField("e", T.DayTimeIntervalType(1, 0)),
            T.StructField("f", T.DayTimeIntervalType(1, 1)),
        ]
    )
    assert schema.toDDL() == "d INTERVAL DAY TO SECOND, e INTERVAL HOUR TO DAY, f INTERVAL HOUR"

    schema = T.StructType(
        [
            T.StructField("g", T.DayTimeIntervalType(1, 2)),
            T.StructField("h", T.DayTimeIntervalType(1, 3)),
            T.StructField("i", T.DayTimeIntervalType(2, 0)),
        ]
    )
    assert schema.toDDL() == "g INTERVAL HOUR TO MINUTE, h INTERVAL HOUR TO SECOND, i INTERVAL MINUTE TO DAY"

    schema = T.StructType(
        [
            T.StructField("j", T.DayTimeIntervalType(2, 1)),
            T.StructField("k", T.DayTimeIntervalType(2, 2)),
            T.StructField("l", T.DayTimeIntervalType(2, 3)),
        ]
    )
    assert schema.toDDL() == "j INTERVAL MINUTE TO HOUR, k INTERVAL MINUTE, l INTERVAL MINUTE TO SECOND"

    schema = T.StructType(
        [
            T.StructField("m", T.DayTimeIntervalType(3, 0)),
            T.StructField("n", T.DayTimeIntervalType(3, 1)),
            T.StructField("o", T.DayTimeIntervalType(3, 2)),
        ]
    )
    assert schema.toDDL() == "m INTERVAL SECOND TO DAY, n INTERVAL SECOND TO HOUR, o INTERVAL SECOND TO MINUTE"

    schema = T.StructType(
        [
            T.StructField("p", T.DayTimeIntervalType(3, 3)),
        ]
    )
    assert schema.toDDL() == "p INTERVAL SECOND"


def test_array(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("a", T.ArrayType(T.StringType())),
            T.StructField("b", T.ArrayType(T.VarcharType(3), False)),
        ]
    )

    assert schema.toDDL() == "a ARRAY<STRING>, b ARRAY<VARCHAR(3)>"


def test_struct(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField(
                "a",
                T.StructType(
                    [
                        T.StructField("f1", T.StringType(), True),
                    ]
                ),
            ),
            T.StructField(
                "b",
                T.StructType(
                    [
                        T.StructField("f2", T.VarcharType(3), True),
                    ]
                ),
            ),
        ]
    )

    assert schema.toDDL() == "a STRUCT<f1: STRING>, b STRUCT<f2: VARCHAR(3)>"


def test_map(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("a", T.MapType(T.StringType(), T.IntegerType(), False)),
            T.StructField("b", T.MapType(T.FloatType(), T.IntegerType(), True)),
        ]
    )

    assert schema.toDDL() == "a MAP<STRING, INT>, b MAP<FLOAT, INT>"


@pytest.mark.skipif(not hasattr(T, "VariantType"), reason="`VariantType` is not available in this Spark version")
def test_variant(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("col", T.VariantType()),
        ]
    )

    assert schema.toDDL() == "col VARIANT"


def test_udt(spark):  # noqa: ARG001
    class UnnamedPythonUDT(T.UserDefinedType):
        @classmethod
        def sqlType(cls):  # noqa: N802
            return T.StringType()

        @classmethod
        def module(cls):
            return "__main__"

    schema = T.StructType(
        [
            T.StructField("col", UnnamedPythonUDT()),
        ]
    )

    assert schema.toDDL() == "col PYTHONUSERDEFINED"


@pytest.mark.xfail(reason="GeometryType not yet implemented in Sail")
def test_geometry(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("col", T.GeometryType(3857)),
        ]
    )

    assert schema.toDDL() == "col GEOMETRY"


@pytest.mark.xfail(reason="GeographyType not yet implemented in Sail")
def test_geography(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("col", T.GeographyType(4326)),
        ]
    )

    assert schema.toDDL() == "col GEOMETRY"


@pytest.mark.skipif(not hasattr(T, "TimeType"), reason="`TimeType` is not available in this Spark version")
def test_time(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("a", T.TimeType(6)),
            T.StructField("b", T.TimeType(3)),
        ]
    )

    assert schema.toDDL() == "a TIME(6), b TIME(3)"


def test_not_null(spark):  # noqa: ARG001
    schema = T.StructType(
        [
            T.StructField("a", T.BooleanType(), True),
            T.StructField("b", T.BooleanType(), False),
        ]
    )
    assert schema.toDDL() == "a BOOLEAN, b BOOLEAN NOT NULL"
