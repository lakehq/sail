import pytest
from pyspark.sql import types as T

def test_null(spark):
    schema = T.StructType(
        [
            T.StructField("null_col1", T.NullType()),
            T.StructField("null_col1", T.NullType()),
        ]
    )
    assert schema.toDDL() == "null_col1 NULL, null_col1 NULL"

def test_binary(spark):
    schema = T.StructType(
        [
            T.StructField("binary_col1", T.BinaryType()),
            T.StructField("binary_col2", T.BinaryType()),
            T.StructField("binary_col3", T.BinaryType()),
        ]
    )
    assert schema.toDDL() == "binary_col1 BINARY, binary_col2 BINARY, binary_col3 BINARY"

def test_boolean(spark):
    schema = T.StructType(
        [
            T.StructField("bool_col1", T.BooleanType()),
            T.StructField("bool_col2", T.BooleanType()),
            T.StructField("bool_col3", T.BooleanType()),
        ]
    )
    assert schema.toDDL() == "bool_col1 BOOLEAN, bool_col2 BOOLEAN, bool_col3 BOOLEAN"


def test_byte(spark):
    schema = T.StructType(
        [
            T.StructField("byte_col1", T.ByteType()),
            T.StructField("byte_col2", T.ByteType()),
            T.StructField("byte_col3", T.ByteType()),
        ]
    )
    assert schema.toDDL() == "byte_col1 TINYINT, byte_col2 TINYINT, byte_col3 TINYINT"


def test_short(spark):
    schema = T.StructType(
        [
            T.StructField("short_col1", T.ShortType()),
            T.StructField("short_col2", T.ShortType()),
            T.StructField("short_col3", T.ShortType()),
        ]
    )
    assert schema.toDDL() == "short_col1 SHORT, short_col2 SHORT, short_col3 SHORT"


def test_integer(spark):
    schema = T.StructType(
        [
            T.StructField("integer_col1", T.IntegerType()),
            T.StructField("integer_col2", T.IntegerType()),
            T.StructField("integer_col3", T.IntegerType()),
        ]
    )
    assert schema.toDDL() == "integer_col1 INT, integer_col2 INT, integer_col3 INT"


def test_long(spark):
    schema = T.StructType(
        [
            T.StructField("long_col1", T.LongType()),
            T.StructField("long_col2", T.LongType()),
            T.StructField("long_col3", T.LongType()),
        ]
    )
    assert schema.toDDL() == "long_col1 BIGINT, long_col2 BIGINT, long_col3 BIGINT"


def test_float(spark):
    schema = T.StructType(
        [
            T.StructField("float_col1", T.FloatType()),
            T.StructField("float_col2", T.FloatType()),
            T.StructField("float_col3", T.FloatType()),
        ]
    )
    assert schema.toDDL() == "float_col1 FLOAT, float_col2 FLOAT, float_col3 FLOAT"


def test_double(spark):
    schema = T.StructType(
        [
            T.StructField("double_col1", T.DoubleType()),
            T.StructField("double_col2", T.DoubleType()),
            T.StructField("double_col3", T.DoubleType()),
        ]
    )
    assert schema.toDDL() == "double_col1 DOUBLE, double_col2 DOUBLE, double_col3 DOUBLE"

def test_decimal(spark):
    schema = T.StructType(
        [
            T.StructField("decimal_col1", T.DecimalType(10, 2)),
            T.StructField("decimal_col2", T.DecimalType(15, 3)),
            T.StructField("decimal_col3", T.DecimalType(20, 4)),
        ]
    )
    assert schema.toDDL() == "decimal_col1 DECIMAL(10,2), decimal_col2 DECIMAL(15,3), decimal_col3 DECIMAL(20,4)"

def test_string(spark):
    schema = T.StructType(
        [
            T.StructField("string_col1", T.StringType()),
            T.StructField("string_col2", T.StringType()),
            T.StructField("string_col3", T.StringType()),
        ]
    )
    assert schema.toDDL() == "string_col1 STRING, string_col2 STRING, string_col3 STRING"


def test_char(spark):
    schema = T.StructType(
        [
            T.StructField("char_col1", T.CharType(1)),
            T.StructField("char_col2", T.CharType(2)),
            T.StructField("char_col3", T.CharType(3)),
        ]
    )
    assert schema.toDDL() == "char_col1 CHAR(1), char_col2 CHAR(2), char_col3 CHAR(3)"


def test_varchar(spark):
    schema = T.StructType(
        [
            T.StructField("varchar_col1", T.VarcharType(1)),
            T.StructField("varchar_col2", T.VarcharType(2)),
            T.StructField("varchar_col3", T.VarcharType(3)),
        ]
    )
    assert schema.toDDL() == "varchar_col1 VARCHAR(1), varchar_col2 VARCHAR(2), varchar_col3 VARCHAR(3)"

def test_date(spark):
    schema = T.StructType(
        [
            T.StructField("date_col1", T.DateType()),
            T.StructField("date_col2", T.DateType()),
            T.StructField("date_col3", T.DateType()),
        ]
    )
    assert schema.toDDL() == "date_col1 DATE, date_col2 DATE, date_col3 DATE"


def test_timestamp(spark):
    schema = T.StructType(
        [
            T.StructField("timestamp_col1", T.TimestampType()),
            T.StructField("date_timestamp_col1", T.TimestampType()),
            T.StructField("date_timestamp_col2", T.TimestampType()),
        ]
    )
    assert schema.toDDL() == "timestamp_col1 TIMESTAMP, date_timestamp_col1 TIMESTAMP, date_timestamp_col2 TIMESTAMP"


def test_timestampntz(spark):
    schema = T.StructType(
        [
            T.StructField("timestampntz_col1", T.TimestampNTZType()),
            T.StructField("timestampntz_col2", T.TimestampNTZType()),
            T.StructField("timestampntz_col3", T.TimestampNTZType()),
        ]
    )
    assert (
        schema.toDDL()
        == "timestampntz_col1 TIMESTAMP_NTZ, timestampntz_col2 TIMESTAMP_NTZ, timestampntz_col3 TIMESTAMP_NTZ"
    )

def test_year_month_interval(spark):
    schema = T.StructType(
        [
            T.StructField("ymi_col1", T.YearMonthIntervalType(0, 0)),
            T.StructField("ymi_col2", T.YearMonthIntervalType(0, 1)),
            T.StructField("ymi_col3", T.YearMonthIntervalType(1, 0)),
        ]
    )
    assert schema.toDDL() == "ymi_col1 INTERVAL YEAR, ymi_col2 INTERVAL YEAR TO MONTH, ymi_col3 INTERVAL MONTH TO YEAR"

    schema = T.StructType(
        [
            T.StructField("ymi_col4", T.YearMonthIntervalType(1, 1)),
            T.StructField("ymi_col5", T.YearMonthIntervalType(startField=0)),
            T.StructField("ymi_col6", T.YearMonthIntervalType(startField=1)),
            T.StructField("ymi_col7", T.YearMonthIntervalType()),
        ]
    )

    assert (
        schema.toDDL()
        == "ymi_col4 INTERVAL MONTH, ymi_col5 INTERVAL YEAR, ymi_col6 INTERVAL MONTH, ymi_col7 INTERVAL YEAR TO MONTH"
    )


def test_day_time_interval(spark):
    schema = T.StructType(
        [
            T.StructField("dti_0_0", T.DayTimeIntervalType(0, 0)),
            T.StructField("dti_0_1", T.DayTimeIntervalType(0, 1)),
            T.StructField("dti_0_2", T.DayTimeIntervalType(0, 2)),
        ]
    )
    assert schema.toDDL() == "dti_0_0 INTERVAL DAY, dti_0_1 INTERVAL DAY TO HOUR, dti_0_2 INTERVAL DAY TO MINUTE"

    schema = T.StructType(
        [
            T.StructField("dti_0_3", T.DayTimeIntervalType(0, 3)),
            T.StructField("dti_1_0", T.DayTimeIntervalType(1, 0)),
            T.StructField("dti_1_1", T.DayTimeIntervalType(1, 1)),
        ]
    )
    assert schema.toDDL() == "dti_0_3 INTERVAL DAY TO SECOND, dti_1_0 INTERVAL HOUR TO DAY, dti_1_1 INTERVAL HOUR"

    schema = T.StructType(
        [
            T.StructField("dti_1_2", T.DayTimeIntervalType(1, 2)),
            T.StructField("dti_1_3", T.DayTimeIntervalType(1, 3)),
            T.StructField("dti_2_0", T.DayTimeIntervalType(2, 0)),
        ]
    )
    assert (
        schema.toDDL()
        == "dti_1_2 INTERVAL HOUR TO MINUTE, dti_1_3 INTERVAL HOUR TO SECOND, dti_2_0 INTERVAL MINUTE TO DAY"
    )

    schema = T.StructType(
        [
            T.StructField("dti_2_1", T.DayTimeIntervalType(2, 1)),
            T.StructField("dti_2_2", T.DayTimeIntervalType(2, 2)),
            T.StructField("dti_2_3", T.DayTimeIntervalType(2, 3)),
        ]
    )
    assert (
        schema.toDDL() == "dti_2_1 INTERVAL MINUTE TO HOUR, dti_2_2 INTERVAL MINUTE, dti_2_3 INTERVAL MINUTE TO SECOND"
    )

    schema = T.StructType(
        [
            T.StructField("dti_3_0", T.DayTimeIntervalType(3, 0)),
            T.StructField("dti_3_1", T.DayTimeIntervalType(3, 1)),
            T.StructField("dti_3_2", T.DayTimeIntervalType(3, 2)),
        ]
    )
    assert (
        schema.toDDL()
        == "dti_3_0 INTERVAL SECOND TO DAY, dti_3_1 INTERVAL SECOND TO HOUR, dti_3_2 INTERVAL SECOND TO MINUTE"
    )

    schema = T.StructType(
        [
            T.StructField("dti_3_3", T.DayTimeIntervalType(3, 3)),
        ]
    )
    assert schema.toDDL() == "dti_3_3 INTERVAL SECOND"


def test_array(spark):
    schema = T.StructType(
        [
            T.StructField("array_col1", T.ArrayType(T.StringType())),
            T.StructField("array_col2", T.ArrayType(T.VarcharType(3), False)),
        ]
    )

    assert schema.toDDL() == "array_col1 ARRAY<STRING>, array_col2 ARRAY<VARCHAR(3)>"


def test_struct(spark):
    schema = T.StructType(
        [
            T.StructField(
                "struct_col1",
                T.StructType(
                    [
                        T.StructField("f1", T.StringType(), True),
                    ]
                ),
            ),
            T.StructField(
                "struct_col2",
                T.StructType(
                    [
                        T.StructField("f2", T.StringType(), True),
                    ]
                ),
            ),
        ]
    )

    assert schema.toDDL() == "struct_col1 STRUCT<f1: STRING>, struct_col2 STRUCT<f2: STRING>"


def test_map(spark):
    schema = T.StructType(
        [
            T.StructField("map_col1", T.MapType(T.StringType(), T.IntegerType(), False)),
            T.StructField("map_col2", T.MapType(T.FloatType(), T.IntegerType(), True)),
        ]
    )

    assert schema.toDDL() == "map_col1 MAP<STRING, INT>, map_col2 MAP<FLOAT, INT>"

def test_variant(spark):
    schema = T.StructType(
        [
            T.StructField("variant_col1", T.VariantType()),
            T.StructField("variant_col2", T.VariantType()),
        ]
    )

    assert schema.toDDL() == "variant_col1 VARIANT, variant_col2 VARIANT"

@pytest.mark.skip(reason="UserDefinedType not yet implemented in PySpark")
def test_udt(spark):
    schema = T.StructType(
        [
            T.StructField("udt_col1", T.UserDefinedType()),
            T.StructField("udt_col2", T.UserDefinedType()),
        ]
    )

    assert schema.toDDL() == "variant_col1 PYTHONUSERDEFINED, variant_col2 PYTHONUSERDEFINED"

@pytest.mark.skip(reason="GeometryType not yet implemented in Sail")
def test_geometry(spark):
    schema = T.StructType(
        [
            T.StructField("geometry_col2", T.GeometryType(3857)),
        ]
    )

    assert schema.toDDL() == "geometry_col1 GEOMETRY, geometry_col2 GEOMETRY"

@pytest.mark.skip(reason="GeographyType not yet implemented in Sail")
def test_geography(spark):
    schema = T.StructType(
        [
            T.StructField("geography_col1", T.GeographyType(4326)),
        ]
    )

    assert schema.toDDL() == "geometry_col1 GEOMETRY, geometry_col2 GEOMETRY"

def test_time(spark):
    schema = T.StructType(
        [
            T.StructField("time_col1", T.TimeType(6)),
        ]
    )

    assert schema.toDDL() == "time_col1 TIME(6)"

def test_not_null(spark):
    schema = T.StructType(
        [
            T.StructField("bool_col1", T.BooleanType(), True),
            T.StructField("bool_col2", T.BooleanType(), False),
        ]
    )
    assert schema.toDDL() == "bool_col1 BOOLEAN, bool_col2 BOOLEAN NOT NULL"
