---
title: Overview
rank: 1
---

# Overview

This page presents an overview of the data types available in Spark.
The name of the data type is shown in **bold font**.

In JVM Spark, these data types correspond directly to Java types or classes.
However, since Sail is powered by Arrow, you may consider this page more of a discussion about "logical" data types. For how these data types are represented in Arrow, you can continue to the [Compatibility](./compatibility.md) guide.

## Numeric Types

- **ByteType** represents 1-byte signed integer numbers. The range is `-128` to `127`.
- **ShortType** represents 2-byte signed integer numbers. The range is `-32768` to `32767`.
- **IntegerType** represents 4-byte signed integer numbers. The range is `-2147483648` to `2147483647`.
- **LongType** represents 8-byte signed integer numbers. The range is `-9223372036854775808` to `9223372036854775807`.
- **FloatType** represents 4-byte single-precision floating-point numbers.
- **DoubleType** represents 8-byte double-precision floating-point numbers.
- **DecimalType** represents arbitrary-precision signed decimal numbers. In JVM Spark, this type is backed internally by `java.math.BigDecimal`, which has an arbitrary-precision integer unscaled value and a 32-bit integer scale.

## String Types

- **StringType** represents character string values.
- **VarcharType(_n_)** represents a variant of **StringType** with a length limit **_n_**. This type is only valid in table schemas but not in functions or operators. Writing would fail if the input exceeds the limit.
- **CharType(_n_)** represents a fixed-length variant of **VarcharType(_n_)**. Reading a column of type **CharType(_n_)** always returns strings of length **_n_**. Comparisons on **CharType(_n_)** columns pad the shorter value to the longer length.

## Binary Type

- **BinaryType** represents byte sequence values.

## Null Type

- **NullType** represents null values.

## Boolean Type

- **BooleanType** represents boolean values.

## Datetime Type

- **DateType** represents calendar dates without a time zone.
- **TimestampType** represents timestamps with local time zone. The time zone is controlled by the `spark.sql.session.timeZone` configuration option. The timestamp has microsecond precision.
- **TimestampNTZType** represents timestamps without time zone. The timestamp has microsecond precision.

In Spark SQL, timestamp types follow a slightly different convention.

- `TIMESTAMP_LTZ` correspond to **TimestampType**.
- `TIMESTAMP_NTZ` correspond to **TimestampNTZType**.
- `TIMESTAMP` is an alias for either `TIMESTAMP_LTZ` (the default) or `TIMESTAMP_NTZ`, depending on the `spark.sql.timestampType` configuration option.

## Interval Types

- **YearMonthIntervalType(_startField_, _endField_)** represents a year-month interval made of a contiguous subset of:

  - **MONTH**, months within years `[0..11]`,
  - **YEAR**, years in the range `[0..178956970]`.

  Valid values for **_startField_** and **_endField_** are 0 (**MONTH**) and 1 (**YEAR**).
  Here are the supported year-month interval types:

  | Year-Month Interval Type                                                      | SQL Type                 |
  | ----------------------------------------------------------------------------- | ------------------------ |
  | **YearMonthIntervalType(YEAR, YEAR)**<br />**YearMonthIntervalType(YEAR)**    | `INTRVAL YEAR`           |
  | **YearMonthIntervalType(YEAR, MONTH)**                                        | `INTERVAL YEAR TO MONTH` |
  | **YearMonthIntervalType(MONTH, MONTH)**<br />**YearMonthIntervalType(MONTH)** | `INTERVAL MONTH`         |

- **DayTimeIntervalType(_startField_, _endField_)** represents a day-time interval made of a contiguous subset of:

  - **SECOND**, seconds within minutes and possibly fractional seconds `[0..59.999999]`,
  - **MINUTE**, minutes within hours `[0..59]`,
  - **HOUR**, hours within days `[0..23]`,
  - **DAY**, days in the range `[0..106751991]`.

  Valid values are 0 (**DAY**), 1 (**HOUR**), 2 (**MINUTE**), 3 (**SECOND**).
  Here are the supported day-time interval types:

  | Day-Time Interval Type                                                       | SQL Type                    |
  | ---------------------------------------------------------------------------- | --------------------------- |
  | **DayTimeIntervalType(DAY, DAY)**<br />**DayTimeIntervalType(DAY)**          | `INTERVAL DAY`              |
  | **DayTimeIntervalType(DAY, HOUR)**                                           | `INTERVAL DAY TO HOUR`      |
  | **DayTimeIntervalType(DAY, MINUTE)**                                         | `INTERVAL DAY TO MINUTE`    |
  | **DayTimeIntervalType(DAY, SECOND)**                                         | `INTERVAL DAY TO SECOND`    |
  | **DayTimeIntervalType(HOUR, HOUR)**<br />**DayTimeIntervalType(HOUR)**       | `INTERVAL HOUR`             |
  | **DayTimeIntervalType(HOUR, MINUTE)**                                        | `INTERVAL HOUR TO MINUTE`   |
  | **DayTimeIntervalType(HOUR, SECOND)**                                        | `INTERVAL HOUR TO SECOND`   |
  | **DayTimeIntervalType(MINUTE, MINUTE)**<br />**DayTimeIntervalType(MINUTE)** | `INTERVAL MINUTE`           |
  | **DayTimeIntervalType(MINUTE, SECOND)**                                      | `INTERVAL MINUTE TO SECOND` |
  | **DayTimeIntervalType(SECOND, SECOND)**<br />**DayTimeIntervalType(SECOND)** | `INTERVAL SECOND`           |

Each field in the interval must have a non-negative value, but an interval can be positive or negative.

## Complex Types

- **ArrayType(_elementType_, _containsNull_)** represents sequences of elements of the same type **_elementType_**. An array type has an element type and a nullability flag **_containsNull_** indicating whether its elements can be null or not.
- **MapType(_keyType_, _valueType_, _valueContainsNull_)** represents sets of key-value pairs. The keys must be of the same type **_keyType_** and cannot be null. The values must be of the same type **_valueType_**, and a value nullability flag **_valueContainsNull_** indicates whether values can be null or not.
- **StructType(_fields_)** represents values with a structure described by a sequence of **_fields_**. Each field **StructField(_name_, _dataType_, _nullable_)** has a name, a data type, and a nullability flag indicating whether the field can be null or not.
