---
title: Supported Features
rank: 1
---

# Supported Features

This page presents a high-level overview of supported Spark SQL features.

## Literals

Sail supports all the Spark SQL literal syntax. These literal types include:

- String Literal
- Binary Literal
- Null Literal
- Boolean Literal
- Numeric Literal
- Datetime Literal
- Interval Literal

### String Literal

A string literal specifies a character string value.

**Syntax**

```
[ r ] { 'char [ ... ]' | "char [ ... ]" }
```

**Parameters**

- **char**

  A character drawn from the supported character set.

  - Use `\` to escape special characters.
  - To represent Unicode characters, use \uXXXX or \Uxxxxxxxx, where XXXX and xxxxxxxx are 16-bit and 32-bit code
    points in hexadecimal respectively (e.g., \u03C0 → π, \U0001F44B → 👋).
  - An ASCII character can also be represented as an octal number preceded by `\`, like `\142` → `b`.

- **r**:

  Case insensitive, indicates`RAW`. If a string literal starts with`r`prefix, neither special characters nor unicode
  characters are escaped by`\`.

The following escape sequences are recognized in regular string literals (without the r prefix), and replaced according
to the following rules:

- `\0`->`\u0000`, unicode character with the code 0;
- `\b`->`\u0008`, backspace;
- `\n`->`\u000a`, linefeed;
- `\r`->`\u000d`, carriage return;
- `\t`->`\u0009`, horizontal tab;
- `\Z`->`\u001A`, substitute;
- `\%`->`\%`;
- `\_`->`\_`;
- `\<other char>`->`<other char>`, skip the slash and leave the character as is.

**Examples**

```sql
SELECT 'Hello, World!' AS col;
+-------------+
|          col|
+-------------+
|Hello, World!|
+-------------+

SELECT "Sail SQL" AS col;
+--------+
|    col |
+--------+
|Sail SQL|
+--------+

SELECT 'it\' s about $25.' AS col;
+----------------+
|             col|
+----------------+
|it's about $25. |
+----------------+

SELECT r "'\n' represents newline character." AS col;
+----------------------------------+
|                               col|
+----------------------------------+
|'\n' represents newline character.|
+----------------------------------+
```

### **Binary Literal**

A binary literal is used to specify a byte sequence value.

**Syntax**

```sql
X
{ 'num [ ... ]' | "num [ ... ]" }
```

**Parameters**

- **num**

  Any hexadecimal number from 0 to F.

**Example**

```sql
SELECT X'123456' AS col;
+----------+
|       col|
+----------+
|[12 34 56]|
+----------+
```

### Null Literal

A null literal represents the SQL null value.

**Syntax**

```sql
NULL
```

**Example**

```sql
SELECT NULL AS col;
+----+
| col|
+----+
|NULL|
+----+
```

### Boolean Literal

A boolean literal specifies a Boolean truth value.

**Syntax**

```
TRUE | FALSE
```

**Examples**

```sql
SELECT TRUE AS col;
+-----+
| col |
+-----+
|true |
+-----+

SELECT FALSE AS col;
+------+
|  col |
+------+
|false |
+------+
```

### Numeric Literal

Numeric literals express fixed-point or floating-point numbers. There are two kinds of numeric literals: **integral
literal** and **fractional literal.**

**Integral Literal Syntax**

```sql
[ + | - ] digit [ ... ] [ L | S | Y ]
```

- **digit**

  Any numeral from 0 to 9.

- **L**

  Case insensitive, indicates`BIGINT`, which is an 8-byte signed integer number.

- **S**

  Case insensitive, indicates`SMALLINT`, which is a 2-byte signed integer number.

- **Y**

  Case insensitive, indicates`TINYINT`, which is a 1-byte signed integer number.

- **default (no postfix)**

  Indicates a 4-byte signed integer number.

**Integral Literal Examples**

```sql
SELECT -2147483600 AS col;
+-----------+
|        col|
+-----------+
|-2147483600|
+-----------+

SELECT 9223372036854775807L AS col;
+-------------------+
|                col|
+-------------------+
|9223372036854775807|
+-------------------+

SELECT -64Y AS col;
+----+
| col|
+----+
|-64 |
+----+

SELECT 512S AS col;
+----+
|col |
+----+
|512 |
+----+
```

**Fractional Literals Syntax**

Decimal literals

```sql
decimal_digits
{ [ BD ] | [ exponent BD ] } | digit [ ... ] [ exponent ] BD
```

Double literals

```sql
decimal_digits
{ D | exponent [ D ] }  | digit [ ... ] { exponent [ D ] | [ exponent ] D }
```

Float literals

```sql
decimal_digits
{ F | exponent [ F ] }  | digit [ ... ] { exponent [ F ] | [ exponent ] F }
```

Where `decimal_digits` is defined as:

```sql
[ + | - ] { digit [ ... ] . [ digit [ ... ] ] | . digit [ ... ] }
```

and `exponent` is defined as:

```sql
E
[ + | - ] digit [ ... ]
```

**Fractional Literals Parameters**

- **digit**

  Any numeral from 0 to 9.

- **D**

  Case insensitive, indicates DOUBLE, which is an 8-byte double-precision floating point number.

- **F**

  Case insensitive, indicates FLOAT, which is a 4-byte single-precision floating point number.

- **BD**

  Case insensitive, indicates DECIMAL, with the total number of digits as precision and the number of digits to right of
  decimal point as scale.

**Fractional Literals Examples**

```sql
SELECT 76.543 AS col, TYPEOF(76.543) AS type;
+-------+------------+
|    col|        type|
+-------+------------+
| 76.543|decimal(5,3)|
+-------+------------+

SELECT 8.21E1 AS col, TYPEOF(8.21E1) AS type;
+------+------+
|   col|  type|
+------+------+
| 82.1 |double|
+------+------+

SELECT -0.4321 AS col;
+--------+
|     col|
+--------+
|-0.4321 |
+--------+

SELECT 250.BD AS col;
+-----+
| col |
+-----+
|250  |
+-----+

SELECT 6.9D AS col;
+-----+
| col |
+-----+
| 6.9 |
+-----+

SELECT -18BD AS col;
+----+
|col |
+----+
|-18 |
+----+

SELECT .789E3F AS col;
+------+
| col  |
+------+
|789.0 |
+------+
```

### Datetime Literal

Datetime literals capture specific dates or timestamps.

**Date Syntax**

```sql
DATE { 'yyyy' |
       'yyyy-[m]m' |
       'yyyy-[m]m-[d]d' |
       'yyyy-[m]m-[d]d[T]' }

```

Note: If month or day is not specified, defaults to 01.

**Date Examples**

```sql
SELECT DATE '2001' AS col;
+----------+
|       col|
+----------+
|2001-01-01|
+----------+

SELECT DATE '2005-07' AS col;
+----------+
|       col|
+----------+
|2005-07-01|
+----------+

SELECT DATE '2019-12-25' AS col;
+----------+
|       col|
+----------+
|2019-12-25|
+----------+

```

### Timestamp Syntax

```sql
TIMESTAMP { 'yyyy' |
            'yyyy-[m]m' |
            'yyyy-[m]m-[d]d' |
            'yyyy-[m]m-[d]d ' |
            'yyyy-[m]m-[d]d[T][h]h[:]' |
            'yyyy-[m]m-[d]d[T][h]h:[m]m[:]' |
            'yyyy-[m]m-[d]d[T][h]h:[m]m:[s]s[.]' |
            'yyyy-[m]m-[d]d[T][h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]'}
```

**Note**: defaults to 00 if hour, minute or second is not specified. `zone_id` should have one of the forms:

- Z - Zulu time zone UTC+0
- +|-[h]h:[m]m
- An id with one of the prefixes UTC+, UTC-, GMT+, GMT-, UT+ or UT-, and a suffix in the formats:
  - +|-h[h]
  - +|-hh[:]mm
  - +|-hh:mm:ss
  - +|-hhmmss
- Region-based zone IDs in the form area/city, such as Europe/Paris

**Note**: defaults to the session local timezone (set via `spark.sql.session.timeZone`) if `zone_id` is not specified.

**Timestamp Examples**

```sql
SELECT TIMESTAMP '2000-02-29 23:59:59.123' AS col;
+-----------------------+
|                    col|
+-----------------------+
|2000-02-29 23:59:59.123|
+-----------------------+

SELECT TIMESTAMP '2015-06-30 12:00:00.999999UTC-05:00' AS col;
+--------------------------+
|                      col |
+--------------------------+
|2015-06-30 17:00:00.999999|
+--------------------------+

SELECT TIMESTAMP '2010-08' AS col;
+-------------------+
|                col|
+-------------------+
|2010-08-01 00:00:00|
+-------------------+
```

### Interval Literal

Interval literals denote fixed spans of time.

Two syntaxes are recognized: **ANSI** and **multi-unit**.

**ANSI Syntax**

The ANSI SQL standard defines interval literals in the form:

```sql
INTERVAL [ <sign> ] <interval string> <interval qualifier>
```

where `<interval qualifier>` can be a single field or in the field-to-field form:

```sql
<interval qualifier> ::= <start field> TO <
end field
> | <single field>
```

The field name is case-insensitive, and can be one of `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE` and `SECOND`.

An interval literal can have either year-month or day-time interval type. The interval sub-type defines the format of
`<interval string>`:

```sql
<interval string> ::= <quote> [ <sign> ] { <year-month literal> | <day-time literal> } <quote>
<year-month literal> ::= <years value> [ <minus sign> <months value> ] | <months value>
<day-time literal> ::= <day-time interval> | <time interval>
<day-time interval> ::= <days value> [ <space> <hours value> [ <colon> <minutes value> [ <colon> <seconds value> ] ] ]
<time interval> ::= <hours value> [ <colon> <minutes value> [ <colon> <seconds value> ] ]
  | <minutes value> [ <colon> <seconds value> ]
  | <seconds value>
```

Supported year-month interval literals and their formats:

| Qualifier     | String Pattern      | Example                             |
| ------------- | ------------------- | ----------------------------------- |
| YEAR          | `[+\|-]'[+\|-]y'`   | `INTERVAL -'1999' YEAR`             |
| YEAR TO MONTH | `[+\|-]'[+\|-]y-m'` | `INTERVAL '-1999-11' YEAR TO MONTH` |
| MONTH         | `[+\|-]'[+\|-]m'`   | `INTERVAL '24' MONTH`               |

Supported day-time interval formats:

| Qualifier        | String Pattern            | Example                                        |
| ---------------- | ------------------------- | ---------------------------------------------- |
| DAY              | `[+\|-]'[+\|-]d'`         | `INTERVAL -'365' DAY`                          |
| DAY TO HOUR      | `[+\|-]'[+\|-]d h'`       | `INTERVAL '-10 05' DAY TO HOUR`                |
| DAY TO MINUTE    | `[+\|-]'[+\|-]d h:m'`     | `INTERVAL '100 10:30' DAY TO MINUTE`           |
| DAY TO SECOND    | `[+\|-]'[+\|-]d h:m:s.n'` | `INTERVAL '100 10:30:40.999999' DAY TO SECOND` |
| HOUR             | `[+\|-]'[+\|-]h'`         | `INTERVAL '123' HOUR`                          |
| HOUR TO MINUTE   | `[+\|-]'[+\|-]h:m'`       | `INTERVAL -'-15:45' HOUR TO MINUTE`            |
| HOUR TO SECOND   | `[+\|-]'[+\|-]h:m:s.n'`   | `INTERVAL '123:10:59' HOUR TO SECOND`          |
| MINUTE           | `[+\|-]'[+\|-]m'`         | `INTERVAL '5000' MINUTE`                       |
| MINUTE TO SECOND | `[+\|-]'[+\|-]m:s.n'`     | `INTERVAL '2000:02.002' MINUTE TO SECOND`      |
| SECOND           | `[+\|-]'[+\|-]s.n'`       | `INTERVAL '2000.000002' SECOND`                |

**ANSI Examples**

```sql
SELECT INTERVAL '5-6' YEAR TO MONTH AS col;
+----------------------------+
|col                         |
+----------------------------+
|INTERVAL '5-6' YEAR TO MONTH|
+----------------------------+

SELECT INTERVAL -'12 23:45:59.888888' DAY TO SECOND AS col;
+--------------------------------------------+
|col                                         |
+--------------------------------------------+
|INTERVAL '-12 23:45:59.888888' DAY TO SECOND|
+--------------------------------------------+

```

**Multi-Unit Syntax**

```sql
INTERVAL interval_value interval_unit [ interval_value interval_unit ... ] |
INTERVAL 'interval_value interval_unit [ interval_value interval_unit ... ]' |
```

**Multi-units Parameters**

- **interval_value**

  **Syntax:**

  ```sql
  [ + | - ] number_value | '[ + | - ] number_value'
  ```

- **interval_unit**

  **Syntax:**

  ```sql
  YEAR[S] | MONTH[S] | WEEK[S] | DAY[S] | HOUR[S] | MINUTE[S] | SECOND[S] |
  MILLISECOND[S] | MICROSECOND[S]

  Mix of the YEAR[S] or MONTH[S] interval units with other units is not allowed.
  ```

**Note**: Although Sail supports `YEAR[S]` and `MONTH[S]` interval units in multi-unit syntax, the Spark client is unable to convert these from Arrow when invoking `.collect` or `.toPandas`.

**Multi-units Examples**

```sql
SELECT INTERVAL 3 WEEK AS col;
+-----------------+
|col              |
+-----------------+
|INTERVAL '21' DAY|
+-----------------+

SELECT INTERVAL -2 WEEKS '3' DAYS AS col;
+------------------+
|col               |
+------------------+
|INTERVAL '-11' DAY|
+------------------+

SELECT INTERVAL '3 DAYS 50 SECONDS' AS col;
+-----------------------------------+
|col                                |
+-----------------------------------+
|INTERVAL '3 00:00:50' DAY TO SECOND|
+-----------------------------------+

SELECT INTERVAL 3 WEEK 4 DAYS 5 HOUR 6 MINUTES 7 SECOND 8 MILLISECOND 9 MICROSECONDS AS col;
+-------------------------------------------+
|col                                        |
+-------------------------------------------+
|INTERVAL '25 05:06:07.008009' DAY TO SECOND|
+-------------------------------------------+
```

## Data Types

Sail supports all Spark SQL data types except the `VARIANT` type introduced in Spark 4.0. Support for the `VARIANT` type is tracked in the [GitHub issue](https://github.com/lakehq/sail/issues/511).

### Supported Data Types

**Numeric types**

- `ByteType`: Represents 1-byte signed integer numbers. The range is `-128` to `127`.
- `ShortType`: Represents 2-byte signed integer numbers. The range is `-32768` to `32767`.
- `IntegerType`: Represents 4-byte signed integer numbers. The range is `-2147483648` to `2147483647`.
- `LongType`: Represents 8-byte signed integer numbers. The range is `-9223372036854775808` to `9223372036854775807`.
- `FloatType`: Represents 4-byte single-precision floating-point numbers.
- `DoubleType`: Represents 8-byte double-precision floating-point numbers.
- `DecimalType`: Represents arbitrary-precision signed decimal numbers. Backed internally by `java.math.BigDecimal`. A `BigDecimal` has an arbitrary-precision integer unscaled value and a 32-bit integer scale.

**String type**

- `StringType`: Represents character string values.
- `VarcharType(length)`: A variant of `StringType` with a length limit. Writes fail if the input exceeds the limit. Note: this type is only valid in table schemas, not in functions or operators.
- `CharType(length)`: A fixed-length variant of `VarcharType(length)`. Reading a column of type `CharType(n)` always returns strings of length `n`. Comparisons on `CharType` columns pad the shorter value to the longer length.

**Binary type**

- `BinaryType`: Represents byte sequence values.

**Boolean type**

- `BooleanType`: Represents boolean values.

**Datetime type**

- `DateType`: Represents calendar dates with year, month, and day fields, without a time zone.
- `TimestampType`: Timestamp with local time zone (`TIMESTAMP_LTZ`). Represents year, month, day, hour, minute, and second, interpreted with the session’s local time zone. The value denotes an absolute point in time.
- `TimestampNTZType`: Timestamp without time zone (`TIMESTAMP_NTZ`). Represents year, month, day, hour, minute, and second. Operations do not consider time zones.

  - Note: `TIMESTAMP` in Spark is a user-configurable alias for either `TIMESTAMP_LTZ` (default) or `TIMESTAMP_NTZ`, controlled by `spark.sql.timestampType`.

**Interval types**

- `YearMonthIntervalType(startField, endField)`: Represents a year-month interval made of a contiguous subset of:
  - `MONTH`, months within years `[0..11]`,
  - `YEAR`, years in the range `[0..178956970]`.
    Individual fields are non-negative, but an interval can be positive or negative.
    `startField` is the leftmost field and `endField` is the rightmost field. Valid values for `startField` and `endField` are 0 (`MONTH`) and 1 (`YEAR`). Supported year-month interval types are:
    | Year-Month Interval Type | SQL type | An instance of the type |
    | ----------------------------------------------------------------------- | ---------------------- | ---------------------------------- |
    | `YearMonthIntervalType(YEAR, YEAR)` or `YearMonthIntervalType(YEAR)` | INTERVAL YEAR | `INTERVAL '2025' YEAR` |
    | `YearMonthIntervalType(YEAR, MONTH)` | INTERVAL YEAR TO MONTH | `INTERVAL '2025-09' YEAR TO MONTH` |
    | `YearMonthIntervalType(MONTH, MONTH)` or `YearMonthIntervalType(MONTH)` | INTERVAL MONTH | `INTERVAL '09' MONTH` |
- `DayTimeIntervalType(startField, endField)`: Represents a day-time interval made of a contiguous subset of:
  - `SECOND`, seconds within minutes and possibly fractional seconds `[0..59.999999]`,
  - `MINUTE`, minutes within hours `[0..59]`,
  - `HOUR`, hours within days `[0..23]`,
  - `DAY`, days in the range `[0..106751991]`.
    Individual fields are non-negative, but an interval can be positive or negative.
    `startField` is the leftmost field and `endField` is the rightmost field. Valid values are 0 (`DAY`), 1 (`HOUR`), 2 (`MINUTE`), 3 (`SECOND`). Supported day-time interval types are:
    | Day-Time Interval Type | SQL type | An instance of the type |
    | ---------------------------------------------------------------------- | ------------------------- | ---------------------------------------------- |
    | `DayTimeIntervalType(DAY, DAY)` or `DayTimeIntervalType(DAY)` | INTERVAL DAY | `INTERVAL '100' DAY` |
    | `DayTimeIntervalType(DAY, HOUR)` | INTERVAL DAY TO HOUR | `INTERVAL '100 10' DAY TO HOUR` |
    | `DayTimeIntervalType(DAY, MINUTE)` | INTERVAL DAY TO MINUTE | `INTERVAL '100 10:30' DAY TO MINUTE` |
    | `DayTimeIntervalType(DAY, SECOND)` | INTERVAL DAY TO SECOND | `INTERVAL '100 10:30:40.999999' DAY TO SECOND` |
    | `DayTimeIntervalType(HOUR, HOUR)` or `DayTimeIntervalType(HOUR)` | INTERVAL HOUR | `INTERVAL '123' HOUR` |
    | `DayTimeIntervalType(HOUR, MINUTE)` | INTERVAL HOUR TO MINUTE | `INTERVAL '123:10' HOUR TO MINUTE` |
    | `DayTimeIntervalType(HOUR, SECOND)` | INTERVAL HOUR TO SECOND | `INTERVAL '123:10:59' HOUR TO SECOND` |
    | `DayTimeIntervalType(MINUTE, MINUTE)` or `DayTimeIntervalType(MINUTE)` | INTERVAL MINUTE | `INTERVAL '1000' MINUTE` |
    | `DayTimeIntervalType(MINUTE, SECOND)` | INTERVAL MINUTE TO SECOND | `INTERVAL '1000:01.001' MINUTE TO SECOND` |
    | `DayTimeIntervalType(SECOND, SECOND)` or `DayTimeIntervalType(SECOND)` | INTERVAL SECOND | `INTERVAL '1000.000001' SECOND` |

**Complex types**

- `ArrayType(elementType, containsNull)`: Represents sequences of elements of type `elementType`. `containsNull` indicates whether elements may be `null`.
- `MapType(keyType, valueType, valueContainsNull)`: Represents key-value mappings. Keys have type `keyType` and cannot be `null`. Values have type `valueType`. `valueContainsNull` indicates whether values may be `null`.
- `StructType(fields)`: Represents values with a structure described by a sequence of `StructField`s (`fields`).
  - `StructField(name, dataType, nullable)`: A field in a `StructType`. `name` gives the field name. `dataType` gives the field’s type. `nullable` indicates whether field values may be `null`.

All data types of Spark SQL are located in the package of `pyspark.sql.types`. You can access them by doing:

```sql
from pyspark.sql.types import *
```

**Python**
| Data type | Value type in Python | API to access or create a data type |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| **ByteType** | int<br>**Note:** Numbers will be converted to 1-byte signed integer numbers at runtime. Please make sure that numbers are within the range of -128 to 127. | ByteType() |
| **ShortType** | int<br>**Note:** Numbers will be converted to 2-byte signed integer numbers at runtime. Please make sure that numbers are within the range of -32768 to 32767. | ShortType() |
| **IntegerType** | int | IntegerType() |
| **LongType** | int<br>**Note:** Numbers will be converted to 8-byte signed integer numbers at runtime. Please make sure that numbers are within the range of -9223372036854775808 to 9223372036854775807. Otherwise, please convert data to `decimal.Decimal` and use `DecimalType`. | LongType() |
| **FloatType** | float<br>**Note:** Numbers will be converted to 4-byte single-precision floating-point numbers at runtime. | FloatType() |
| **DoubleType** | float | DoubleType() |
| **DecimalType** | decimal.Decimal | DecimalType() |
| **StringType** | str | StringType() |
| **CharType(length)** | str | CharType(length) |
| **VarcharType(length)** | str | VarcharType(length) |
| **BinaryType** | bytearray | BinaryType() |
| **BooleanType** | bool | BooleanType() |
| **TimestampType** | datetime.datetime | TimestampType() |
| **TimestampNTZType** | datetime.datetime | TimestampNTZType() |
| **DateType** | datetime.date | DateType() |
| **DayTimeIntervalType** | datetime.timedelta | DayTimeIntervalType() |
| **ArrayType** | list, tuple, or array | ArrayType(elementType, [*containsNull*])<br>**Note:** The default value of _containsNull_ is True. |
| **MapType** | dict | MapType(keyType, valueType, [*valueContainsNull*])<br>**Note:** The default value of _valueContainsNull_ is True. |
| **StructType**| list or tuple | StructType(_fields_)<br>**Note:** _fields_ is a Seq of StructFields. Also, two fields with the same name are not allowed. |
| **StructField** | The value type in Python of the data type of this field (for example, int for a StructField with the data type IntegerType) | StructField(_name_, _dataType_, [*nullable*])<br>**Note:** The default value of _nullable_ is True. |

**SQL**
| Data type | SQL name |
| --- | --- |
| **BooleanType** | BOOLEAN |
| **ByteType** | BYTE, TINYINT |
| **ShortType** | SHORT, SMALLINT |
| **IntegerType** | INT, INTEGER |
| **LongType** | LONG, BIGINT |
| **FloatType** | FLOAT, REAL |
| **DoubleType** | DOUBLE |
| **DateType** | DATE |
| **TimestampType** | TIMESTAMP, TIMESTAMP_LTZ |
| **TimestampNTZType** | TIMESTAMP_NTZ |
| **StringType** | STRING |
| **CharType(length)** | CHAR(length) |
| **VarcharType(length)** | VARCHAR(length) |
| **BinaryType** | BINARY |
| **DecimalType** | DECIMAL, DEC, NUMERIC |
| **YearMonthIntervalType** | INTERVAL YEAR, INTERVAL YEAR TO MONTH, INTERVAL MONTH |
| **DayTimeIntervalType** | INTERVAL DAY, INTERVAL DAY TO HOUR, INTERVAL DAY TO MINUTE, INTERVAL DAY TO SECOND, INTERVAL HOUR, INTERVAL HOUR TO MINUTE, INTERVAL HOUR TO SECOND, INTERVAL MINUTE, INTERVAL MINUTE TO SECOND, INTERVAL SECOND |
| **ArrayType** | ARRAY<element_type> |
| **StructType** | STRUCT<field1_name: field1_type, field2_name: field2_type, …><br>**Note:** ‘:’ is optional. |
| **MapType** | MAP<key_type, value_type> |

### Sail SQL Parser Extensions and Aliases

Sail’s SQL parser accepts several additional type names and unsigned variants that do not exist in Spark’s parser. These parse successfully in Sail and are mapped to Arrow-backed internal types. When interoperating with Spark (e.g., writing to a Spark-managed catalog), Sail will serialize to the nearest compatible Spark type where possible.

| Data type (parsed name)     | Canonical Sail type                                  | Spark equivalent when serialized              | Notes                                                                                  |
| --------------------------- | ---------------------------------------------------- | --------------------------------------------- | -------------------------------------------------------------------------------------- |
| **BOOL**                    | **BooleanType**                                      | **BOOLEAN**                                   | Alias; Spark parser does not accept **BOOL** as a keyword.                             |
| **INT8**                    | **ByteType**                                         | **TINYINT**                                   | Alias.                                                                                 |
| **INT16**                   | **ShortType**                                        | **SMALLINT**                                  | Alias.                                                                                 |
| **INT32**                   | **IntegerType**                                      | **INT**                                       | Alias.                                                                                 |
| **INT64**                   | **LongType**                                         | **BIGINT**                                    | Alias.                                                                                 |
| **UINT8**                   | Arrow **UInt8**                                      | _none_ → may widen to **SMALLINT**            | Unsigned integer; no native Spark type. May down-map or be rejected depending on sink. |
| **UINT16**                  | Arrow **UInt16**                                     | _none_ → may widen to **INT**                 | Unsigned integer; same caveat.                                                         |
| **UINT32**                  | Arrow **UInt32**                                     | _none_ → may widen to **BIGINT**              | Unsigned integer; same caveat.                                                         |
| **UINT64**                  | Arrow **UInt64**                                     | _none_                                        | Unsigned integer; usually requires explicit cast.                                      |
| **FLOAT32**                 | **FloatType**                                        | **FLOAT**                                     | Alias.                                                                                 |
| **FLOAT64**                 | **DoubleType**                                       | **DOUBLE**                                    | Alias.                                                                                 |
| **BYTEA**                   | **BinaryType**                                       | **BINARY**                                    | PostgreSQL-style alias.                                                                |
| **TEXT**                    | Large UTF-8 string                                   | **STRING**                                    | No length cap; surfaced as **STRING** to Spark.                                        |
| **DATE64**                  | Arrow **Date64**                                     | **DATE**                                      | Millisecond resolution internally.                                                     |
| **TIMESTAMP(p)**            | **TimestampType** (precision p)                      | **TIMESTAMP**                                 | Precision preserved by Sail; Spark ignores precision in syntax.                        |
| **TIMESTAMP_NTZ(p)**        | **TimestampNTZType** (precision p)                   | **TIMESTAMP_NTZ**                             | Precision accepted.                                                                    |
| **TIMESTAMP_LTZ(p)**        | **TimestampType** with local time zone (precision p) | **TIMESTAMP**, **TIMESTAMP_LTZ**              | Precision accepted.                                                                    |
| **INTERVAL MONTH_DAY_NANO** | Arrow **Month-Day-Nano**                             | nearest Spark Year-Month or Day-Time interval | Serialized to the closest supported Spark interval or requires explicit cast.          |

**Tip:** If a table must be readable by Spark without changes, prefer the standard Spark types above or cast Sail-only inputs to their Spark equivalents.

### Floating Point Special Values

LakeSail preserves Spark’s special floating-point values when mapping to Apache Arrow types.

- **Infinity**
  - Spark literals: `Inf`, `+Inf`, `Infinity`, `+Infinity` (case-insensitive)
  - Arrow mapping:
    - `FloatType` → Arrow `Float32` with `+∞` (`f32::INFINITY`, `float("inf")`)
    - `DoubleType` → Arrow `Float64` with `+∞` (`f64::INFINITY`, `float("inf")`)
- **Negative Infinity**
  - Spark literals: `Inf`, `Infinity`
  - Arrow mapping:
    - `FloatType` → Arrow `Float32` with `∞` (`f32::NEG_INFINITY`)
    - `DoubleType` → Arrow `Float64` with `∞` (`f64::NEG_INFINITY`)
- **Not a Number (NaN)**
  - Spark literal: `NaN`
  - Arrow mapping:
    - `FloatType` → Arrow `Float32` with NaN (`f32::NAN`)
    - `DoubleType` → Arrow `Float64` with NaN (`f64::NAN`)

Arrow itself does not define named constants for these values; they are represented using the floating-point semantics of the host language (e.g., `f64::INFINITY` in Rust).

**Positive and Negative Infinity Semantics**

- Positive infinity multiplied by any positive value returns positive infinity.
- Negative infinity multiplied by any positive value returns negative infinity.
- Positive infinity multiplied by any negative value returns negative infinity.
- Negative infinity multiplied by any negative value returns positive infinity.
- Positive/negative infinity multiplied by 0 returns NaN.
- Positive/negative infinity is equal to itself.
- In aggregations, all positive infinity values are grouped together. Similarly, all negative infinity values are grouped together.
- Positive infinity and negative infinity are treated as normal values in join keys.
- Positive infinity sorts lower than NaN and higher than any other values.
- Negative infinity sorts lower than any other values.

**NaN Semantics**

The handling of `NaN` for `float` or `double` differs from standard IEEE semantics in specific ways:

- NaN = NaN returns true.
- In aggregations, all NaN values are grouped together.
- NaN is treated as a normal value in join keys.
- NaN values go last when in ascending order, larger than any other numeric value.

**Examples**

```sql
SELECT double('infinity') AS col;
+--------+
|     col|
+--------+
|Infinity|
+--------+

SELECT float('-inf') AS col;
+---------+
|      col|
+---------+
|-Infinity|
+---------+

SELECT float('NaN') AS col;
+---+
|col|
+---+
|NaN|
+---+

SELECT double('infinity') * 0 AS col;
+---+
|col|
+---+
|NaN|
+---+

SELECT double('-infinity') * (-1234567) AS col;
+--------+
|     col|
+--------+
|Infinity|
+--------+

SELECT double('infinity') < double('NaN') AS col;
+----+
| col|
+----+
|true|
+----+

SELECT double('NaN') = double('NaN') AS col;
+----+
| col|
+----+
|true|
+----+

SELECT double('inf') = double('infinity') AS col;
+----+
| col|
+----+
|true|
+----+

CREATE TABLE test (c1 int, c2 double);
INSERT INTO test VALUES
  (1, double('infinity')),
  (2, double('infinity')),
  (3, double('inf')),
  (4, double('-inf')),
  (5, double('NaN')),
  (6, double('NaN')),
  (7, double('-infinity'))
;
SELECT COUNT(*), c2
FROM test
GROUP BY c2
ORDER BY c2;
+---------+---------+
| count(1)|       c2|
+---------+---------+
|        2|-Infinity|
|        3| Infinity|
|        2|      NaN|
+---------+---------+
```

## Expressions

Sail supports most Spark SQL expression syntax, including unary and binary operators, predicates, `CASE` clause etc.

Sail supports most common Spark SQL functions. The effort to reach full function parity with Spark is tracked in
the [GitHub issue](https://github.com/lakehq/sail/issues/398).

## Statements

### Data Retrieval (Query)

The following table lists the supported clauses in the `SELECT` statement.

| Clause                            | Supported          |
| --------------------------------- | ------------------ |
| `FROM <relation>`                 | :white_check_mark: |
| `FROM <format>.<path>` (files)    | :construction:     |
| `WHERE`                           | :white_check_mark: |
| `GROUP BY`                        | :white_check_mark: |
| `HAVING`                          | :white_check_mark: |
| `ORDER BY`                        | :white_check_mark: |
| `OFFSET`                          | :white_check_mark: |
| `LIMIT`                           | :white_check_mark: |
| `JOIN`                            | :white_check_mark: |
| `UNION`                           | :white_check_mark: |
| `INTERSECT`                       | :white_check_mark: |
| `EXCEPT` / `MINUS`                | :white_check_mark: |
| `WITH` (common table expressions) | :white_check_mark: |
| `VALUES` (inline tables)          | :white_check_mark: |
| `OVER <window>`                   | :white_check_mark: |
| `/*+ ... */` (hints)              | :construction:     |
| `CLUSTER BY`                      | :construction:     |
| `DISTRIBUTE BY`                   | :construction:     |
| `SORT BY`                         | :construction:     |
| `PIVOT`                           | :construction:     |
| `UNPIVOT`                         | :construction:     |
| `LATERAL VIEW`                    | :white_check_mark: |
| `LATERAL <subquery>`              | :construction:     |
| `TABLESAMPLE`                     | :construction:     |
| `TRANSFORM`                       | :construction:     |

The `EXPLAIN` statement is also supported, but the output shows the Sail logical and physical plan.

The `DESCRIBE QUERY` statement is not supported yet.

### Data Manipulation

| Statement                           | Supported          |
| ----------------------------------- | ------------------ |
| `INSERT INTO <table>`               | :white_check_mark: |
| `INSERT OVERWRITE <table>`          | :construction:     |
| `INSERT OVERWRITE DIRECTORY <path>` | :construction:     |
| `LOAD DATA`                         | :construction:     |
| `COPY INTO`                         | :construction:     |
| `MERGE INTO`                        | :construction:     |
| `UPDATE`                            | :construction:     |
| `DELETE FROM`                       | :construction:     |

The `COPY INTO`, `MERGE INTO`, `UPDATE`, and `DELETE FROM` statements are not core Spark features.
But some extensions support these statements for lakehouse tables (e.g., Delta Lake).

### Catalog Management

| Statement               | Supported                    |
| ----------------------- | ---------------------------- |
| `ALTER DATABASE`        | :construction:               |
| `ALTER TABLE`           | :construction:               |
| `ALTER VIEW`            | :construction:               |
| `ANALYZE TABLE`         | :construction:               |
| `CREATE DATABASE`       | :white_check_mark:           |
| `CREATE FUNCTION`       | :construction:               |
| `CREATE TABLE`          | :white_check_mark: (partial) |
| `CREATE TEMPORARY VIEW` | :white_check_mark:           |
| `CREATE VIEW`           | :construction:               |
| `DESCRIBE DATABASE`     | :construction:               |
| `DESCRIBE FUNCTION`     | :construction:               |
| `DESCRIBE TABLE`        | :construction:               |
| `DROP DATABASE`         | :white_check_mark:           |
| `DROP FUNCTION`         | :construction:               |
| `DROP TABLE`            | :white_check_mark:           |
| `DROP VIEW`             | :white_check_mark:           |
| `REFRESH <path>`        | :construction:               |
| `REFRESH FUNCTION`      | :construction:               |
| `REFRESH TABLE`         | :construction:               |
| `REPAIR TABLE`          | :construction:               |
| `SHOW COLUMNS`          | :white_check_mark:           |
| `SHOW CREATE TABLE`     | :construction:               |
| `SHOW DATABASES`        | :white_check_mark:           |
| `SHOW FUNCTIONS`        | :construction:               |
| `SHOW PARTITIONS`       | :construction:               |
| `SHOW TABLE`            | :construction:               |
| `SHOW TABLES`           | :white_check_mark:           |
| `SHOW TBLPROPERTIES`    | :construction:               |
| `SHOW VIEWS`            | :white_check_mark:           |
| `TRUNCATE TABLE`        | :construction:               |
| `USE DATABASE`          | :white_check_mark:           |

Currently, Sail only supports in-memory catalog, which means the databases and tables are available only within the
session.
Remote catalog support is in our roadmap.

For the `CREATE TABLE` statement, the `CREATE TABLE ... AS ...` syntax is not supported yet.

### Configuration Management

| Statement                   | Supported      |
| --------------------------- | -------------- |
| `RESET` (reset options)     | :construction: |
| `SET` (list or set options) | :construction: |

### Artifact Management

| Statement   | Supported |
| ----------- | --------- |
| `ADD FILE`  | :x:       |
| `ADD JAR`   | :x:       |
| `LIST FILE` | :x:       |
| `LIST JAR`  | :x:       |

### Cache Management

| Statement       | Supported      |
| --------------- | -------------- |
| `CACHE TABLE`   | :construction: |
| `CLEAR CACHE`   | :construction: |
| `UNCACHE TABLE` | :construction: |
