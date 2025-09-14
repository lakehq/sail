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

SELECT 6.9 D AS col;
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

SELECT .789E3 F AS col;
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

Sail supports all Spark SQL data types except the `VARIANT` type introduced in Spark 4.0. Support for the `VARIANT` type
is tracked in the [GitHub issue](https://github.com/lakehq/sail/issues/511).

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
