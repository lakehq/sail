@to_char
Feature: to_char and to_varchar comprehensive tests

  Rule: Argument count validation

    Scenario: zero arguments errors
      When query
        """
        SELECT to_char() AS result
        """
      Then query error .*

    Scenario: one argument errors
      When query
        """
        SELECT to_char(42) AS result
        """
      Then query error .*

    Scenario: one argument date errors
      When query
        """
        SELECT to_char(DATE'2024-01-15') AS result
        """
      Then query error .*

    Scenario: three arguments errors
      When query
        """
        SELECT to_char(42, '999', 'extra') AS result
        """
      Then query error .*

  Rule: NULL combinatorial - numeric mode

    Scenario: NULL int with valid format
      When query
        """
        SELECT to_char(CAST(NULL AS INT), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: valid int with NULL format
      When query
        """
        SELECT to_char(42, CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL int with NULL format
      When query
        """
        SELECT to_char(CAST(NULL AS INT), CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL BIGINT
      When query
        """
        SELECT to_char(CAST(NULL AS BIGINT), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL SMALLINT
      When query
        """
        SELECT to_char(CAST(NULL AS SMALLINT), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL TINYINT
      When query
        """
        SELECT to_char(CAST(NULL AS TINYINT), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL FLOAT
      When query
        """
        SELECT to_char(CAST(NULL AS FLOAT), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL DOUBLE
      When query
        """
        SELECT to_char(CAST(NULL AS DOUBLE), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL DECIMAL
      When query
        """
        SELECT to_char(CAST(NULL AS DECIMAL(10,2)), '999.99') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: NULL combinatorial - temporal mode

    Scenario: NULL date with valid format
      When query
        """
        SELECT to_char(CAST(NULL AS DATE), 'yyyy-MM-dd') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: valid date with NULL format
      When query
        """
        SELECT to_char(DATE'2024-01-15', CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL date with NULL format
      When query
        """
        SELECT to_char(CAST(NULL AS DATE), CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL timestamp with valid format
      When query
        """
        SELECT to_char(CAST(NULL AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: valid timestamp with NULL format
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-15 12:00:00', CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL timestamp with NULL format
      When query
        """
        SELECT to_char(CAST(NULL AS TIMESTAMP), CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: NULL combinatorial - binary mode

    Scenario: NULL binary with utf-8 format
      When query
        """
        SELECT to_char(CAST(NULL AS BINARY), 'utf-8') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL binary with hex format
      When query
        """
        SELECT to_char(CAST(NULL AS BINARY), 'hex') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL binary with base64 format
      When query
        """
        SELECT to_char(CAST(NULL AS BINARY), 'base64') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: valid binary with NULL format errors
      When query
        """
        SELECT to_char(X'48656C6C6F', CAST(NULL AS STRING)) AS result
        """
      Then query error .*

    Scenario: NULL binary with NULL format errors
      When query
        """
        SELECT to_char(CAST(NULL AS BINARY), CAST(NULL AS STRING)) AS result
        """
      Then query error .*

  Rule: NaN Infinity and negative Infinity - DOUBLE

    Scenario: NaN DOUBLE with integer format
      When query
        """
        SELECT to_char(CAST('NaN' AS DOUBLE), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Infinity DOUBLE with integer format
      When query
        """
        SELECT to_char(CAST('Infinity' AS DOUBLE), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: negative Infinity DOUBLE with integer format
      When query
        """
        SELECT to_char(CAST('-Infinity' AS DOUBLE), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NaN DOUBLE with decimal format
      When query
        """
        SELECT to_char(CAST('NaN' AS DOUBLE), '9.9') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Infinity DOUBLE with decimal format
      When query
        """
        SELECT to_char(CAST('Infinity' AS DOUBLE), '9.9') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: negative Infinity DOUBLE with decimal format
      When query
        """
        SELECT to_char(CAST('-Infinity' AS DOUBLE), '9.9') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NaN DOUBLE with sign format
      When query
        """
        SELECT to_char(CAST('NaN' AS DOUBLE), 'S999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Infinity DOUBLE with sign format
      When query
        """
        SELECT to_char(CAST('Infinity' AS DOUBLE), 'S999') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: NaN Infinity and negative Infinity - FLOAT

    Scenario: NaN FLOAT with integer format
      When query
        """
        SELECT to_char(CAST('NaN' AS FLOAT), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Infinity FLOAT with integer format
      When query
        """
        SELECT to_char(CAST('Infinity' AS FLOAT), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: negative Infinity FLOAT with integer format
      When query
        """
        SELECT to_char(CAST('-Infinity' AS FLOAT), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Multi-row column tests - numeric

    Scenario: multi-row integers with optional digit format
      When query
        """
        SELECT to_char(col, '999') AS result FROM VALUES (1), (NULL), (0), (-1), (999) AS t(col)
        """
      Then query result
        | result |
        |   1    |
        | NULL   |
        |        |
        |   1    |
        | 999    |

    Scenario: multi-row integers with S prefix
      When query
        """
        SELECT to_char(col, 'S999') AS result FROM VALUES (42), (-42), (0), (NULL), (999) AS t(col)
        """
      Then query result
        | result |
        |  +42   |
        |  -42   |
        |    +   |
        | NULL   |
        | +999   |

    Scenario: multi-row BIGINT
      When query
        """
        SELECT to_char(col, '9999999999999999999') AS result FROM VALUES (CAST(1 AS BIGINT)), (CAST(NULL AS BIGINT)), (CAST(0 AS BIGINT)), (CAST(-1 AS BIGINT)) AS t(col)
        """
      Then query result
        | result              |
        |                   1 |
        | NULL                |
        |                     |
        |                   1 |

    Scenario: multi-row DOUBLE with decimal format
      When query
        """
        SELECT to_char(col, '999.99') AS result FROM VALUES (CAST(1.5 AS DOUBLE)), (CAST(NULL AS DOUBLE)), (CAST('NaN' AS DOUBLE)), (CAST('Infinity' AS DOUBLE)), (CAST(0.0 AS DOUBLE)) AS t(col)
        """
      Then query result
        | result |
        |   1.50 |
        | NULL   |
        | NULL   |
        | NULL   |
        |   0.00 |

    Scenario: multi-row DECIMAL
      When query
        """
        SELECT to_char(col, '999.99') AS result FROM VALUES (CAST(1.23 AS DECIMAL(5,2))), (CAST(NULL AS DECIMAL(5,2))), (CAST(0.00 AS DECIMAL(5,2))), (CAST(-9.99 AS DECIMAL(5,2))) AS t(col)
        """
      Then query result
        | result |
        |   1.23 |
        | NULL   |
        |   0.00 |
        |   9.99 |

    Scenario: multi-row string numeric input
      When query
        """
        SELECT to_char(col, '999') AS result FROM VALUES ('1'), ('42'), (NULL), ('-5') AS t(col)
        """
      Then query result
        | result |
        |   1    |
        |  42    |
        | NULL   |
        |   5    |

  Rule: Multi-row column tests - temporal

    Scenario: multi-row dates
      When query
        """
        SELECT to_char(col, 'yyyy-MM-dd') AS result FROM VALUES (DATE'2024-01-15'), (CAST(NULL AS DATE)), (DATE'1970-01-01'), (DATE'2024-02-29'), (DATE'9999-12-31') AS t(col)
        """
      Then query result
        | result     |
        | 2024-01-15 |
        | NULL       |
        | 1970-01-01 |
        | 2024-02-29 |
        | 9999-12-31 |

    Scenario: multi-row timestamps
      When query
        """
        SELECT to_char(col, 'yyyy-MM-dd HH:mm:ss') AS result FROM VALUES (TIMESTAMP'2024-01-15 12:00:00'), (CAST(NULL AS TIMESTAMP)), (TIMESTAMP'1970-01-01 00:00:00'), (TIMESTAMP'2024-12-31 23:59:59') AS t(col)
        """
      Then query result
        | result              |
        | 2024-01-15 12:00:00 |
        | NULL                |
        | 1970-01-01 00:00:00 |
        | 2024-12-31 23:59:59 |

  Rule: Multi-row column tests - binary

    Scenario: multi-row binary hex
      When query
        """
        SELECT to_char(col, 'hex') AS result FROM VALUES (X'48656C6C6F'), (CAST(NULL AS BINARY)), (X''), (X'FF'), (X'0000') AS t(col)
        """
      Then query result
        | result     |
        | 48656C6C6F |
        | NULL       |
        |            |
        | FF         |
        | 0000       |

  Rule: Date formatting

    Scenario: date with yyyy-MM-dd format
      When query
        """
        SELECT to_char(DATE'2024-01-15', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result     |
        | 2024-01-15 |

    Scenario: date with MM/dd/yyyy format
      When query
        """
        SELECT to_char(DATE'2024-01-15', 'MM/dd/yyyy') AS result
        """
      Then query result
        | result     |
        | 01/15/2024 |

    Scenario: date with full month name
      When query
        """
        SELECT to_char(DATE'2024-01-15', 'dd MMMM yyyy') AS result
        """
      Then query result
        | result          |
        | 15 January 2024 |

    Scenario: date extracting year only
      When query
        """
        SELECT to_char(DATE'2024-01-15', 'yyyy') AS result
        """
      Then query result
        | result |
        | 2024   |

    Scenario: date with abbreviated month
      When query
        """
        SELECT to_char(DATE'2024-06-15', 'dd MMM yyyy') AS result
        """
      Then query result
        | result      |
        | 15 Jun 2024 |

    Scenario: leap year date with day of week
      When query
        """
        SELECT to_char(DATE'2024-02-29', 'EEEE') AS result
        """
      Then query result
        | result   |
        | Thursday |

    Scenario: leap year date with date and day name
      When query
        """
        SELECT to_char(DATE'2024-02-29', 'yyyy-MM-dd EEEE') AS result
        """
      Then query result
        | result              |
        | 2024-02-29 Thursday |

    Scenario: date epoch
      When query
        """
        SELECT to_char(DATE'1970-01-01', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result     |
        | 1970-01-01 |

    Scenario: date year boundary
      When query
        """
        SELECT to_char(DATE'2023-12-31', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result     |
        | 2023-12-31 |

    Scenario: date minimum
      When query
        """
        SELECT to_char(DATE'0001-01-01', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result     |
        | 0001-01-01 |

    Scenario: date maximum
      When query
        """
        SELECT to_char(DATE'9999-12-31', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result     |
        | 9999-12-31 |

    Scenario: date day of year
      When query
        """
        SELECT to_char(DATE'2024-12-31', 'D') AS result
        """
      Then query result
        | result |
        | 366    |

    @sail-bug
    # Sail returns literal 'Q' instead of quarter number - temporal format pattern not handled
    Scenario: date quarter
      When query
        """
        SELECT to_char(DATE'2024-06-15', 'Q') AS result
        """
      Then query result
        | result |
        | 2      |

  Rule: Timestamp formatting

    Scenario: timestamp full datetime
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-15 13:45:30', 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 13:45:30 |

    Scenario: timestamp time only
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-15 13:45:30', 'HH:mm') AS result
        """
      Then query result
        | result |
        | 13:45  |

    Scenario: timestamp with milliseconds
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-15 13:45:30.123', 'yyyy-MM-dd HH:mm:ss.SSS') AS result
        """
      Then query result
        | result                  |
        | 2024-01-15 13:45:30.123 |

    # Standalone fractional-seconds patterns: the chrono mapping uses a leading dot
    # (SSS -> %.3f), but a standalone pattern must drop it. Validated against Spark JVM.
    Scenario: timestamp with standalone milliseconds
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-15 13:45:30.123', 'SSS') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: timestamp with standalone centiseconds
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-15 13:45:30.123', 'SS') AS result
        """
      Then query result
        | result |
        | 12     |

    Scenario: timestamp with standalone deciseconds
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-15 13:45:30.123', 'S') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: timestamp with microseconds
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-15 12:30:45.123456', 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS result
        """
      Then query result
        | result                     |
        | 2024-01-15 12:30:45.123456 |

    Scenario: timestamp date part only
      When query
        """
        SELECT to_char(TIMESTAMP'2024-12-31 23:59:59', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result     |
        | 2024-12-31 |

    Scenario: timestamp AM/PM at midnight
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-15 00:00:00', 'HH:mm:ss a') AS result
        """
      Then query result
        | result      |
        | 00:00:00 AM |

    Scenario: timestamp AM/PM at noon
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-15 12:00:00', 'hh:mm:ss a') AS result
        """
      Then query result
        | result      |
        | 12:00:00 PM |

    Scenario: timestamp epoch
      When query
        """
        SELECT to_char(TIMESTAMP'1970-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 1970-01-01 00:00:00 |

    Scenario: timestamp end of day with microseconds
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-15 23:59:59.999999', 'HH:mm:ss.SSSSSS') AS result
        """
      Then query result
        | result          |
        | 23:59:59.999999 |

    Scenario: timestamp cast from date
      When query
        """
        SELECT to_char(CAST(CAST('2024-01-15' AS DATE) AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 00:00:00 |

    Scenario: timestamp AM indicator
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-01 00:00:00', 'a') AS result
        """
      Then query result
        | result |
        | AM     |

  Rule: Numeric formatting - basic

    Scenario: integer with zero-padded format
      When query
        """
        SELECT to_char(123, '000') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: integer with leading zero padding
      When query
        """
        SELECT to_char(42, '00000') AS result
        """
      Then query result
        | result |
        | 00042  |

    Scenario: zero with zero-padded format
      When query
        """
        SELECT to_char(0, '000') AS result
        """
      Then query result
        | result |
        | 000    |

    Scenario: zero with optional digit format produces spaces
      When query
        """
        SELECT to_char(0, '999') AS result
        """
      Then query result
        | result |
        |        |

    # Wrap with delimiters so the harness (which strips surrounding whitespace)
    # actually verifies the space padding width. Validated against Spark JVM.
    Scenario: zero with optional digit format pads to exact width
      When query
        """
        SELECT concat('[', to_char(0, '999'), ']') AS result
        """
      Then query result
        | result |
        | [   ]  |

    Scenario: decimal with zero padding
      When query
        """
        SELECT to_char(CAST(1.5 AS DECIMAL(3,1)), '0.00') AS result
        """
      Then query result
        | result |
        | 1.50   |

    Scenario: small decimal
      When query
        """
        SELECT to_char(CAST(0.001 AS DECIMAL(4,3)), '9.999') AS result
        """
      Then query result
        | result |
        | 0.001  |

    Scenario: mixed zero and nine padding
      When query
        """
        SELECT to_char(42, '099') AS result
        """
      Then query result
        | result |
        | 042    |

    Scenario: mixed nine and zero padding with space
      When query
        """
        SELECT to_char(42, '990') AS result
        """
      Then query result
        | result |
        |  42    |

    Scenario: single digit with wide format
      When query
        """
        SELECT to_char(5, '9999') AS result
        """
      Then query result
        | result |
        |    5   |

  Rule: Numeric formatting - thousands separator

    Scenario: comma separator
      When query
        """
        SELECT to_char(12345, '99,999') AS result
        """
      Then query result
        | result |
        | 12,345 |

    Scenario: decimal with thousands separator
      When query
        """
        SELECT to_char(CAST(12345.67 AS DECIMAL(7,2)), '99,999.99') AS result
        """
      Then query result
        | result    |
        | 12,345.67 |

    Scenario: G separator is alias for comma
      When query
        """
        SELECT to_char(12345, '99G999') AS result
        """
      Then query result
        | result |
        | 12,345 |

    Scenario: D separator is alias for period
      When query
        """
        SELECT to_char(CAST(123.45 AS DECIMAL(5,2)), '999D99') AS result
        """
      Then query result
        | result |
        | 123.45 |

    Scenario: G and D together
      When query
        """
        SELECT to_char(CAST(12345.67 AS DECIMAL(7,2)), '99G999D99') AS result
        """
      Then query result
        | result    |
        | 12,345.67 |

    Scenario: large number with thousands
      When query
        """
        SELECT to_char(CAST(99999999.99 AS DECIMAL(10,2)), '99,999,999.99') AS result
        """
      Then query result
        | result        |
        | 99,999,999.99 |

  Rule: Numeric formatting - sign handling

    Scenario: negative without sign spec drops sign
      When query
        """
        SELECT to_char(-123, '999') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: negative with S prefix
      When query
        """
        SELECT to_char(-123, 'S999') AS result
        """
      Then query result
        | result |
        | -123   |

    Scenario: positive with S prefix shows plus
      When query
        """
        SELECT to_char(42, 'S999') AS result
        """
      Then query result
        | result |
        |  +42   |

    Scenario: negative with trailing S
      When query
        """
        SELECT to_char(-123, '999S') AS result
        """
      Then query result
        | result |
        | 123-   |

    Scenario: positive with trailing S
      When query
        """
        SELECT to_char(42, '999S') AS result
        """
      Then query result
        | result |
        |  42+   |

    Scenario: negative with MI prefix
      When query
        """
        SELECT to_char(-42, 'MI999') AS result
        """
      Then query result
        | result |
        |  -42   |

    Scenario: positive with MI prefix shows space
      When query
        """
        SELECT to_char(42, 'MI999') AS result
        """
      Then query result
        | result |
        |   42   |

    Scenario: zero with MI prefix
      When query
        """
        SELECT to_char(0, 'MI999') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: zero with MI suffix
      When query
        """
        SELECT to_char(0, '999MI') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: zero with S prefix
      When query
        """
        SELECT to_char(0, 'S999') AS result
        """
      Then query result
        | result |
        |    +   |

    Scenario: negative with S and zero padding
      When query
        """
        SELECT to_char(-42, 'S0000') AS result
        """
      Then query result
        | result |
        | -0042  |

    Scenario: SMALLINT negative with S
      When query
        """
        SELECT to_char(CAST(-32768 AS SMALLINT), 'S00000') AS result
        """
      Then query result
        | result |
        | -32768 |

    Scenario: SMALLINT negative without S drops sign
      When query
        """
        SELECT to_char(CAST(-32768 AS SMALLINT), '000000') AS result
        """
      Then query result
        | result |
        | 032768 |

  Rule: Numeric formatting - PR sign

    Scenario: PR negative wraps in angle brackets
      When query
        """
        SELECT to_char(-123, '999PR') AS result
        """
      Then query result
        | result |
        | <123>  |

    Scenario: PR positive shows spaces
      When query
        """
        SELECT to_char(123, '999PR') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: PR zero shows spaces
      When query
        """
        SELECT to_char(0, '999PR') AS result
        """
      Then query result
        | result |
        |        |

  Rule: Numeric formatting - overflow

    Scenario: integer overflow produces hash marks
      When query
        """
        SELECT to_char(1234, '99') AS result
        """
      Then query result
        | result |
        | ##     |

    Scenario: decimal overflow produces hash marks
      When query
        """
        SELECT to_char(CAST(3.14159 AS DECIMAL(10,5)), '9.999') AS result
        """
      Then query result
        | result |
        | #.###  |

    Scenario: overflow with S sign
      When query
        """
        SELECT to_char(1234, 'S99') AS result
        """
      Then query result
        | result |
        | +##    |

    Scenario: overflow with dollar
      When query
        """
        SELECT to_char(1234, '$99') AS result
        """
      Then query result
        | result |
        | $##    |

    Scenario: overflow with PR
      When query
        """
        SELECT to_char(1234, '99PR') AS result
        """
      Then query result
        | result |
        | ##     |

    Scenario: overflow decimal value
      When query
        """
        SELECT to_char(CAST(999.999 AS DECIMAL(6,3)), '99.9') AS result
        """
      Then query result
        | result |
        | ##.#   |

    Scenario: INT_MAX overflow
      When query
        """
        SELECT to_char(2147483647, '99') AS result
        """
      Then query result
        | result |
        | ##     |

    Scenario: BIGINT_MAX overflow
      When query
        """
        SELECT to_char(CAST(9223372036854775807 AS BIGINT), '999') AS result
        """
      Then query result
        | result |
        | ###    |

    Scenario: TINYINT_MIN overflow
      When query
        """
        SELECT to_char(CAST(-128 AS TINYINT), '99') AS result
        """
      Then query result
        | result |
        | ##     |

  Rule: Numeric formatting - currency

    Scenario: dollar sign
      When query
        """
        SELECT to_char(1234, '$9,999') AS result
        """
      Then query result
        | result  |
        | $1,234  |

    Scenario: dollar with thousands
      When query
        """
        SELECT to_char(12345, '$99,999') AS result
        """
      Then query result
        | result  |
        | $12,345 |

    Scenario: dollar with trailing S suffix
      When query
        """
        SELECT to_char(-1234, '$9,999S') AS result
        """
      Then query result
        | result  |
        | $1,234- |

    Scenario: dollar with PR
      When query
        """
        SELECT to_char(-1234, '$9,999PR') AS result
        """
      Then query result
        | result   |
        | <$1,234> |

  Rule: Numeric formatting - decimal format

    Scenario: high precision decimal
      When query
        """
        SELECT to_char(CAST(3.14159265 AS DECIMAL(10,8)), '9.99999999') AS result
        """
      Then query result
        | result     |
        | 3.14159265 |

    Scenario: decimal truncated overflows
      When query
        """
        SELECT to_char(CAST(3.14159 AS DECIMAL(10,5)), '9.99') AS result
        """
      Then query result
        | result |
        | #.##   |

    Scenario: decimal no integer part
      When query
        """
        SELECT to_char(CAST(0.123 AS DECIMAL(4,3)), '.999') AS result
        """
      Then query result
        | result |
        | .123   |

    Scenario: decimal zero with optional integer
      When query
        """
        SELECT to_char(CAST(0 AS DECIMAL(5,2)), '99.99') AS result
        """
      Then query result
        | result |
        |  0.00  |

    Scenario: negative decimal without sign spec drops sign
      When query
        """
        SELECT to_char(CAST(-0.5 AS DECIMAL(2,1)), '9.9') AS result
        """
      Then query result
        | result |
        | 0.5    |

    Scenario: negative with zero padding drops sign
      When query
        """
        SELECT to_char(-123, '0999') AS result
        """
      Then query result
        | result |
        | 0123   |

    Scenario: max precision 38 digits
      When query
        """
        SELECT to_char(CAST(12345678901234567890123456789012345678 AS DECIMAL(38,0)), '99999999999999999999999999999999999999') AS result
        """
      Then query result
        | result                                 |
        | 12345678901234567890123456789012345678  |

    Scenario: max scale 18 digits
      When query
        """
        SELECT to_char(CAST(1.123456789012345678 AS DECIMAL(19,18)), '9.999999999999999999') AS result
        """
      Then query result
        | result               |
        | 1.123456789012345678 |

  Rule: Numeric formatting - boundary values

    Scenario: TINYINT min with S
      When query
        """
        SELECT to_char(CAST(-128 AS TINYINT), 'S000') AS result
        """
      Then query result
        | result |
        | -128   |

    Scenario: TINYINT max
      When query
        """
        SELECT to_char(CAST(127 AS TINYINT), '000') AS result
        """
      Then query result
        | result |
        | 127    |

    Scenario: TINYINT zero
      When query
        """
        SELECT to_char(CAST(0 AS TINYINT), '000') AS result
        """
      Then query result
        | result |
        | 000    |

    Scenario: SMALLINT min with S
      When query
        """
        SELECT to_char(CAST(-32768 AS SMALLINT), 'S00000') AS result
        """
      Then query result
        | result |
        | -32768 |

    Scenario: SMALLINT max
      When query
        """
        SELECT to_char(CAST(32767 AS SMALLINT), '00000') AS result
        """
      Then query result
        | result |
        | 32767  |

    Scenario: INT max
      When query
        """
        SELECT to_char(2147483647, '9999999999') AS result
        """
      Then query result
        | result     |
        | 2147483647 |

    Scenario: INT min with S
      When query
        """
        SELECT to_char(-2147483648, 'S9999999999') AS result
        """
      Then query result
        | result      |
        | -2147483648 |

    Scenario: INT min without S drops sign
      When query
        """
        SELECT to_char(-2147483648, '9999999999') AS result
        """
      Then query result
        | result     |
        | 2147483648 |

    Scenario: BIGINT max
      When query
        """
        SELECT to_char(CAST(9223372036854775807 AS BIGINT), '9999999999999999999') AS result
        """
      Then query result
        | result              |
        | 9223372036854775807 |

    Scenario: BIGINT min with S
      When query
        """
        SELECT to_char(CAST(-9223372036854775808 AS BIGINT), 'S9999999999999999999') AS result
        """
      Then query result
        | result               |
        | -9223372036854775808 |

    Scenario: BIGINT min without S drops sign
      When query
        """
        SELECT to_char(CAST(-9223372036854775808 AS BIGINT), '9999999999999999999') AS result
        """
      Then query result
        | result              |
        | 9223372036854775808 |

  Rule: Numeric formatting - special float values

    Scenario: negative zero DOUBLE with integer format
      When query
        """
        SELECT to_char(CAST(-0.0 AS DOUBLE), '999') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: negative zero DOUBLE with S format
      When query
        """
        SELECT to_char(CAST(-0.0 AS DOUBLE), 'S999') AS result
        """
      Then query result
        | result |
        |    +   |

    Scenario: negative zero FLOAT with decimal format
      When query
        """
        SELECT to_char(CAST(-0.0 AS FLOAT), '9.9') AS result
        """
      Then query result
        | result |
        | 0.0    |

    Scenario: DOUBLE with decimal format
      When query
        """
        SELECT to_char(CAST(3.14 AS DOUBLE), '9.99') AS result
        """
      Then query result
        | result |
        | 3.14   |

    Scenario: FLOAT with decimal format overflows due to precision
      # Spark implicitly casts Float32 to Decimal(14, 7) before formatting.
      # `3.14` as f32 stores as ~3.1400001, so the 7th decimal is non-zero —
      # the format's scale 2 can't represent that, so Spark (and Sail) emit
      # overflow markers.
      When query
        """
        SELECT to_char(CAST(3.14 AS FLOAT), '9.99') AS result
        """
      Then query result
        | result |
        | #.##   |

    Scenario: FLOAT with wider format shows precision noise
      When query
        """
        SELECT to_char(CAST(3.14 AS FLOAT), '9.9999999') AS result
        """
      Then query result
        | result    |
        | 3.1400001 |

    Scenario: DOUBLE pi with wider format
      When query
        """
        SELECT to_char(CAST(3.14 AS DOUBLE), '9.999') AS result
        """
      Then query result
        | result |
        | 3.140  |

    Scenario: integer as DOUBLE
      When query
        """
        SELECT to_char(CAST(42 AS DOUBLE), '999') AS result
        """
      Then query result
        | result |
        |  42    |

    Scenario: integer as FLOAT
      When query
        """
        SELECT to_char(CAST(42 AS FLOAT), '999') AS result
        """
      Then query result
        | result |
        |  42    |

  Rule: Numeric formatting - string input coercion

    Scenario: string numeric input
      When query
        """
        SELECT to_char('42', '999') AS result
        """
      Then query result
        | result |
        |  42    |

    Scenario: string decimal input
      When query
        """
        SELECT to_char('3.14', '9.99') AS result
        """
      Then query result
        | result |
        | 3.14   |

    Scenario: string negative input
      When query
        """
        SELECT to_char('-100', 'S999') AS result
        """
      Then query result
        | result |
        | -100   |

  Rule: Binary formatting

    Scenario: binary to UTF-8
      When query
        """
        SELECT to_char(encode('hello world', 'utf-8'), 'utf-8') AS result
        """
      Then query result
        | result      |
        | hello world |

    # Spark decodes UTF-8 strictly: invalid bytes raise MALFORMED_CHARACTER_CODING,
    # they are NOT replaced lossily. Validated against Spark JVM.
    Scenario: invalid UTF-8 binary errors
      When query
        """
        SELECT to_char(X'FF', 'utf-8') AS result
        """
      Then query error .*

    Scenario: invalid UTF-8 byte mid-string errors
      When query
        """
        SELECT to_char(X'48FF65', 'utf-8') AS result
        """
      Then query error .*

    Scenario: binary to base64
      When query
        """
        SELECT to_char(x'48656C6C6F', 'base64') AS result
        """
      Then query result
        | result   |
        | SGVsbG8= |

    Scenario: binary to hex
      When query
        """
        SELECT to_char(x'48656C6C6F', 'hex') AS result
        """
      Then query result
        | result     |
        | 48656C6C6F |

    Scenario: empty binary to UTF-8
      When query
        """
        SELECT to_char(encode('', 'utf-8'), 'utf-8') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: empty binary to base64
      When query
        """
        SELECT to_char(x'', 'base64') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: empty binary to hex
      When query
        """
        SELECT to_char(x'', 'hex') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: all zeros hex
      When query
        """
        SELECT to_char(x'0000000000', 'hex') AS result
        """
      Then query result
        | result     |
        | 0000000000 |

    Scenario: all FF hex
      When query
        """
        SELECT to_char(x'FFFFFFFFFFFF', 'hex') AS result
        """
      Then query result
        | result       |
        | FFFFFFFFFFFF |

    Scenario: single byte hex
      When query
        """
        SELECT to_char(x'42', 'hex') AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: single byte base64
      When query
        """
        SELECT to_char(x'42', 'base64') AS result
        """
      Then query result
        | result |
        | Qg==   |

    Scenario: case insensitive format UTF-8
      When query
        """
        SELECT to_char(encode('hello', 'utf-8'), 'UTF-8') AS result
        """
      Then query result
        | result |
        | hello  |

    Scenario: case insensitive format HEX
      When query
        """
        SELECT to_char(x'48656C6C6F', 'HEX') AS result
        """
      Then query result
        | result     |
        | 48656C6C6F |

    Scenario: case insensitive format BASE64
      When query
        """
        SELECT to_char(x'48656C6C6F', 'BASE64') AS result
        """
      Then query result
        | result   |
        | SGVsbG8= |

    Scenario: large binary hex length
      When query
        """
        SELECT length(to_char(encode(repeat('x', 1000), 'utf-8'), 'hex')) AS result
        """
      Then query result
        | result |
        | 2000   |

  Rule: to_varchar alias

    Scenario: to_varchar with date
      When query
        """
        SELECT to_varchar(DATE'2024-06-15', 'yyyy/MM/dd') AS result
        """
      Then query result
        | result     |
        | 2024/06/15 |

    Scenario: to_varchar with integer
      When query
        """
        SELECT to_varchar(42, '000') AS result
        """
      Then query result
        | result |
        | 042    |

    Scenario: to_varchar with binary hex
      When query
        """
        SELECT to_varchar(X'48656C6C6F', 'hex') AS result
        """
      Then query result
        | result     |
        | 48656C6C6F |

    Scenario: to_varchar with decimal
      When query
        """
        SELECT to_varchar(CAST(1.23 AS DECIMAL(5,2)), '9.99') AS result
        """
      Then query result
        | result |
        | 1.23   |

  Rule: Numeric formatting - type coercion

    Scenario: DECIMAL with thousands
      When query
        """
        SELECT to_char(CAST(1234.5678 AS DECIMAL(10,4)), '9,999.9999') AS result
        """
      Then query result
        | result      |
        | 1,234.5678  |

  Rule: Expressions and nesting

    Scenario: to_char in WHERE clause
      When query
        """
        SELECT col FROM VALUES (1), (2), (3), (4), (5) AS t(col) WHERE to_char(col, '9') = '3'
        """
      Then query result
        | col |
        | 3   |

    Scenario: to_char in ORDER BY
      When query
        """
        SELECT col, to_char(col, '000') AS formatted FROM VALUES (3), (1), (2) AS t(col) ORDER BY formatted
        """
      Then query result ordered
        | col | formatted |
        | 1   | 001       |
        | 2   | 002       |
        | 3   | 003       |

    Scenario: to_char in CASE WHEN
      When query
        """
        SELECT CASE WHEN to_char(42, '99') = '42' THEN 'yes' ELSE 'no' END AS result
        """
      Then query result
        | result |
        | yes    |

    @sail-bug
    # Sail can't CAST(' 42' AS INT) - doesn't trim leading spaces before casting string to int
    Scenario: nested to_char
      When query
        """
        SELECT to_char(CAST(to_char(42, '999') AS INT), '0000') AS result
        """
      Then query result
        | result |
        | 0042   |

  Rule: Error conditions

    Scenario: boolean input errors
      When query
        """
        SELECT to_char(true, '999') AS result
        """
      Then query error .*

    Scenario: array input errors
      When query
        """
        SELECT to_char(array(1,2,3), '999') AS result
        """
      Then query error .*

    Scenario: map input errors
      When query
        """
        SELECT to_char(map('a',1), '999') AS result
        """
      Then query error .*

    Scenario: struct input errors
      When query
        """
        SELECT to_char(named_struct('a',1), '999') AS result
        """
      Then query error .*

    Scenario: binary with invalid format errors
      When query
        """
        SELECT to_char(X'48656C6C6F', 'invalid') AS result
        """
      Then query error .*

    Scenario: binary with number format errors
      When query
        """
        SELECT to_char(X'48656C6C6F', '999') AS result
        """
      Then query error .*

    Scenario: invalid format string hash marks
      When query
        """
        SELECT to_char(42, '###') AS result
        """
      Then query error .*

    Scenario: empty format string errors
      When query
        """
        SELECT to_char(42, '') AS result
        """
      Then query error .*

    Scenario: L currency is not valid
      When query
        """
        SELECT to_char(1234, 'L9,999') AS result
        """
      Then query error .*

  Rule: MI suffix (bug-hunt additions)
    # The existing feature covered MI prefix only; MI in suffix position
    # is the canonical Oracle/PostgreSQL placement. Values validated against
    # Spark JVM (Spark 4.0 local).

    Scenario: MI suffix on negative
      When query
        """
        SELECT to_char(-5, '99MI') AS result
        """
      Then query result
        | result |
        |  5-    |

    Scenario: MI suffix on positive shows trailing space
      When query
        """
        SELECT to_char(5, '99MI') AS result
        """
      Then query result
        | result |
        |  5     |

    Scenario: MI suffix on zero collapses to all spaces
      # Spark: zero with all-9 format yields spaces for the digits,
      # and MI for positive (zero treated as positive) is a trailing space.
      When query
        """
        SELECT to_char(0, '99MI') AS result
        """
      Then query result
        | result |
        |        |

  Rule: PR on decimals and positive (bug-hunt additions)
    # Existing coverage has PR on integers; decimal + PR combinations were
    # missing. Validated against Spark JVM.

    Scenario: PR on negative decimal wraps the whole number
      When query
        """
        SELECT to_char(-1.5, '9.9PR') AS result
        """
      Then query result
        | result |
        | <1.5>  |

    Scenario: PR on negative decimal without leading int slot
      When query
        """
        SELECT to_char(-0.5, '.9PR') AS result
        """
      Then query result
        | result |
        | <.5>   |

    Scenario: PR positive wider format pads right with spaces
      When query
        """
        SELECT to_char(1234, '9999PR') AS result
        """
      Then query result
        | result |
        | 1234   |

  Rule: Non-thousand grouping positions (bug-hunt additions)
    # Spark supports arbitrary comma/G positions, not just every 3 digits.
    # This mirrors Oracle's behavior and e.g. Indian-style 2,2,3 lakh
    # grouping. Validated against Spark JVM.

    Scenario: Indian-style 2,2,3 grouping (lakh)
      When query
        """
        SELECT to_char(12345, '9,99,99') AS result
        """
      Then query result
        | result  |
        | 1,23,45 |

    Scenario: Comma between every single digit
      When query
        """
        SELECT to_char(1234567, '9,9,9,9,9,9,9') AS result
        """
      Then query result
        | result        |
        | 1,2,3,4,5,6,7 |

    Scenario: Short 2-digit grouping
      When query
        """
        SELECT to_char(12, '9,9') AS result
        """
      Then query result
        | result |
        | 1,2    |

    Scenario: 4-digit grouping (non-standard but allowed)
      When query
        """
        SELECT to_char(12345678, '9,9999,9999') AS result
        """
      Then query result
        | result      |
        |   1234,5678 |

  Rule: Format without integer slots (bug-hunt additions)
    # Formats starting with "." (no 9/0 before the decimal) are legal for
    # magnitudes < 1. Validated against Spark JVM.

    Scenario: No-integer format on sub-one positive
      When query
        """
        SELECT to_char(0.5, '.99') AS result
        """
      Then query result
        | result |
        | .50    |

    Scenario: No-integer format on very small positive
      When query
        """
        SELECT to_char(0.05, '.99') AS result
        """
      Then query result
        | result |
        | .05    |

    Scenario: No-integer format on negative drops sign without sign spec
      When query
        """
        SELECT to_char(-0.5, '.99') AS result
        """
      Then query result
        | result |
        | .50    |

  Rule: Lowercase grouping and decimal markers (bug-hunt additions)
    # Spark accepts both G/D (Oracle-style) and g/d (lowercase). Existing
    # coverage only tested uppercase.

    Scenario: Lowercase g acts as thousands separator
      When query
        """
        SELECT to_char(1234, '9g999') AS result
        """
      Then query result
        | result |
        | 1,234  |

    Scenario: Lowercase d acts as decimal point
      When query
        """
        SELECT to_char(1.5, '9d9') AS result
        """
      Then query result
        | result |
        | 1.5    |

  Rule: Negative zero with sign specs (bug-hunt additions)
    # Spark treats -0.0 as non-negative for sign placement (matches IEEE
    # semantics where -0.0 == 0.0 numerically). Sail's impl already checks
    # bits to distinguish, but this is the only explicit coverage.

    Scenario: Negative zero DOUBLE with S treated as positive
      When query
        """
        SELECT to_char(CAST(-0.0 AS DOUBLE), 'S99') AS result
        """
      Then query result
        | result |
        |   +    |

    Scenario: Negative zero DOUBLE with MI shows space
      When query
        """
        SELECT to_char(CAST(-0.0 AS DOUBLE), '99MI') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: Negative zero DOUBLE with PR shows spaces
      When query
        """
        SELECT to_char(CAST(-0.0 AS DOUBLE), '99PR') AS result
        """
      Then query result
        | result |
        |        |

    @sail-bug
    Scenario: Negative zero DECIMAL with S treated as positive
      # Sail bug: for decimal values with integer part == 0, Spark omits the
      # leading '0' slot (format `S9.99` on 0.00 produces ' +.00'), but Sail
      # emits '+0.00'. The decimal path doesn't apply the "drop leading zero"
      # rule that the f64 path does.
      When query
        """
        SELECT to_char(CAST(-0.0 AS DECIMAL(10,2)), 'S9.99') AS result
        """
      Then query result
        | result |
        |  +.00  |

  Rule: Decimal scale vs format scale interactions (bug-hunt additions)
    # The scale overflow path (input scale > format scale) is tested once
    # already but not combined with format features like currency or
    # grouping. Validated against Spark JVM.

    Scenario: Decimal input scale > format scale produces overflow markers
      When query
        """
        SELECT to_char(CAST(1.234 AS DECIMAL(10,3)), '9.9') AS result
        """
      Then query result
        | result |
        | #.#    |

    Scenario: Decimal input scale < format scale pads with zeros
      When query
        """
        SELECT to_char(CAST(1.23 AS DECIMAL(10,2)), '9.999') AS result
        """
      Then query result
        | result |
        | 1.230  |

    Scenario: Integer value as decimal with fractional format pads zeros
      When query
        """
        SELECT to_char(CAST(1.5 AS DECIMAL(10,1)), 'S9.9') AS result
        """
      Then query result
        | result |
        | +1.5   |

    # Decimal128(_, 0) with a fractional format must use the exact i128 path, not
    # f64 — values beyond ~15 significant digits lose precision through a double.
    # Validated against Spark JVM.
    Scenario: Decimal scale-0 with fractional format keeps full precision
      When query
        """
        SELECT to_char(CAST(12345678901234567 AS DECIMAL(38,0)), '99999999999999999.99') AS result
        """
      Then query result
        | result               |
        | 12345678901234567.00 |

    Scenario: Large decimal scale-0 with fractional format is exact not overflow
      When query
        """
        SELECT to_char(CAST(99999999999999999999 AS DECIMAL(38,0)), '99999999999999999999.99') AS result
        """
      Then query result
        | result                  |
        | 99999999999999999999.00 |

    Scenario: Negative decimal scale-0 with sign and fractional format
      When query
        """
        SELECT to_char(CAST(-12345678901234567 AS DECIMAL(38,0)), 'S99999999999999999.99') AS result
        """
      Then query result
        | result                |
        | -12345678901234567.00 |

    Scenario: Decimal scale-0 with grouping and fractional format
      When query
        """
        SELECT to_char(CAST(12345678901234567 AS DECIMAL(38,0)), '99,999,999,999,999,999.99') AS result
        """
      Then query result
        | result                    |
        | 12,345,678,901,234,567.00 |

    Scenario: Small decimal scale-0 with fractional format
      When query
        """
        SELECT to_char(CAST(7 AS DECIMAL(38,0)), '9.99') AS result
        """
      Then query result
        | result |
        | 7.00   |

  Rule: Trailing S on decimal (bug-hunt additions)
    # Existing coverage has S prefix (numeric) and S suffix with integers
    # only. Trailing S combined with decimal format is the natural
    # extension.

    Scenario: Trailing S on negative decimal
      When query
        """
        SELECT to_char(-1.5, '9.9S') AS result
        """
      Then query result
        | result |
        | 1.5-   |

    Scenario: Trailing S on positive decimal
      When query
        """
        SELECT to_char(1.5, '9.9S') AS result
        """
      Then query result
        | result |
        | 1.5+   |

  Rule: Multi-row with mixed overflow and NULL (bug-hunt additions)
    # Combines null-propagation, overflow markers, and non-overflow
    # formatting in a single column. Exercises the null-aware path in the
    # overflow return branch.

    Scenario: Multi-row INT mixing normal, overflow, NULL
      When query
        """
        SELECT to_char(v, '99') AS result FROM VALUES
          (1),
          (50),
          (100),
          (CAST(NULL AS INT)) AS t(v)
        """
      Then query result ordered
        | result |
        |  1     |
        | 50     |
        | ##     |
        | NULL   |

  Rule: Complex sign + currency + grouping (bug-hunt additions)
    # Combination of all format features in one query — highest
    # interaction-risk scenario for regression.

    Scenario: S prefix + dollar + thousands + decimals on negative
      When query
        """
        SELECT to_char(-1234.56, 'S$9,999.99') AS result
        """
      Then query result
        | result      |
        | -$1,234.56  |

  Rule: Error conditions (bug-hunt additions)

    Scenario: Double S in format errors
      When query
        """
        SELECT to_char(5, 'SS99') AS result
        """
      Then query error .*

    Scenario: Comma at start of format errors
      When query
        """
        SELECT to_char(1, ',999') AS result
        """
      Then query error .*

    Scenario: Comma at end of format errors
      When query
        """
        SELECT to_char(1, '999,') AS result
        """
      Then query error .*

    Scenario: S alone with no digits errors
      When query
        """
        SELECT to_char(-5, 'S') AS result
        """
      Then query error .*

    Scenario: Dot alone with no digits errors
      When query
        """
        SELECT to_char(1.5, '.') AS result
        """
      Then query error .*

    Scenario: Space inside format errors
      When query
        """
        SELECT to_char(5, '9 9') AS result
        """
      Then query error .*

  Rule: All-null short-circuit must NOT bypass format validation

    # These tests lock the invariant that format errors fire BEFORE the
    # all-null short-circuit. If a future refactor moves the short-circuit
    # above RegexSpec::try_from, these flip from error to pass (silent bug).

    Scenario: to_char all-null int column with invalid double-S format still errors
      When query
        """
        SELECT to_char(v, 'SS999') AS result FROM VALUES
          (CAST(NULL AS INT)),
          (CAST(NULL AS INT))
          AS t(v)
        """
      Then query error .*

    Scenario: to_char all-null double column with invalid comma-start format still errors
      When query
        """
        SELECT to_char(v, ',999') AS result FROM VALUES
          (CAST(NULL AS DOUBLE))
          AS t(v)
        """
      Then query error .*

    Scenario: to_char all-null decimal column with invalid dot-only format still errors
      When query
        """
        SELECT to_char(v, '.') AS result FROM VALUES
          (CAST(NULL AS DECIMAL(10,2)))
          AS t(v)
        """
      Then query error .*

    Scenario: to_char all-null int column with VALID format returns all NULL
      When query
        """
        SELECT to_char(v, '999') AS result FROM VALUES
          (CAST(NULL AS INT)),
          (CAST(NULL AS INT))
          AS t(v)
        """
      Then query result ordered
        | result |
        | NULL   |
        | NULL   |

  Rule: ANSI mode - string coercion and decimal overflow (bug-hunt additions)

    Scenario: ANSI false invalid string coerced to NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_char('hello', '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: ANSI true invalid string errors
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_char('hello', '999') AS result
        """
      Then query error .*

    @sail-bug
    Scenario: ANSI false DECIMAL overflow returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_char(CAST(99999 AS DECIMAL(5,2)), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: ANSI true DECIMAL overflow errors
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_char(CAST(99999 AS DECIMAL(5,2)), '999') AS result
        """
      Then query error .*

    Scenario: ANSI false multi-row mixed valid and invalid string
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_char(v, '999') AS result FROM VALUES ('1'), ('hello'), ('3') AS t(v)
        """
      Then query result ordered
        | result |
        |   1    |
        | NULL   |
        |   3    |

  Rule: Binary formatting - edge cases (bug-hunt additions)

    Scenario: binary with invalid UTF-8 bytes errors
      When query
        """
        SELECT to_char(X'80FF', 'utf-8') AS result
        """
      Then query error .*

    Scenario: binary with valid two-byte UTF-8 sequence
      When query
        """
        SELECT to_char(X'C3A9', 'utf-8') AS result
        """
      Then query result
        | result |
        | é      |

  Rule: Numeric formatting - FLOAT overflow (bug-hunt additions)

    Scenario: FLOAT MAX value overflows any format and errors
      When query
        """
        SELECT to_char(CAST(3.4028235E38 AS FLOAT), '999') AS result
        """
      Then query error .*

  Rule: TIMESTAMP_NTZ formatting (bug-hunt additions)

    Scenario: TIMESTAMP_NTZ with datetime format
      When query
        """
        SELECT to_char(TIMESTAMP_NTZ '2024-03-15 09:30:00', 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 2024-03-15 09:30:00 |

    Scenario: TIMESTAMP_NTZ with microseconds format
      When query
        """
        SELECT to_char(TIMESTAMP_NTZ '2024-03-15 09:30:00.123456', 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS result
        """
      Then query result
        | result                        |
        | 2024-03-15 09:30:00.123456    |

    Scenario: NULL TIMESTAMP_NTZ with valid format returns NULL
      When query
        """
        SELECT to_char(CAST(NULL AS TIMESTAMP_NTZ), 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: valid TIMESTAMP_NTZ with NULL format returns NULL
      When query
        """
        SELECT to_char(TIMESTAMP_NTZ '2024-03-15 09:30:00', NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Temporal formatting - standalone fractional-seconds (SSS fix)

    Scenario: SSS standalone returns milliseconds without leading dot
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-15 12:34:56.789', 'SSS') AS result
        """
      Then query result
        | result |
        | 789    |

    Scenario: SSSSSS standalone returns microseconds without leading dot
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-15 12:34:56.789012', 'SSSSSS') AS result
        """
      Then query result
        | result |
        | 789012 |

    Scenario: S single digit returns tenths without leading dot
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-15 12:34:56.100', 'S') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: SSS with NULL timestamp returns NULL
      When query
        """
        SELECT to_char(CAST(NULL AS TIMESTAMP), 'SSS') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: compound format HH:mm:ss.SSS retains dot via chrono
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-15 12:34:56.789', 'HH:mm:ss.SSS') AS result
        """
      Then query result
        | result       |
        | 12:34:56.789 |

    Scenario: SSS with TIMESTAMP_NTZ returns milliseconds without dot
      When query
        """
        SELECT to_char(TIMESTAMP_NTZ'2024-01-15 12:34:56.789', 'SSS') AS result
        """
      Then query result
        | result |
        | 789    |

  Rule: Padding verification with bracket notation

    @sail-bug
    Scenario: MI prefix negative has leading space before minus
      When query
        """
        SELECT concat('[', to_char(-42, 'MI999'), ']') AS result
        """
      Then query result
        | result |
        | [ -42] |

    @sail-bug
    Scenario: MI prefix positive has two leading spaces
      When query
        """
        SELECT concat('[', to_char(42, 'MI999'), ']') AS result
        """
      Then query result
        | result |
        | [  42] |

    Scenario: MI prefix zero is all spaces
      When query
        """
        SELECT concat('[', to_char(0, 'MI999'), ']') AS result
        """
      Then query result
        | result |
        | [    ] |

    Scenario: MI suffix negative shows trailing minus
      When query
        """
        SELECT concat('[', to_char(-5, '99MI'), ']') AS result
        """
      Then query result
        | result |
        | [ 5-]  |

    Scenario: MI suffix positive shows trailing space
      When query
        """
        SELECT concat('[', to_char(5, '99MI'), ']') AS result
        """
      Then query result
        | result |
        | [ 5 ]  |

    Scenario: MI suffix zero is all spaces
      When query
        """
        SELECT concat('[', to_char(0, '99MI'), ']') AS result
        """
      Then query result
        | result |
        | [   ]  |

    Scenario: PR negative wraps in angle brackets no extra spaces
      When query
        """
        SELECT concat('[', to_char(-123, '999PR'), ']') AS result
        """
      Then query result
        | result  |
        | [<123>] |

    @sail-bug
    Scenario: PR positive has two trailing spaces
      When query
        """
        SELECT concat('[', to_char(123, '999PR'), ']') AS result
        """
      Then query result
        | result   |
        | [123  ]  |

    Scenario: PR zero is five spaces
      When query
        """
        SELECT concat('[', to_char(0, '999PR'), ']') AS result
        """
      Then query result
        | result    |
        | [     ]   |

    Scenario: 9-digit format pads small value with leading spaces
      When query
        """
        SELECT concat('[', to_char(5, '9999'), ']') AS result
        """
      Then query result
        | result  |
        | [   5]  |

    Scenario: 9-digit format pads two-digit value
      When query
        """
        SELECT concat('[', to_char(42, '9999'), ']') AS result
        """
      Then query result
        | result |
        | [  42] |

    Scenario: 9-digit format negative suppresses sign without sign specifier
      When query
        """
        SELECT concat('[', to_char(-42, '9999'), ']') AS result
        """
      Then query result
        | result |
        | [  42] |

  Rule: Decimal precision — exact i128 arithmetic preserves all significant digits

    Scenario: 20-digit decimal with 10 fractional digits preserves full precision
      When query
        """
        SELECT to_char(CAST('12345678901234567890.123456789' AS DECIMAL(38,10)), '99999999999999999999.9999999999') AS result
        """
      Then query result
        | result                          |
        | 12345678901234567890.1234567890 |

    Scenario: 19-digit decimal with 1 fractional digit preserves all digits
      When query
        """
        SELECT to_char(CAST('1234567890123456789.9' AS DECIMAL(38,1)), '9999999999999999999.9') AS result
        """
      Then query result
        | result                |
        | 1234567890123456789.9 |

    Scenario: value at 2-power-53-plus-1 boundary preserves exact digits
      When query
        """
        SELECT to_char(CAST('9007199254740993.01' AS DECIMAL(38,2)), '9999999999999999.99') AS result
        """
      Then query result
        | result              |
        | 9007199254740993.01 |

    Scenario: 19-digit value with 2 fractional digits preserves all digits
      When query
        """
        SELECT to_char(CAST('9999999999999998765.43' AS DECIMAL(38,2)), '9999999999999999999.99') AS result
        """
      Then query result
        | result                 |
        | 9999999999999998765.43 |

    Scenario: 17-digit decimal with 1 fractional digit preserves fractional part
      When query
        """
        SELECT to_char(CAST('12345678901234567.8' AS DECIMAL(38,1)), '99999999999999999.9') AS result
        """
      Then query result
        | result              |
        | 12345678901234567.8 |

    Scenario: 18-digit decimal with 1 fractional digit preserves all digits
      When query
        """
        SELECT to_char(CAST('123456789012345678.9' AS DECIMAL(38,1)), '999999999999999999.9') AS result
        """
      Then query result
        | result               |
        | 123456789012345678.9 |

    Scenario: 19-digit max value with 2 fractional digits formats correctly
      When query
        """
        SELECT to_char(CAST('9999999999999999999.99' AS DECIMAL(38,2)), '9999999999999999999.99') AS result
        """
      Then query result
        | result                 |
        | 9999999999999999999.99 |

    Scenario: 17-digit max value with 2 fractional digits formats correctly
      When query
        """
        SELECT to_char(CAST('99999999999999999.99' AS DECIMAL(38,2)), '99999999999999999.99') AS result
        """
      Then query result
        | result               |
        | 99999999999999999.99 |

  Rule: Large Decimal precision — exact i128 arithmetic preserves all digits

    Scenario: 29-significant-digit decimal preserves full precision
      When query
        """
        SELECT to_char(CAST('12345678901234567890.123456789' AS DECIMAL(38,9)), '99999999999999999999.999999999') AS result
        """
      Then query result
        | result                         |
        | 12345678901234567890.123456789 |

    Scenario: 20-significant-digit decimal preserves last digits
      When query
        """
        SELECT to_char(CAST('1234567890123456789.9' AS DECIMAL(38,1)), '9999999999999999999.9') AS result
        """
      Then query result
        | result                |
        | 1234567890123456789.9 |
