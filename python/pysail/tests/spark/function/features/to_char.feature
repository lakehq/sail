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

    @sail-bug
    # Sail returns '##' instead of '+##' - sign prefix missing in overflow string
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

    @sail-bug
    # Sail returns '$<1,234>' instead of '<$1,234>' - dollar position wrong with PR sign
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

    @sail-bug
    # Sail doesn't handle format strings starting with decimal point (no integer digits)
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

    @sail-bug
    # Sail returns '1.123456789012345691' - f64 precision loss for high-scale decimals
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

    @sail-bug
    # Sail returns '3.14' instead of '#.##' - FLOAT f32->DECIMAL precision not matching Spark
    Scenario: FLOAT with decimal format overflows due to precision
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
