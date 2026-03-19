@to_char
Feature: to_char and to_varchar functions

  Rule: Date formatting

    Scenario: to_char with date and yyyy-MM-dd format
      When query
        """
        SELECT to_char(DATE'2024-01-15', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result     |
        | 2024-01-15 |

    Scenario: to_char with date and MM/dd/yyyy format
      When query
        """
        SELECT to_char(DATE'2024-01-15', 'MM/dd/yyyy') AS result
        """
      Then query result
        | result     |
        | 01/15/2024 |

    Scenario: to_char with date and full month name
      When query
        """
        SELECT to_char(DATE'2024-01-15', 'dd MMMM yyyy') AS result
        """
      Then query result
        | result          |
        | 15 January 2024 |

    Scenario: to_char with date extracting year only
      When query
        """
        SELECT to_char(DATE'2024-01-15', 'yyyy') AS result
        """
      Then query result
        | result |
        | 2024   |

    Scenario: to_char with leap year date and day of week
      When query
        """
        SELECT to_char(DATE'2024-02-29', 'EEEE') AS result
        """
      Then query result
        | result   |
        | Thursday |

  Rule: Timestamp formatting

    Scenario: to_char with timestamp and full datetime format
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-15 13:45:30', 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 13:45:30 |

    Scenario: to_char with timestamp extracting time only
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-15 13:45:30', 'HH:mm') AS result
        """
      Then query result
        | result |
        | 13:45  |

    Scenario: to_char with timestamp and milliseconds
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-15 13:45:30.123', 'yyyy-MM-dd HH:mm:ss.SSS') AS result
        """
      Then query result
        | result                  |
        | 2024-01-15 13:45:30.123 |

    Scenario: to_char with timestamp extracting date part only
      When query
        """
        SELECT to_char(TIMESTAMP'2024-12-31 23:59:59', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result     |
        | 2024-12-31 |

    Scenario: to_char with timestamp and AM/PM
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-01 00:00:00', 'a') AS result
        """
      Then query result
        | result |
        | AM     |

  Rule: to_varchar is an alias for to_char

    Scenario: to_varchar with date
      When query
        """
        SELECT to_varchar(DATE'2024-06-15', 'yyyy/MM/dd') AS result
        """
      Then query result
        | result     |
        | 2024/06/15 |

  Rule: NULL handling

    Scenario: to_char with NULL format returns NULL
      When query
        """
        SELECT to_char(TIMESTAMP'2024-01-15 10:30:00', NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_char with NULL date returns NULL
      When query
        """
        SELECT to_char(CAST(NULL AS DATE), 'yyyy-MM-dd') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_char with NULL timestamp returns NULL
      When query
        """
        SELECT to_char(CAST(NULL AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Numeric formatting - basic

    Scenario: to_char integer with zero-padded format
      When query
        """
        SELECT to_char(123, '000') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: to_char integer with leading zero padding
      When query
        """
        SELECT to_char(42, '00000') AS result
        """
      Then query result
        | result |
        | 00042  |

    Scenario: to_char zero with zero-padded format
      When query
        """
        SELECT to_char(0, '000') AS result
        """
      Then query result
        | result |
        | 000    |

    Scenario: to_char zero with optional digit format produces spaces
      When query
        """
        SELECT to_char(0, '999') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: to_char with decimal format
      When query
        """
        SELECT to_char(CAST(1.5 AS DECIMAL(3,1)), '0.00') AS result
        """
      Then query result
        | result |
        | 1.50   |

    Scenario: to_char with small decimal
      When query
        """
        SELECT to_char(CAST(0.001 AS DECIMAL(4,3)), '9.999') AS result
        """
      Then query result
        | result |
        | 0.001  |

  Rule: Numeric formatting - thousands separator

    Scenario: to_char with thousands separator
      When query
        """
        SELECT to_char(12345, '99,999') AS result
        """
      Then query result
        | result |
        | 12,345 |

    Scenario: to_char decimal with thousands separator
      When query
        """
        SELECT to_char(CAST(12345.67 AS DECIMAL(7,2)), '99,999.99') AS result
        """
      Then query result
        | result    |
        | 12,345.67 |

  Rule: Numeric formatting - sign handling

    Scenario: to_char negative without sign spec drops sign
      When query
        """
        SELECT to_char(-123, '999') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: to_char negative with S prefix
      When query
        """
        SELECT to_char(-123, 'S999') AS result
        """
      Then query result
        | result |
        | -123   |

    Scenario: to_char positive with S prefix shows plus
      When query
        """
        SELECT to_char(42, 'S999') AS result
        """
      Then query result
        | result |
        |  +42   |

    Scenario: to_char negative with trailing S
      When query
        """
        SELECT to_char(-123, '999S') AS result
        """
      Then query result
        | result |
        | 123-   |

  Rule: Numeric formatting - overflow

    Scenario: to_char overflow produces hash marks
      When query
        """
        SELECT to_char(1234, '99') AS result
        """
      Then query result
        | result |
        | ##     |

  Rule: Numeric formatting - currency

    Scenario: to_char with dollar sign
      When query
        """
        SELECT to_char(1234, '$9,999') AS result
        """
      Then query result
        | result |
        | $1,234 |

  Rule: Numeric formatting - to_varchar alias

    Scenario: to_varchar with integer
      When query
        """
        SELECT to_varchar(42, '000') AS result
        """
      Then query result
        | result |
        | 042    |

  Rule: Numeric formatting - NULL handling

    Scenario: to_char numeric with NULL format
      When query
        """
        SELECT to_char(123, NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_char NULL integer
      When query
        """
        SELECT to_char(CAST(NULL AS INT), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Numeric formatting - type coercion

    Scenario: to_char with BIGINT
      When query
        """
        SELECT to_char(CAST(9223372036854775807 AS BIGINT), '9999999999999999999') AS result
        """
      Then query result
        | result              |
        | 9223372036854775807 |

    Scenario: to_char with TINYINT
      When query
        """
        SELECT to_char(CAST(127 AS TINYINT), '000') AS result
        """
      Then query result
        | result |
        | 127    |

    Scenario: to_char with DECIMAL
      When query
        """
        SELECT to_char(CAST(1234.5678 AS DECIMAL(10,4)), '9,999.9999') AS result
        """
      Then query result
        | result      |
        | 1,234.5678  |

  Rule: Numeric formatting - edge cases

    Scenario: to_char negative with zero padding drops sign
      When query
        """
        SELECT to_char(-123, '0999') AS result
        """
      Then query result
        | result |
        | 0123   |

    Scenario: to_char decimal zero with optional integer
      When query
        """
        SELECT to_char(CAST(0 AS DECIMAL(5,2)), '99.99') AS result
        """
      Then query result
        | result |
        |  0.00  |

    Scenario: to_char negative decimal without sign spec drops sign
      When query
        """
        SELECT to_char(CAST(-0.5 AS DECIMAL(2,1)), '9.9') AS result
        """
      Then query result
        | result |
        | 0.5    |

    Scenario: to_char decimal overflow produces hash marks
      When query
        """
        SELECT to_char(CAST(3.14159 AS DECIMAL(10,5)), '9.999') AS result
        """
      Then query result
        | result |
        | #.###  |

    Scenario: to_char mixed zero and nine padding
      When query
        """
        SELECT to_char(42, '099') AS result
        """
      Then query result
        | result |
        | 042    |

    Scenario: to_char mixed nine and zero padding with space
      When query
        """
        SELECT to_char(42, '990') AS result
        """
      Then query result
        | result |
        |  42    |

    Scenario: to_char large number with thousands
      When query
        """
        SELECT to_char(CAST(99999999.99 AS DECIMAL(10,2)), '99,999,999.99') AS result
        """
      Then query result
        | result          |
        | 99,999,999.99   |

    Scenario: to_char negative with S suffix
      When query
        """
        SELECT to_char(42, '999S') AS result
        """
      Then query result
        | result |
        |  42+   |

    Scenario: to_char negative with MI prefix
      When query
        """
        SELECT to_char(-42, 'MI999') AS result
        """
      Then query result
        | result |
        |  -42   |

    Scenario: to_char positive with MI prefix shows space
      When query
        """
        SELECT to_char(42, 'MI999') AS result
        """
      Then query result
        | result |
        |   42   |

    Scenario: to_char with dollar and thousands
      When query
        """
        SELECT to_char(12345, '$99,999') AS result
        """
      Then query result
        | result  |
        | $12,345 |

    Scenario: to_char single digit with wide format
      When query
        """
        SELECT to_char(5, '9999') AS result
        """
      Then query result
        | result |
        |    5   |

    Scenario: to_char negative with S and zero padding
      When query
        """
        SELECT to_char(-42, 'S0000') AS result
        """
      Then query result
        | result |
        | -0042  |

    Scenario: to_char SMALLINT negative with S
      When query
        """
        SELECT to_char(CAST(-32768 AS SMALLINT), 'S00000') AS result
        """
      Then query result
        | result |
        | -32768 |

    Scenario: to_char SMALLINT negative without S drops sign
      When query
        """
        SELECT to_char(CAST(-32768 AS SMALLINT), '000000') AS result
        """
      Then query result
        | result |
        | 032768 |

  Rule: Numeric formatting - special values and casts

    Scenario: to_char with FLOAT NaN
      When query
        """
        SELECT to_char(CAST('NaN' AS DOUBLE), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_char with FLOAT Infinity
      When query
        """
        SELECT to_char(CAST('Infinity' AS DOUBLE), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_char with FLOAT negative Infinity
      When query
        """
        SELECT to_char(CAST('-Infinity' AS DOUBLE), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_char both args NULL
      When query
        """
        SELECT to_char(CAST(NULL AS INT), CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_char with negative zero
      When query
        """
        SELECT to_char(CAST(-0.0 AS DOUBLE), '9.9') AS result
        """
      Then query result
        | result |
        | 0.0    |

    Scenario: to_char integer as DOUBLE
      When query
        """
        SELECT to_char(CAST(42 AS DOUBLE), '999') AS result
        """
      Then query result
        | result |
        |  42    |

    Scenario: to_char integer as FLOAT
      When query
        """
        SELECT to_char(CAST(42 AS FLOAT), '999') AS result
        """
      Then query result
        | result |
        |  42    |

  Rule: Binary formatting

    Scenario: Binary to UTF-8
      When query
        """
        SELECT to_char(encode('hello world', 'utf-8'), 'utf-8') AS result
        """
      Then query result
        | result      |
        | hello world |

    Scenario: Binary to base64
      When query
        """
        SELECT to_char(x'48656C6C6F', 'base64') AS result
        """
      Then query result
        | result   |
        | SGVsbG8= |

    Scenario: Binary to hex
      When query
        """
        SELECT to_char(x'48656C6C6F', 'hex') AS result
        """
      Then query result
        | result     |
        | 48656C6C6F |

    Scenario: Empty binary to UTF-8
      When query
        """
        SELECT to_char(encode('', 'utf-8'), 'utf-8') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: Empty binary to base64
      When query
        """
        SELECT to_char(x'', 'base64') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: Empty binary to hex
      When query
        """
        SELECT to_char(x'', 'hex') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: NULL binary to UTF-8
      When query
        """
        SELECT to_char(CAST(NULL AS BINARY), 'utf-8') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL binary to base64
      When query
        """
        SELECT to_char(CAST(NULL AS BINARY), 'base64') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL binary to hex
      When query
        """
        SELECT to_char(CAST(NULL AS BINARY), 'hex') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: All zeros hex
      When query
        """
        SELECT to_char(x'0000000000', 'hex') AS result
        """
      Then query result
        | result     |
        | 0000000000 |

    Scenario: All FF hex
      When query
        """
        SELECT to_char(x'FFFFFFFFFFFF', 'hex') AS result
        """
      Then query result
        | result       |
        | FFFFFFFFFFFF |

    Scenario: Single byte hex
      When query
        """
        SELECT to_char(x'42', 'hex') AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: Single byte base64
      When query
        """
        SELECT to_char(x'42', 'base64') AS result
        """
      Then query result
        | result |
        | Qg==   |

    Scenario: Case insensitive format UTF-8
      When query
        """
        SELECT to_char(encode('hello', 'utf-8'), 'UTF-8') AS result
        """
      Then query result
        | result |
        | hello  |

    Scenario: Case insensitive format HEX
      When query
        """
        SELECT to_char(x'48656C6C6F', 'HEX') AS result
        """
      Then query result
        | result     |
        | 48656C6C6F |

    Scenario: Case insensitive format BASE64
      When query
        """
        SELECT to_char(x'48656C6C6F', 'BASE64') AS result
        """
      Then query result
        | result   |
        | SGVsbG8= |

    Scenario: Large binary hex length
      When query
        """
        SELECT length(to_char(encode(repeat('x', 1000), 'utf-8'), 'hex')) AS result
        """
      Then query result
        | result |
        | 2000   |
