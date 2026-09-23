Feature: to_date with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: to_date — the argument may come from a column

    @function(columnargs)
    Scenario: to_date with the argument as a literal
      When query
        """
        SELECT to_date('2016-12-31', 'yyyy-MM-dd') AS result
        """
      Then query result ordered
        | result     |
        | 2016-12-31 |

    @function(columnargs)
    Scenario: to_date takes argument 2 from a column
      When query
        """
        SELECT to_date('2016-12-31', c) AS result FROM VALUES (1, 'yyyy-MM-dd'), (2, 'yyyy-MM-dd') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result     |
        | 2016-12-31 |
        | 2016-12-31 |

  Rule: Formatted numeric date keys are implicitly cast to string

    Scenario Outline: To date parses a formatted <case> literal
      When query
        """
        SELECT to_date(<value>, 'yyyyMMdd') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case         | value                            | result     |
        | integer      | 20260220                         | 2026-02-20 |
        | bigint       | CAST(20251201 AS BIGINT)          | 2025-12-01 |
        | decimal      | CAST(20260220 AS DECIMAL(8, 0))   | 2026-02-20 |
        | typed null   | CAST(NULL AS BIGINT)             | NULL       |
        | untyped null | NULL                             | NULL       |

    Scenario Outline: To date parses numeric columns with literal and dynamic formats with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT
          id,
          to_date(value, 'yyyyMMdd') AS literal_format,
          to_date(value, format) AS dynamic_format
        FROM VALUES
          (1, 20260220, 'yyyyMMdd'),
          (2, 20261201, 'yyyyddMM'),
          (3, CAST(NULL AS INT), 'yyyyMMdd'),
          (4, 20251201, CAST(NULL AS STRING))
          AS t(id, value, format)
        ORDER BY id
        """
      Then query result ordered
        | id | literal_format | dynamic_format |
        | 1  | 2026-02-20     | 2026-02-20     |
        | 2  | 2026-12-01     | 2026-01-12     |
        | 3  | NULL           | NULL           |
        | 4  | 2025-12-01     | NULL           |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario: To date parses a numeric literal using a format column
      When query
        """
        SELECT id, to_date(20261201, format) AS result
        FROM VALUES
          (1, 'yyyyMMdd'),
          (2, 'yyyyddMM'),
          (3, CAST(NULL AS STRING))
          AS t(id, format)
        ORDER BY id
        """
      Then query result ordered
        | id | result     |
        | 1  | 2026-12-01 |
        | 2  | 2026-01-12 |
        | 3  | NULL       |

    Scenario: To date returns NULL for invalid numeric date keys with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          to_date(20260229, 'yyyyMMdd') AS literal_value,
          to_date(value, 'yyyyMMdd') AS column_value
        FROM VALUES (20260229), (20260015) AS t(value)
        """
      Then query result
        | literal_value | column_value |
        | NULL          | NULL         |
        | NULL          | NULL         |

    Scenario Outline: To date rejects an invalid numeric <case> with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_date(<value>, 'yyyyMMdd') AS result
        FROM VALUES (20260229), (20260015) AS t(value)
        """
      Then query error (?i)(CANNOT_PARSE_TIMESTAMP|invalid parsed (date|month)|out of range|DateValue)

      Examples:
        | case    | value    |
        | literal | 20260229 |
        | column  | value    |

  Rule: Invalid string input follows ANSI mode

    Scenario Outline: To date returns NULL for <case> with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_date(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                       | args                    |
        | default invalid leap day   | '2026-02-29'            |
        | default invalid month      | '2026-00-15'            |
        | formatted invalid leap day | '20260229', 'yyyyMMdd' |
        | formatted invalid month    | '20260015', 'yyyyMMdd' |

    Scenario: To date returns NULL for invalid string columns with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          id,
          to_date(value) AS default_result,
          to_date(compact_value, 'yyyyMMdd') AS formatted_result
        FROM VALUES
          (1, '2026-01-01', '20260101'),
          (2, '2026-02-29', '20260229'),
          (3, '2026-00-15', '20260015'),
          (4, CAST(NULL AS STRING), CAST(NULL AS STRING))
          AS t(id, value, compact_value)
        ORDER BY id
        """
      Then query result ordered
        | id | default_result | formatted_result |
        | 1  | 2026-01-01     | 2026-01-01       |
        | 2  | NULL           | NULL             |
        | 3  | NULL           | NULL             |
        | 4  | NULL           | NULL             |

    Scenario Outline: To date rejects <case> literal with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_date(<args>) AS result
        """
      Then query error (?i)(CANNOT_PARSE_TIMESTAMP|CAST_INVALID_INPUT|invalid parsed (date|month)|out of range|DateValue)

      Examples:
        | case                       | args                    |
        | default invalid leap day   | '2026-02-29'            |
        | default invalid month      | '2026-00-15'            |
        | formatted invalid leap day | '20260229', 'yyyyMMdd' |
        | formatted invalid month    | '20260015', 'yyyyMMdd' |

    Scenario Outline: To date rejects <case> column with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <call> AS result FROM VALUES ('<value>') AS t(value)
        """
      Then query error (?i)(CANNOT_PARSE_TIMESTAMP|CAST_INVALID_INPUT|invalid parsed (date|month)|out of range|DateValue)

      Examples:
        | case                       | call                       | value      |
        | default invalid leap day   | to_date(value)             | 2026-02-29 |
        | default invalid month      | to_date(value)             | 2026-00-15 |
        | formatted invalid leap day | to_date(value, 'yyyyMMdd') | 20260229   |
        | formatted invalid month    | to_date(value, 'yyyyMMdd') | 20260015   |

    Scenario Outline: To date <case> is nullable with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_date(<args>) AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

      Examples:
        | case            | args                    |
        | default format  | '2026-01-01'            |
        | explicit format | '20260101', 'yyyyMMdd' |

  Rule: Explicit NULL format semantics

    Scenario: To date NULL format semantics distinguishes omitted and explicit format
      When query
        """
        SELECT
          to_date('2024-01-15') AS omitted_format,
          to_date('2024-01-15', CAST(NULL AS STRING)) AS explicit_null_format
        """
      Then query result
        | omitted_format | explicit_null_format |
        | 2024-01-15     | NULL                 |

    Scenario: To date NULL format semantics propagates a column format for a scalar value
      When query
        """
        SELECT id, format, to_date('2024-01-15', format) AS result
        FROM VALUES
          (1, 'yyyy-MM-dd'),
          (2, CAST(NULL AS STRING))
          AS t(id, format)
        ORDER BY id
        """
      Then query result ordered
        | id | format     | result     |
        | 1  | yyyy-MM-dd | 2024-01-15 |
        | 2  | NULL       | NULL       |

    Scenario: To date NULL format semantics propagates a scalar NULL format for column values
      When query
        """
        SELECT id, value, to_date(value, CAST(NULL AS STRING)) AS result
        FROM VALUES
          (1, '2024-01-15'),
          (2, '2024-01-16'),
          (3, CAST(NULL AS STRING))
          AS t(id, value)
        ORDER BY id
        """
      Then query result ordered
        | id | value      | result |
        | 1  | 2024-01-15 | NULL   |
        | 2  | 2024-01-16 | NULL   |
        | 3  | NULL       | NULL   |

    Scenario: To date NULL format semantics propagates paired value and format columns
      When query
        """
        SELECT id, to_date(value, format) AS result
        FROM VALUES
          (1, '2024-01-15', 'yyyy-MM-dd'),
          (2, '15/01/2024', 'dd/MM/yyyy'),
          (3, '2024-01-15', CAST(NULL AS STRING)),
          (4, CAST(NULL AS STRING), 'yyyy-MM-dd')
          AS t(id, value, format)
        ORDER BY id
        """
      Then query result ordered
        | id | result     |
        | 1  | 2024-01-15 |
        | 2  | 2024-01-15 |
        | 3  | NULL       |
        | 4  | NULL       |

    Scenario Outline: To date NULL format semantics ignores format for a <type> input
      When query
        """
        SELECT id, format, to_date(<value>, format) AS result
        FROM VALUES
          (1, 'yyyy-MM-dd'),
          (2, CAST(NULL AS STRING)),
          (3, 'invalid_format')
          AS t(id, format)
        ORDER BY id
        """
      Then query result ordered
        | id | format         | result     |
        | 1  | yyyy-MM-dd     | 2024-01-15 |
        | 2  | NULL           | 2024-01-15 |
        | 3  | invalid_format | 2024-01-15 |

      Examples:
        | type      | value                                |
        | DATE      | DATE '2024-01-15'                    |
        | TIMESTAMP | TIMESTAMP '2024-01-15 23:45:00'      |

    Scenario: To date typed DATE rejects a complex format
      When query
        """
        SELECT to_date(
          DATE '2024-01-15',
          array('yyyy-MM-dd')
        )
        """
      Then query error (?i)(DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE|format.*string.*(array|list)|expects.*STRING.*(array|list)|requires.*STRING.*(array|list))

    Scenario: To date typed DATE accepts an atomic format through string coercion
      When query
        """
        SELECT to_date(DATE '2024-01-15', 123) AS result
        """
      Then query result
        | result     |
        | 2024-01-15 |

    Scenario Outline: To date NULL format semantics short-circuits a bad value with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_date('not-a-date', CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | ansi  |
        | true  |
        | false |

  @function(nullability)
  Rule: Output schema

    Scenario Outline: a typed DATE with a literal format respects ANSI <ansi> nullability
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_date(DATE '2024-01-15', 'yyyy-MM-dd') AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = <nullable>)
        """

      Examples:
        | ansi  | nullable |
        | false | true     |
        | true  | false    |

    Scenario: a typed DATE with a nullable format stays nullable in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_date(DATE '2024-01-15', format) AS result
        FROM VALUES
          ('yyyy-MM-dd'),
          (CAST(NULL AS STRING))
          AS t(format)
        """
      Then query result
        | result     |
        | 2024-01-15 |
        | 2024-01-15 |
      And query schema
        """
        root
         |-- result: date (nullable = true)
        """

    Scenario: a non-null string literal yields a date
      When query
        """
        SELECT to_date('2024-01-15') AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

    Scenario: a non-null string column yields a date
      When query
        """
        SELECT to_date(date_format(CAST(id AS TIMESTAMP), 'yyyy-MM-dd')) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

    Scenario: a nullable string column stays nullable
      When query
        """
        SELECT to_date(c) AS result FROM VALUES ('2024-01-15'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

  Rule: Result values (migrated from test_to_date.txt doctests)

    # `name` is a separate slot because Spark rewrites the derived column name:
    # TIMESTAMP_LTZ becomes TIMESTAMP and the value is shown already converted.
    Scenario Outline: Result values: <case>
      When query
        """
        SELECT to_date(<args>)
        """
      Then query result
        | to_date(<name>) |
        | <result>        |

      Examples:
        | case                        | args                                                        | name                                                   | result     |
        | to_date doctest #1 (result) | TIMESTAMP_NTZ '2025-11-02 23:30:45.123456'                  | TIMESTAMP_NTZ '2025-11-02 23:30:45.123456'             | 2025-11-02 |
        | to_date doctest #2 (result) | TIMESTAMP_LTZ '2025-11-02 23:30:45.123456'                  | TIMESTAMP '2025-11-02 23:30:45.123456'                 | 2025-11-02 |
        | to_date doctest #3 (result) | TIMESTAMP_LTZ '2025-11-02 23:30:45.123456 America/New_York' | TIMESTAMP '2025-11-03 04:30:45.123456'                 | 2025-11-03 |
        | to_date doctest #4 (result) | TIMESTAMP '2025-11-03 23:30:45.123456', 'invalid_format'    | TIMESTAMP '2025-11-03 23:30:45.123456', invalid_format | 2025-11-03 |

    Scenario: to_date doctest #5 (result)
      When query
        """
        SELECT ts, CAST(ts AS TIMESTAMP_NTZ) AS ts_ntz, CAST(ts AS TIMESTAMP_LTZ) AS ts_ltz, to_date(CAST(ts AS TIMESTAMP_NTZ)) AS date_ntz, to_date(CAST(ts AS TIMESTAMP_LTZ)) AS date_ltz FROM VALUES ('2025-11-02 23:30:45.123456'), ('2025-11-02 23:30:45.123456-08:00'), ('2025-11-02 23:30:45.123456+01:00') AS t(ts)
        """
      Then query result
        | ts                               | ts_ntz                     | ts_ltz                     | date_ntz   | date_ltz   |
        | 2025-11-02 23:30:45.123456       | 2025-11-02 23:30:45.123456 | 2025-11-02 23:30:45.123456 | 2025-11-02 | 2025-11-02 |
        | 2025-11-02 23:30:45.123456-08:00 | 2025-11-02 23:30:45.123456 | 2025-11-03 07:30:45.123456 | 2025-11-02 | 2025-11-03 |
        | 2025-11-02 23:30:45.123456+01:00 | 2025-11-02 23:30:45.123456 | 2025-11-02 22:30:45.123456 | 2025-11-02 | 2025-11-02 |

  Rule: Valid input parses

    Scenario: ISO date
      When query
      """
      SELECT to_date('2024-01-15') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

    Scenario: With format
      When query
      """
      SELECT to_date('15/01/2024', 'dd/MM/yyyy') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

    Scenario: Cast from timestamp
      When query
      """
      SELECT to_date(TIMESTAMP '2024-01-15 10:30:00') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

    Scenario: Cast from TIMESTAMP_NTZ preserves wall clock
      When query
      """
      SELECT to_date(TIMESTAMP_NTZ '2025-11-02 23:30:45.123456') AS result
      """
      Then query result
      | result     |
      | 2025-11-02 |

    Scenario: Cast from TIMESTAMP_LTZ in UTC session preserves wall clock
      When query
      """
      SELECT to_date(TIMESTAMP_LTZ '2025-11-02 23:30:45.123456') AS result
      """
      Then query result
      | result     |
      | 2025-11-02 |

    Scenario: TIMESTAMP_LTZ with offset converts to session timezone
      When query
      """
      SELECT to_date(TIMESTAMP_LTZ '2025-11-02 23:30:45.123456 America/New_York') AS result
      """
      Then query result
      | result     |
      | 2025-11-03 |

  Rule: Numeric / boolean args coerce to string (like Spark)

    Scenario: Integer with format coerces and parses
      When query
      """
      SELECT to_date(20240115, 'yyyyMMdd') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

  Rule: Invalid input honors ANSI mode
    # to_date errors on invalid input under ANSI and returns NULL otherwise.

    Scenario: Garbage string under ANSI on errors
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT to_date('not-a-date') AS result
      """
      Then query error .*

    Scenario: Garbage string under ANSI off returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT to_date('not-a-date') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Format mismatch under ANSI on errors
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT to_date('2024-01-15', 'dd/MM/yyyy') AS result
      """
      Then query error .*

    Scenario: Format mismatch under ANSI off returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT to_date('2024-01-15', 'dd/MM/yyyy') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Numeric invalid under ANSI on errors
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT to_date(20240115) AS result
      """
      Then query error .*

    @sail-bug
    Scenario: Numeric invalid under ANSI off returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT to_date(20240115) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: NULL input propagates

    Scenario: NULL input returns NULL
      When query
      """
      SELECT to_date(CAST(NULL AS STRING)) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: NULL format returns NULL
      When query
      """
      SELECT to_date('2024-01-15', NULL) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Invalid input throws

    Scenario: Garbage string raises error
      When query
      """
      SELECT to_date('not-a-date')
      """
      Then query error CAST_INVALID_INPUT|error in SQL parser|cannot be cast

    @sail-bug
    Scenario: Format mismatch raises error
      When query
      """
      SELECT to_date('2024-01-15', 'dd/MM/yyyy')
      """
      Then query error CANNOT_PARSE_TIMESTAMP|invalid characters|CONVERSION_INVALID_INPUT|cannot be parsed

  Rule: Without a format, to_date follows the lenient STRING to DATE cast
    # Spark 4.2.0 datetimeExpressions.scala: ParseToDate without a format is
    # Cast(left, DateType, ansi). SparkDateTimeUtils.stringToDate trims every char <= ' ' at both
    # ends, accepts a signed year of 4 to 7 digits, and ignores anything after the day once a ' '
    # or 'T' follows it. The DATE range reaches +5881580-07-11.

    @sail-bug
    Scenario Outline: to_date accepts the lenient cast form <case> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_date(<value>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                    | ansi  | value                         | result         |
        | surrounding spaces      | true  | '  2024-01-15  '              | 2024-01-15     |
        | surrounding spaces      | false | '  2024-01-15  '              | 2024-01-15     |
        | leading newline         | true  | concat(chr(10), '2024-01-16') | 2024-01-16     |
        | T and a time            | true  | '2024-01-17T10:30'            | 2024-01-17     |
        | T and garbage           | false | '2024-01-18Tgarbage'          | 2024-01-18     |
        | T and a zoned time      | true  | '2024-01-19T10:30:45Z'        | 2024-01-19     |
        | six-digit year          | true  | '294248-01-01'                | +294248-01-01  |
        | largest date            | false | '5881580-07-11'               | +5881580-07-11 |
        | negative six-digit year | true  | '-290308-12-21'               | -290308-12-21  |

    @sail-bug
    Scenario: to_date applies the lenient cast per row with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT i, to_date(v) AS result
        FROM VALUES
          (1, ' 2024-01-15 '),
          (2, '2024-01-16T99'),
          (3, '2024-01-17 garbage'),
          (4, '294248-01-01'),
          (5, '2024-1-8'),
          (6, '2024-02-30')
          AS x(i, v)
        ORDER BY i
        """
      Then query result ordered
        | i | result        |
        | 1 | 2024-01-15    |
        | 2 | 2024-01-16    |
        | 3 | 2024-01-17    |
        | 4 | +294248-01-01 |
        | 5 | 2024-01-08    |
        | 6 | NULL          |

  Rule: A numeric or boolean value is cast to STRING before parsing, never read as a day count
    # Spark 4.2.0 datetimeExpressions.scala: ParseToDate is ImplicitCastInputTypes over
    # STRING/DATE/TIMESTAMP/TIMESTAMP_NTZ, so 1 becomes '1' and true becomes 'true', which
    # stringToDate rejects (fewer than 4 year digits): NULL with ANSI off, CAST_INVALID_INPUT on.

    @sail-bug
    Scenario Outline: to_date of <case> input returns NULL with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_date(<value>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case    | value |
        | integer | 1     |
        | double  | 1.5D  |
        | boolean | true  |

    @sail-bug
    Scenario Outline: to_date of <case> input fails the string cast with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_date(<value>) AS result
        """
      Then query error CAST_INVALID_INPUT

      Examples:
        | case    | value |
        | integer | 1     |
        | boolean | true  |

    @sail-bug
    Scenario: to_date of an integer column returns NULL per row with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT i, to_date(v) AS result
        FROM VALUES (1, 1), (2, 20240115), (3, CAST(NULL AS INT)) AS x(i, v)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | NULL   |
        | 2 | NULL   |
        | 3 | NULL   |

    @sail-bug
    @function(nullability)
    Scenario: to_date of an integer is nullable with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_date(1) AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

  @function(nullability)
  Rule: A literal string with a literal format cannot be NULL with ANSI enabled

    # Spark 4.2.0 datetimeExpressions.scala: ToTimestamp.nullable is
    # `if (failOnError) children.exists(_.nullable) else true`, and the outer Cast to DATE keeps it.
    @sail-bug
    Scenario: to_date of a string literal with a format is not nullable with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_date('2024-01-15', 'yyyy-MM-dd') AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """

  Rule: A TIMESTAMP input takes its date in the session time zone

    Scenario Outline: to_date of an offset timestamp string takes the date in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT to_date(to_timestamp('2024-06-15 12:00:00+00:00')) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | zone              | result     |
        | Pacific/Chatham   | 2024-06-16 |
        | Pacific/Pago_Pago | 2024-06-15 |

    Scenario: to_date of a timestamp column crosses midnight per row in a 45-minute offset zone
      Given config spark.sql.session.timeZone = Pacific/Chatham
      When query
        """
        SELECT i, to_date(to_timestamp(s)) AS result
        FROM VALUES (1, '2024-06-15 11:00:00Z'), (2, '2024-06-15 11:30:00Z') AS x(i, s)
        ORDER BY i
        """
      Then query result ordered
        | i | result     |
        | 1 | 2024-06-15 |
        | 2 | 2024-06-16 |

    Scenario: to_date with a time format keeps the parsed local date in a 45-minute offset zone
      Given config spark.sql.session.timeZone = Pacific/Chatham
      When query
        """
        SELECT to_date('2024-06-15 23:30', 'yyyy-MM-dd HH:mm') AS result
        """
      Then query result
        | result     |
        | 2024-06-15 |

    # timestamp_seconds(1700000000) is 2023-11-14 22:13:20 UTC, already 2023-11-15 in Chatham
    # (+13:45). Sail keeps the UTC date: the timestamp_seconds result appears to carry a fixed UTC
    # zone that overrides the session zone, while to_timestamp inputs are converted correctly.
    @sail-bug
    Scenario: to_date of a timestamp_seconds value takes the date in a 45-minute offset zone
      Given config spark.sql.session.timeZone = Pacific/Chatham
      When query
        """
        SELECT to_date(timestamp_seconds(1700000000)) AS result
        """
      Then query result
        | result     |
        | 2023-11-15 |
