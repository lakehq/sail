Feature: unix_timestamp with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: unix_timestamp — the argument may come from a column

    @function(columnargs)
    Scenario: unix_timestamp with the argument as a literal
      When query
        """
        SELECT unix_timestamp('2016-04-08', 'yyyy-MM-dd') AS result
        """
      Then query result ordered
        | result     |
        | 1460073600 |

    @function(columnargs)
    Scenario: unix_timestamp takes argument 2 from a column
      When query
        """
        SELECT unix_timestamp('2016-04-08', c) AS result FROM VALUES (1, 'yyyy-MM-dd'), (2, 'yyyy-MM-dd') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result     |
        | 1460073600 |
        | 1460073600 |

  Rule: Spark parsing, NULL format, and typed input contract

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: Unix timestamp parsing contract uses the one-argument default format
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          unix_timestamp('2024-01-15') AS date_only,
          unix_timestamp('2024-01-15 01:02:03') AS full_timestamp
        """
      Then query result
        | date_only | full_timestamp |
        | NULL      | 1705280523     |
      And query schema
        """
        root
         |-- date_only: long (nullable = true)
         |-- full_timestamp: long (nullable = true)
        """

    Scenario Outline: Unix timestamp parsing contract scalar formats have ANSI <ansi> nullability
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT
          unix_timestamp('2024-01-15', 'yyyy-MM-dd') AS parsed,
          unix_timestamp('2024-01-15', CAST(NULL AS STRING)) AS null_format
        """
      Then query result
        | parsed     | null_format |
        | 1705276800 | NULL        |
      And query schema
        """
        root
         |-- parsed: long (nullable = <parsed_nullable>)
         |-- null_format: long (nullable = true)
        """

      Examples:
        | ansi  | parsed_nullable |
        | true  | false           |
        | false | true            |

    Scenario: Unix timestamp parsing contract accepts a scalar value and format column
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT unix_timestamp('2024-01-15', format) AS result
        FROM VALUES
          (1, 'yyyy-MM-dd'),
          (2, CAST(NULL AS STRING))
        AS t(id, format)
        ORDER BY id
        """
      Then query result ordered
        | result     |
        | 1705276800 |
        | NULL       |
      And query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: Unix timestamp parsing contract accepts a value column and scalar format
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT unix_timestamp(value, 'yyyy-MM-dd') AS result
        FROM VALUES
          (1, '2024-01-15'),
          (2, CAST(NULL AS STRING))
        AS t(id, value)
        ORDER BY id
        """
      Then query result ordered
        | result     |
        | 1705276800 |
        | NULL       |
      And query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: Unix timestamp parsing contract handles value and format columns row by row with ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT unix_timestamp(value, format) AS result
        FROM VALUES
          (1, '2024-01-15', 'yyyy-MM-dd'),
          (2, '15/01/2024', 'dd/MM/yyyy'),
          (3, 'bad-value', 'yyyy-MM-dd'),
          (4, '2024-01-15', CAST(NULL AS STRING)),
          (5, CAST(NULL AS STRING), 'yyyy-MM-dd')
        AS t(id, value, format)
        ORDER BY id
        """
      Then query result ordered
        | result     |
        | 1705276800 |
        | 1705276800 |
        | NULL       |
        | NULL       |
        | NULL       |
      And query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: Unix timestamp parsing contract errors on a mixed value column with ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, unix_timestamp(value, 'yyyy-MM-dd') AS result
        FROM VALUES
          (1, '2024-01-15'),
          (2, 'bad-value')
        AS t(id, value)
        ORDER BY id
        """
      Then query error CANNOT_PARSE_TIMESTAMP

    Scenario Outline: Unix timestamp parsing contract ignores scalar and column formats for <type>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          unix_timestamp(<input>, format) AS column_format,
          unix_timestamp(<input>, CAST(NULL AS STRING)) AS scalar_null_format,
          unix_timestamp(<input>, 'invalid[') AS scalar_invalid_format
        FROM VALUES
          (1, 'yyyy-MM-dd'),
          (2, CAST(NULL AS STRING)),
          (3, 'invalid[')
        AS t(id, format)
        ORDER BY id
        """
      Then query result ordered
        | column_format | scalar_null_format | scalar_invalid_format |
        | <result>      | <result>            | <result>              |
        | <result>      | <result>            | <result>              |
        | <result>      | <result>            | <result>              |
      And query schema
        """
        root
         |-- column_format: long (nullable = true)
         |-- scalar_null_format: long (nullable = true)
         |-- scalar_invalid_format: long (nullable = false)
        """

      Examples:
        | type          | input                                | result     |
        | DATE          | DATE '2024-01-15'                    | 1705276800 |
        | TIMESTAMP     | TIMESTAMP '2024-01-15 01:02:03'      | 1705280523 |
        | TIMESTAMP_NTZ | TIMESTAMP_NTZ '2024-01-15 01:02:03'  | 1705280523 |

    Scenario Outline: Unix timestamp parsing contract rejects a <case> format for a typed DATE input
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT unix_timestamp(
          DATE '2024-01-15',
          <format>
        )
        """
      Then query error (?i)(DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE|expects.*STRING|requires.*STRING|must.*STRING)

      Examples:
        | case       | format             |
        | atomic     | 123                |
        | collection | array('yyyy-MM-dd') |

    Scenario Outline: Unix timestamp parsing contract does not evaluate an ignored format for <case>
      Given config spark.sql.session.timeZone = UTC
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT unix_timestamp(
          <input>,
          CAST(raise_error(CAST(id AS STRING)) AS STRING)
        ) AS result
        FROM range(1)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case       | input             | result |
        | typed DATE | DATE '1970-01-01' | 0      |
        | NULL       | NULL              | NULL   |

    Scenario: Unix timestamp parsing contract applies the session zone to typed inputs
      Given config spark.sql.session.timeZone = America/Los_Angeles
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          unix_timestamp(DATE '2024-01-15', CAST(NULL AS STRING)) AS date_null_format,
          unix_timestamp(DATE '2024-01-15', 'invalid[') AS date_invalid_format,
          unix_timestamp(TIMESTAMP '2024-01-15 01:02:03', 'invalid[') AS timestamp_result,
          unix_timestamp(TIMESTAMP_NTZ '2024-01-15 01:02:03', 'invalid[') AS timestamp_ntz_result
        """
      Then query result
        | date_null_format | date_invalid_format | timestamp_result | timestamp_ntz_result |
        | 1705305600       | 1705305600          | 1705309323       | 1705280523           |
      And query schema
        """
        root
         |-- date_null_format: long (nullable = true)
         |-- date_invalid_format: long (nullable = false)
         |-- timestamp_result: long (nullable = false)
         |-- timestamp_ntz_result: long (nullable = false)
        """

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null string literal yields a bigint
      When query
        """
        SELECT unix_timestamp('2024-01-15', 'yyyy-MM-dd') AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    @sail-bug
    Scenario: a non-null string column yields a bigint
      When query
        """
        SELECT unix_timestamp(date_format(CAST(id AS TIMESTAMP), 'yyyy-MM-dd'), 'yyyy-MM-dd') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: a nullable string column stays nullable
      When query
        """
        SELECT unix_timestamp(c, 'yyyy-MM-dd') AS result FROM VALUES ('2024-01-15'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

  # Spark 4.2.0 datetimeExpressions.scala, ToTimestamp.eval: the parsed microseconds (or the
  # TIMESTAMP value) are divided by MICROS_PER_SECOND with Java's `/`, which truncates
  # toward zero. Half a second before the epoch is 0, not floor(-0.5) = -1. (unix_seconds
  # uses Math.floorDiv instead.)
  Rule: unix_timestamp truncates sub-second instants toward zero

    Background:
      Given config spark.sql.session.timeZone = UTC

    @sail-bug
    Scenario Outline: unix_timestamp truncates a parsed pre-epoch fraction toward zero: <case>
      When query
        """
        SELECT unix_timestamp('<value>', 'yyyy-MM-dd HH:mm:ss.S') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case               | value                 | result |
        | half second before | 1969-12-31 23:59:59.5 | 0      |
        | 1.5 seconds before | 1969-12-31 23:59:58.5 | -1     |

    @sail-bug
    Scenario: unix_timestamp truncates parsed fractions toward zero from a column
      When query
        """
        SELECT unix_timestamp(c, 'yyyy-MM-dd HH:mm:ss.S') AS result
        FROM VALUES (1, '1969-12-31 23:59:59.5'), (2, '1970-01-01 00:00:01.5'), (3, '1969-12-31 23:59:58.5') AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result |
        | 0      |
        | 1      |
        | -1     |

    Scenario: unix_timestamp truncates TIMESTAMP fractions toward zero from a column
      When query
        """
        SELECT unix_timestamp(c) AS result
        FROM VALUES (1, TIMESTAMP '1969-12-31 23:59:59.5'), (2, TIMESTAMP '1970-01-01 00:00:01.5'), (3, NULL) AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result |
        | 0      |
        | 1      |
        | NULL   |

  Rule: unix_timestamp rejects out-of-range fields per ANSI mode

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: unix_timestamp rejects <case> under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT unix_timestamp('<value>', 'yyyy-MM-dd') AS result
        """
      Then query error \[CANNOT_PARSE_TIMESTAMP

      Examples:
        | case        | value      |
        | February 30 | 2024-02-30 |
        | month 13    | 2024-13-01 |

    Scenario Outline: unix_timestamp returns NULL for <case> without ANSI
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT unix_timestamp('<value>', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case        | value      |
        | February 30 | 2024-02-30 |
        | month 13    | 2024-13-01 |

    @sail-bug
    Scenario: unix_timestamp rejects a numeric argument with Spark's type-mismatch error
      When query
        """
        SELECT unix_timestamp(123) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE

  # ToTimestamp parses STRING and converts DATE (daysToMicros) with the session zoneId;
  # a TIMESTAMP_NTZ is its wall clock read as UTC, so it never depends on the zone.
  Rule: unix_timestamp interprets local values in the session time zone

    Scenario Outline: unix_timestamp of string, DATE, TIMESTAMP and TIMESTAMP_NTZ in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT
          unix_timestamp('2024-01-15 01:02:03') AS str,
          unix_timestamp(DATE '2024-07-15') AS dt,
          unix_timestamp(TIMESTAMP '2024-01-15 01:02:03') AS ltz,
          unix_timestamp(TIMESTAMP_NTZ '2024-01-15 01:02:03') AS ntz
        """
      Then query result
        | str   | dt   | ltz   | ntz        |
        | <str> | <dt> | <str> | 1705280523 |

      Examples:
        | zone                | str        | dt         |
        | America/Los_Angeles | 1705309323 | 1721026800 |
        | Asia/Kolkata        | 1705260723 | 1720981800 |
        | Pacific/Chatham     | 1705231023 | 1720955700 |
        | Pacific/Pago_Pago   | 1705320123 | 1721041200 |

    Scenario Outline: unix_timestamp parses string rows in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT unix_timestamp(c, 'yyyy-MM-dd HH:mm') AS result
        FROM VALUES (1, '2024-01-15 01:02'), (2, '2024-07-15 23:59'), (3, '1970-01-01 00:00') AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result |
        | <r1>   |
        | <r2>   |
        | <r3>   |

      Examples:
        | zone                | r1         | r2         | r3     |
        | America/Los_Angeles | 1705309320 | 1721113140 | 28800  |
        | Asia/Kolkata        | 1705260720 | 1721068140 | -19800 |
        | Pacific/Chatham     | 1705231020 | 1721042040 | -45900 |
        | Pacific/Pago_Pago   | 1705320120 | 1721127540 | 39600  |

    Scenario Outline: unix_timestamp converts DATE rows in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT unix_timestamp(c) AS result
        FROM VALUES (1, DATE '2024-01-15'), (2, DATE '2024-07-15'), (3, DATE '1970-01-01') AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result |
        | <r1>   |
        | <r2>   |
        | <r3>   |

      Examples:
        | zone                | r1         | r2         | r3     |
        | America/Los_Angeles | 1705305600 | 1721026800 | 28800  |
        | Asia/Kolkata        | 1705257000 | 1720981800 | -19800 |
        | Pacific/Chatham     | 1705227300 | 1720955700 | -45900 |
        | Pacific/Pago_Pago   | 1705316400 | 1721041200 | 39600  |

    # A gap time moves forward by the gap; an overlap time takes the earlier offset.
    Scenario: unix_timestamp resolves Los Angeles DST transitions row by row
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT unix_timestamp(c) AS result
        FROM VALUES (1, '2024-03-10 02:30:00'), (2, '2024-11-03 01:30:00'), (3, '2024-03-10 01:59:59') AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result     |
        | 1710066600 |
        | 1730622600 |
        | 1710064799 |

    Scenario: unix_timestamp resolves Chatham DST transitions
      Given config spark.sql.session.timeZone = Pacific/Chatham
      When query
        """
        SELECT unix_timestamp('2024-09-29 03:00:00') AS gap, unix_timestamp('2024-04-07 03:00:00') AS overlap
        """
      Then query result
        | gap        | overlap    |
        | 1727532900 | 1712409300 |
