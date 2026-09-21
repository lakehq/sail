Feature: try_to_timestamp
  Safe variant of to_timestamp that returns NULL on parse failure.

  Rule: Single-argument form parses with default formats

    Scenario Outline: Single argument: <case>
      When query
        """
        SELECT try_to_timestamp(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                | input                        | result                     |
        | ISO timestamp parses                | '2024-01-15 10:30:45'        | 2024-01-15 10:30:45        |
        | Date-only parses with midnight time | '2024-01-15'                 | 2024-01-15 00:00:00        |
        | Microseconds preserved              | '2024-01-15 10:30:45.123456' | 2024-01-15 10:30:45.123456 |
        | Cast from date                      | DATE '2024-01-15'            | 2024-01-15 00:00:00        |
        | Garbage returns NULL                | 'not-a-timestamp'            | NULL                       |
        | Empty string returns NULL           | ''                           | NULL                       |
        | Invalid month returns NULL          | '2024-13-15 10:30:45'        | NULL                       |
        | NULL input                          | CAST(NULL AS STRING)         | NULL                       |

    Scenario: ISO timestamp parses
      When query
        """
        SELECT try_to_timestamp('2024-01-15 10:30:45') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:45 |

    Scenario: Date-only parses with midnight time
      When query
        """
        SELECT try_to_timestamp('2024-01-15') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 00:00:00 |

    Scenario: Microseconds preserved
      When query
        """
        SELECT try_to_timestamp('2024-01-15 10:30:45.123456') AS result
        """
      Then query result
        | result                     |
        | 2024-01-15 10:30:45.123456 |

    Scenario: Garbage returns NULL
      When query
        """
        SELECT try_to_timestamp('not-a-timestamp') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Empty string returns NULL
      When query
        """
        SELECT try_to_timestamp('') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Invalid month returns NULL
      When query
        """
        SELECT try_to_timestamp('2024-13-15 10:30:45') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL input
      When query
        """
        SELECT try_to_timestamp(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Two-argument form parses with format string

    Scenario Outline: Two arguments: <case>
      When query
        """
        SELECT try_to_timestamp(<value>, <format>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                | value                 | format                | result              |
        | Spark format yyyy-MM-dd HH:mm:ss    | '2024-01-15 10:30:45' | 'yyyy-MM-dd HH:mm:ss' | 2024-01-15 10:30:45 |
        | Custom format dd/MM/yyyy            | '15/01/2024 10:30:45' | 'dd/MM/yyyy HH:mm:ss' | 2024-01-15 10:30:45 |
        | Format mismatch returns NULL        | '2024-01-15'          | 'dd/MM/yyyy'          | NULL                |
        | NULL value with format returns NULL | CAST(NULL AS STRING)  | 'yyyy-MM-dd HH:mm:ss' | NULL                |
        | NULL format returns NULL            | '2024-01-15 10:30:45' | NULL                  | NULL                |

    Scenario: Spark format yyyy-MM-dd HH:mm:ss
      When query
        """
        SELECT try_to_timestamp('2024-01-15 10:30:45', 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:45 |

    Scenario: Custom format dd/MM/yyyy
      When query
        """
        SELECT try_to_timestamp('15/01/2024 10:30:45', 'dd/MM/yyyy HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:45 |

    Scenario: Format mismatch returns NULL
      When query
        """
        SELECT try_to_timestamp('2024-01-15', 'dd/MM/yyyy') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL value with format returns NULL
      When query
        """
        SELECT try_to_timestamp(CAST(NULL AS STRING), 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Non-finite floating-point string literals return NULL

    Scenario Outline: Non-finite literal: <case>
      When query
        """
        SELECT try_to_timestamp(<input>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                  | input       |
        | NaN string returns NULL               | 'NaN'       |
        | Infinity string returns NULL          | 'Infinity'  |
        | Negative Infinity string returns NULL | '-Infinity' |

    Scenario: NaN string returns NULL
      When query
        """
        SELECT try_to_timestamp('NaN') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Infinity string returns NULL
      When query
        """
        SELECT try_to_timestamp('Infinity') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Negative Infinity string returns NULL
      When query
        """
        SELECT try_to_timestamp('-Infinity') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Per-row format (column-expression format)

    Scenario: Different format per row all parse
      When query
        """
        SELECT try_to_timestamp(d, f) AS result FROM VALUES
          ('2024-01-15 10:30:00', 'yyyy-MM-dd HH:mm:ss'),
          ('15/01/2024 10:30:00', 'dd/MM/yyyy HH:mm:ss') AS t(d, f)
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:00 |
        | 2024-01-15 10:30:00 |

    Scenario: Per-row format with NULL format propagates to NULL
      When query
        """
        SELECT try_to_timestamp(d, f) AS result FROM VALUES
          ('2024-01-15 10:30:00', 'yyyy-MM-dd HH:mm:ss'),
          ('2024-01-16 11:00:00', CAST(NULL AS STRING)) AS t(d, f)
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:00 |
        | NULL                |

  Rule: Multi-row arrays handle per-row failures

    Scenario: Mixed valid and invalid in batch
      When query
        """
        SELECT try_to_timestamp(t) AS result FROM VALUES
          ('2024-01-15 10:30:45'),
          ('garbage'),
          ('2024-01-15'),
          (NULL) AS x(t)
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:45 |
        | NULL                |
        | 2024-01-15 00:00:00 |
        | NULL                |

  Rule: Result values (migrated from test_try_to_timestamp.txt doctests)

    Scenario: try_to_timestamp doctest #1 (result) — input LTZ timestamps under Amsterdam
      Given config spark.sql.session.timeZone = Europe/Amsterdam
      When query
        """
        SELECT ts FROM VALUES (TIMESTAMP_LTZ '2023-01-01 10:00:00'), (TIMESTAMP_LTZ '2023-01-01 03:00:00') AS t(ts)
        """
      Then query result
        | ts                  |
        | 2023-01-01 10:00:00 |
        | 2023-01-01 03:00:00 |

    Scenario Outline: Result values: <case>
      Given config spark.sql.session.timeZone = Europe/Amsterdam
      And config spark.sql.timestampType = <timestamp_type>
      When query
        """
        SELECT try_to_timestamp(ts) AS r FROM VALUES (TIMESTAMP_LTZ '2023-01-01 10:00:00'), (TIMESTAMP_LTZ '2023-01-01 03:00:00') AS t(ts)
        """
      Then query result
        | r                   |
        | 2023-01-01 10:00:00 |
        | 2023-01-01 03:00:00 |

      Examples:
        | case                                                               | timestamp_type |
        | try_to_timestamp doctest #2 (result) — timestampType TIMESTAMP_LTZ | TIMESTAMP_LTZ  |
        | try_to_timestamp doctest #4 (result) — timestampType TIMESTAMP_NTZ | TIMESTAMP_NTZ  |

  Rule: Output schema (migrated from test_try_to_timestamp.txt printSchema doctests)

    # A TIMESTAMP input cannot fail to convert, so Spark keeps the result non-nullable even for
    # the `try_` variant. Sail widens it.
    @sail-bug
    @function(nullability)
    Scenario: try_to_timestamp doctest #3 (schema) — timestampType TIMESTAMP_LTZ
      Given config spark.sql.session.timeZone = Europe/Amsterdam
      And config spark.sql.timestampType = TIMESTAMP_LTZ
      When query
        """
        SELECT try_to_timestamp(ts) AS r FROM VALUES (TIMESTAMP_LTZ '2023-01-01 10:00:00'), (TIMESTAMP_LTZ '2023-01-01 03:00:00') AS t(ts)
        """
      Then query schema
        """
        root
         |-- r: timestamp (nullable = false)
        """

    @sail-bug
    @function(nullability)
    Scenario: try_to_timestamp doctest #5 (schema) — timestampType TIMESTAMP_NTZ
      Given config spark.sql.session.timeZone = Europe/Amsterdam
      And config spark.sql.timestampType = TIMESTAMP_NTZ
      When query
        """
        SELECT try_to_timestamp(ts) AS r FROM VALUES (TIMESTAMP_LTZ '2023-01-01 10:00:00'), (TIMESTAMP_LTZ '2023-01-01 03:00:00') AS t(ts)
        """
      Then query schema
        """
        root
         |-- r: timestamp_ntz (nullable = false)
        """

  Rule: A numeric value is a count of seconds since the epoch
    # Spark 4.2.0 datetimeExpressions.scala: TryToTimestampExpressionBuilder builds
    # ParseToTimestamp(failOnError = false), i.e. a non-ANSI Cast(left, TimestampType).
    # Spark 4.2.0 Cast.scala doubleToTimestamp: NaN is NULL and the Long conversion saturates.

    @sail-bug
    Scenario Outline: try_to_timestamp reads <case> input as seconds with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_to_timestamp(<value>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case    | value      | result                        |
        | integer | 1700000000 | 2023-11-14 22:13:20           |
        | double  | 1.5D       | 1970-01-01 00:00:01.5         |
        | huge    | 1e20D      | +294247-01-10 04:00:54.775807 |

    Scenario: try_to_timestamp of a NaN double returns NULL with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_to_timestamp(CAST('NaN' AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    @function(nullability)
    Scenario: try_to_timestamp of an integer literal is not nullable
      When query
        """
        SELECT try_to_timestamp(1700000000) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

  Rule: Without a format, try_to_timestamp follows the lenient STRING to TIMESTAMP cast

    @sail-bug
    Scenario: try_to_timestamp trims surrounding whitespace
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_to_timestamp('  2024-01-15 10:30:00  ') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:00 |

    @sail-bug
    Scenario: try_to_timestamp of a bare time takes the current date
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_to_timestamp('10:30:45') = timestamp(concat(CAST(current_date() AS STRING), ' 10:30:45')) AS result
        """
      Then query result
        | result |
        | true   |

  Rule: An invalid pattern is an error even for the try_ variant
    # Spark 4.2.0 datetimeExpressions.scala: ToTimestamp only turns parse errors
    # (DateTimeException / ParseException) into NULL; the formatter is built outside that
    # try, so an illegal pattern still raises INVALID_DATETIME_PATTERN.

    @sail-bug
    Scenario Outline: try_to_timestamp with an illegal pattern fails with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT try_to_timestamp('2024-01-15', 'yyyy-MM-dd-qqq') AS result
        """
      Then query error INVALID_DATETIME_PATTERN

      Examples:
        | ansi  |
        | true  |
        | false |

  Rule: The session time zone governs strings without an offset and numbers

    Scenario Outline: try_to_timestamp reads a string without an offset as local time in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT unix_seconds(try_to_timestamp(<value>)) AS s
        """
      Then query result
        | s   |
        | <s> |

      Examples:
        | zone                | value                 | s          |
        | America/Los_Angeles | '2024-03-10 02:30:00' | 1710066600 |
        | Asia/Kolkata        | '2024-06-15 12:00:00' | 1718433000 |
        | Pacific/Chatham     | '2024-06-15 12:00:00' | 1718406900 |
        | Pacific/Pago_Pago   | '2024-06-15 12:00:00' | 1718492400 |

    @sail-bug
    Scenario: try_to_timestamp of a double number of seconds is shown in a half-hour offset zone
      Given config spark.sql.session.timeZone = Asia/Kolkata
      When query
        """
        SELECT try_to_timestamp(1.5D) AS result
        """
      Then query result
        | result                |
        | 1970-01-01 05:30:01.5 |
