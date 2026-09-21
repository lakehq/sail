Feature: to_timestamp_ntz

  Rule: Result values (migrated from test_to_timestamp_ntz.txt doctests)

    Scenario: to_timestamp_ntz doctest #1 (result) — input LTZ timestamps under Amsterdam
      Given config spark.sql.session.timeZone = Europe/Amsterdam
      When query
        """
        SELECT ts FROM VALUES (TIMESTAMP_LTZ '2023-01-01 10:00:00'), (TIMESTAMP_LTZ '2023-01-01 03:00:00') AS t(ts)
        """
      Then query result
        | ts                  |
        | 2023-01-01 10:00:00 |
        | 2023-01-01 03:00:00 |

    Scenario: to_timestamp_ntz doctest #2 (result)
      Given config spark.sql.session.timeZone = Europe/Amsterdam
      When query
        """
        SELECT to_timestamp_ntz(ts) AS r FROM VALUES (TIMESTAMP_LTZ '2023-01-01 10:00:00'), (TIMESTAMP_LTZ '2023-01-01 03:00:00') AS t(ts)
        """
      Then query result
        | r                   |
        | 2023-01-01 10:00:00 |
        | 2023-01-01 03:00:00 |

  Rule: Output schema (migrated from test_to_timestamp_ntz.txt printSchema doctests)

    # The inline-table TIMESTAMP column is non-nullable and the conversion cannot fail, so
    # Spark keeps the result non-nullable. Sail widens it.
    @sail-bug
    @function(nullability)
    Scenario: to_timestamp_ntz doctest #3 (schema)
      Given config spark.sql.session.timeZone = Europe/Amsterdam
      When query
        """
        SELECT to_timestamp_ntz(ts) AS r FROM VALUES (TIMESTAMP_LTZ '2023-01-01 10:00:00'), (TIMESTAMP_LTZ '2023-01-01 03:00:00') AS t(ts)
        """
      Then query schema
        """
        root
         |-- r: timestamp_ntz (nullable = false)
        """

  Rule: Without a format, to_timestamp_ntz follows the STRING to TIMESTAMP_NTZ cast
    # Spark 4.2.0 datetimeExpressions.scala: ParseToTimestamp only adds NumericType to its input
    # types for a TIMESTAMP result, so for TIMESTAMP_NTZ a number is implicitly cast to STRING
    # first. Spark 4.2.0 SparkDateTimeUtils.stringToTimestampWithoutTimeZone trims the input,
    # rejects a bare time (no date), validates and then drops a zone suffix, and reaches +294247.

    @sail-bug
    Scenario Outline: to_timestamp_ntz of <case> input returns NULL with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp_ntz(<value>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case    | value      |
        | integer | 1700000000 |
        | double  | 1.5D       |

    @sail-bug
    Scenario: to_timestamp_ntz of an integer fails the string cast with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp_ntz(1) AS result
        """
      Then query error CAST_INVALID_INPUT

    @sail-bug
    Scenario Outline: to_timestamp_ntz accepts the lenient cast form <case> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp_ntz(<value>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case               | ansi  | value            | result                 |
        | surrounding spaces | true  | '  2024-01-15  ' | 2024-01-15 00:00:00    |
        | surrounding spaces | false | '  2024-01-15  ' | 2024-01-15 00:00:00    |
        | six-digit year     | true  | '294247-01-01'   | +294247-01-01 00:00:00 |

    @sail-bug
    Scenario: to_timestamp_ntz returns NULL for an unknown zone suffix per row with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT i, to_timestamp_ntz(v) AS result
        FROM VALUES
          (1, '2024-01-15 10:00:00'),
          (2, '2024-01-16 11:00:00 XYZ'),
          (3, '2024-01-17 12:00:00+02:00')
          AS x(i, v)
        ORDER BY i
        """
      Then query result ordered
        | i | result              |
        | 1 | 2024-01-15 10:00:00 |
        | 2 | NULL                |
        | 3 | 2024-01-17 12:00:00 |

    @sail-bug
    Scenario Outline: to_timestamp_ntz rejects <case> with CAST_INVALID_INPUT under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp_ntz(<value>) AS result
        """
      Then query error CAST_INVALID_INPUT

      Examples:
        | case            | value                     |
        | an unknown zone | '2024-01-15 10:30:45 XYZ' |
        | a bare time     | '10:30:45'                |
        | an hour of 25   | '2024-01-15 25:00:00'     |

    Scenario: to_timestamp_ntz of a bare time returns NULL with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp_ntz('10:30:45') AS result
        """
      Then query result
        | result |
        | NULL   |

  @function(nullability)
  Rule: Formatted input nullability

    # Spark 4.2.0 datetimeExpressions.scala: ToTimestamp.nullable is
    # `if (failOnError) children.exists(_.nullable) else true`.
    @sail-bug
    Scenario: to_timestamp_ntz of a string literal with a format is not nullable with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp_ntz('2024-01-15', 'yyyy-MM-dd') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp_ntz (nullable = false)
        """

  Rule: The session time zone never shifts a TIMESTAMP_NTZ wall clock

    Scenario Outline: to_timestamp_ntz keeps the wall clock of <case> in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT to_timestamp_ntz(<value>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                      | zone                | value                       | result              |
        | a time in the DST gap     | America/Los_Angeles | '2024-03-10 02:30:00'       | 2024-03-10 02:30:00 |
        | a time in the DST overlap | America/Los_Angeles | '2024-11-03 01:30:00'       | 2024-11-03 01:30:00 |
        | a UTC offset string       | Asia/Kolkata        | '2024-06-15 12:00:00+00:00' | 2024-06-15 12:00:00 |
        | a positive offset string  | Pacific/Chatham     | '2024-07-15 20:00:00+02:00' | 2024-07-15 20:00:00 |
        | a plain string            | Pacific/Pago_Pago   | '2024-06-15 12:00:00'       | 2024-06-15 12:00:00 |

    Scenario: to_timestamp_ntz with a format keeps the DST-gap wall clock
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT to_timestamp_ntz('2024-03-10 02:30', 'yyyy-MM-dd HH:mm') AS result
        """
      Then query result
        | result              |
        | 2024-03-10 02:30:00 |
