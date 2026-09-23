Feature: date_format with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: date_format — the argument is resolved per row, not taken from the first row

    @function(columnargs)
    Scenario: date_format with the argument as a literal
      When query
        """
        SELECT date_format('2016-04-08', 'y') AS result
        """
      Then query result ordered
        | result |
        | 2016   |

    @function(columnargs)
    Scenario: date_format takes argument 2 from a column containing NULL
      When query
        """
        SELECT date_format('2016-04-08', c) AS result FROM VALUES (1, 'y'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 2016   |
        | NULL   |

    @function(columnargs)
    Scenario: date_format takes argument 2 from a column holding two different values
      When query
        """
        SELECT date_format(TIMESTAMP '2026-02-02 10:20:30', c) AS result FROM VALUES (1, 'y'), (2, 'MM') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 2026   |
        | 02     |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null timestamp literal yields a string
      When query
        """
        SELECT date_format(TIMESTAMP '2024-01-15 10:00:00', 'yyyy-MM') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null timestamp column yields a string
      When query
        """
        SELECT date_format(CAST(id AS TIMESTAMP), 'yyyy') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable timestamp column stays nullable
      When query
        """
        SELECT date_format(c, 'yyyy') AS result FROM VALUES (TIMESTAMP '2024-01-15 10:00:00'), (CAST(NULL AS TIMESTAMP)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  @function(nullability)
  Rule: Nullability through Spark's implicit casts
  # String -> * is force-nullable (Cast.scala:458)

    @sail-bug
    Scenario Outline: date_format without an implicit cast keeps its non-nullable schema
      When query
        """
        SELECT date_format(<input>'2024-01-15 10:00:00', 'yyyy-MM') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

      Examples:
        | case    | input      |
        | no cast | TIMESTAMP  |

    Scenario Outline: date_format through a force-nullable implicit cast: <case>
      When query
        """
        SELECT date_format(<input>'2024-01-15 10:00:00', 'yyyy-MM') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

      Examples:
        | case                | input |
        | STRING -> TIMESTAMP |       |

  Rule: A STRING argument goes through the STRING to TIMESTAMP cast
    # Spark 4.2.0 datetimeExpressions.scala: DateFormatClass is ImplicitCastInputTypes with a
    # TIMESTAMP first input, so a string is cast with the session ANSI mode
    # (Spark 4.2.0 Cast.scala castToTimestamp): CAST_INVALID_INPUT with ANSI on, NULL with it off.
    # The cast trims whitespace and accepts years up to +294247.

    @sail-bug
    Scenario: date_format of a malformed string literal fails with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT date_format('not a date', 'yyyy') AS result
        """
      Then query error CAST_INVALID_INPUT

    @sail-bug
    Scenario: date_format of a string column with a malformed row fails with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT i, date_format(v, 'yyyy-MM') AS result
        FROM VALUES (1, '2024-01-15'), (2, 'garbage'), (3, '2025-02-16 10:00:00') AS x(i, v)
        ORDER BY i
        """
      Then query error CAST_INVALID_INPUT

    Scenario: date_format of a string column returns NULL for a malformed row with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT i, date_format(v, 'yyyy-MM') AS result
        FROM VALUES (1, '2024-01-15'), (2, 'garbage'), (3, '2025-02-16 10:00:00') AS x(i, v)
        ORDER BY i
        """
      Then query result ordered
        | i | result  |
        | 1 | 2024-01 |
        | 2 | NULL    |
        | 3 | 2025-02 |

    @sail-bug
    Scenario Outline: date_format of the string <case> casts it leniently with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT date_format(<value>, 'yyyy') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case               | ansi  | value            | result  |
        | surrounding spaces | true  | '  2024-01-15 '  | 2024    |
        | surrounding spaces | false | '  2024-01-15 '  | 2024    |
        | six-digit year     | true  | '294247-01-01'   | +294247 |

    @sail-bug
    Scenario Outline: date_format rejects a numeric <case> at analysis
      When query
        """
        SELECT date_format(<value>, 'yyyy') AS result
        """
      Then query error DATATYPE_MISMATCH

      Examples:
        | case    | value      |
        | integer | 1700000000 |
        | double  | 1.5D       |

  Rule: A NULL timestamp is NULL without validating the pattern
    # Spark 4.2.0 datetimeExpressions.scala: DateFormatClass is NullIntolerant and builds its
    # formatter lazily, so an all-NULL input never compiles the pattern.

    @sail-bug
    Scenario: date_format of a NULL timestamp literal ignores an invalid pattern
      When query
        """
        SELECT date_format(CAST(NULL AS TIMESTAMP), 'qqqqq') AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: date_format of an all-NULL timestamp column ignores an invalid pattern
      When query
        """
        SELECT i, date_format(c, 'qqqqq') AS result
        FROM VALUES (1, CAST(NULL AS TIMESTAMP)), (2, CAST(NULL AS TIMESTAMP)) AS x(i, c)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | NULL   |
        | 2 | NULL   |

  Rule: A TIMESTAMP is formatted in the session time zone

    Scenario Outline: date_format renders the instant <case> in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT date_format(to_timestamp(<value>), 'yyyy-MM-dd HH:mm XXX') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                               | zone                | value                       | result                  |
        | before the DST overlap             | America/Los_Angeles | '2024-11-03 08:30:00+00:00' | 2024-11-03 01:30 -07:00 |
        | inside the DST overlap             | America/Los_Angeles | '2024-11-03 09:30:00+00:00' | 2024-11-03 01:30 -08:00 |
        | across midnight with half an hour  | Asia/Kolkata        | '2024-07-15 20:00:00Z'      | 2024-07-16 01:30 +05:30 |
        | in southern summer                 | Pacific/Chatham     | '2024-01-15 08:00:00Z'      | 2024-01-15 21:45 +13:45 |
        | in southern winter                 | Pacific/Chatham     | '2024-07-15 20:00:00Z'      | 2024-07-16 08:45 +12:45 |
        | back across midnight               | Pacific/Pago_Pago   | '2024-01-15 08:00:00Z'      | 2024-01-14 21:00 -11:00 |

    Scenario: date_format of a TIMESTAMP_NTZ ignores a 45-minute offset session zone
      Given config spark.sql.session.timeZone = Pacific/Chatham
      When query
        """
        SELECT date_format(to_timestamp_ntz('2024-06-15 12:00:00'), 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 2024-06-15 12:00:00 |

    # Spark renders 1700000000 seconds in the session zone. Sail renders it in UTC: the
    # timestamp_seconds / timestamp_millis result seems to carry a fixed UTC zone, whereas
    # date_format of to_timestamp(...) or CAST(1700000000 AS TIMESTAMP) is converted correctly.
    @sail-bug
    Scenario Outline: date_format of a timestamp_seconds value is rendered in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT date_format(timestamp_seconds(1700000000), 'yyyy-MM-dd HH:mm:ss XXX') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | zone                | result                     |
        | America/Los_Angeles | 2023-11-14 14:13:20 -08:00 |
        | Asia/Kolkata        | 2023-11-15 03:43:20 +05:30 |
        | Pacific/Chatham     | 2023-11-15 11:58:20 +13:45 |
        | Pacific/Pago_Pago   | 2023-11-14 11:13:20 -11:00 |

    @sail-bug
    Scenario: date_format of a timestamp_millis value is rendered in a half-hour offset zone
      Given config spark.sql.session.timeZone = Asia/Kolkata
      When query
        """
        SELECT date_format(timestamp_millis(1700000000000), 'HH:mm') AS result
        """
      Then query result
        | result |
        | 03:43  |
