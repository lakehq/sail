Feature: from_unixtime with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: from_unixtime — the argument is resolved per row, not taken from the first row

    @function(columnargs)
    Scenario: from_unixtime with the argument as a literal
      When query
        """
        SELECT from_unixtime(0, 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result ordered
        | result              |
        | 1970-01-01 00:00:00 |

    @function(columnargs)
    Scenario: from_unixtime takes argument 2 from a column containing NULL
      When query
        """
        SELECT from_unixtime(0, c) AS result FROM VALUES (1, 'yyyy-MM-dd HH:mm:ss'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result              |
        | 1970-01-01 00:00:00 |
        | NULL                |

    @function(columnargs)
    Scenario: from_unixtime takes argument 2 from a column holding two different values
      When query
        """
        SELECT from_unixtime(0, c) AS result FROM VALUES (1, 'yyyy'), (2, 'MM') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 1970   |
        | 01     |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null bigint literal yields a string
      When query
        """
        SELECT from_unixtime(0) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null bigint column yields a string
      When query
        """
        SELECT from_unixtime(id) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable bigint column stays nullable
      When query
        """
        SELECT from_unixtime(c) AS result FROM VALUES (CAST(0 AS BIGINT)), (CAST(NULL AS BIGINT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  # Spark 4.2.0 datetimeExpressions.scala, FromUnixTime: ImplicitCastInputTypes over
  # (BIGINT, STRING), then `formatter.format(seconds * MICROS_PER_SECOND)`.
  Rule: from_unixtime implicitly casts the seconds to BIGINT

    # Cast to BIGINT truncates toward zero: -1.9 is -1 second, not floor(-1.9) = -2.
    Scenario Outline: from_unixtime truncates fractional seconds toward zero: <case>
      When query
        """
        SELECT from_unixtime(<sec>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case     | sec  | result              |
        | positive | 1.9  | 1970-01-01 00:00:01 |
        | negative | -1.9 | 1969-12-31 23:59:59 |

    @sail-bug
    Scenario: from_unixtime casts a numeric string to BIGINT
      When query
        """
        SELECT from_unixtime('1230219000') AS result
        """
      Then query result
        | result              |
        | 2008-12-25 15:30:00 |

    @sail-bug
    Scenario: from_unixtime rejects a non-numeric string under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT from_unixtime('x') AS result
        """
      Then query error \[CAST_INVALID_INPUT

    @sail-bug
    Scenario: from_unixtime resolves a non-numeric string row to NULL without ANSI
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT from_unixtime(c) AS result
        FROM VALUES (1, '1230219000'), (2, 'x'), (3, '0') AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result              |
        | 2008-12-25 15:30:00 |
        | NULL                |
        | 1970-01-01 00:00:00 |

    # DATE and TIMESTAMP cannot be implicitly cast to BIGINT.
    @sail-bug
    Scenario Outline: from_unixtime rejects a <case> argument
      When query
        """
        SELECT from_unixtime(<input>) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE

      Examples:
        | case      | input                           |
        | date      | DATE '2024-01-01'               |
        | timestamp | TIMESTAMP '2024-01-01 00:00:00' |

  # The default pattern 'yyyy-MM-dd HH:mm:ss' renders the proleptic year: year 0 is 0000,
  # year -1 is -0001 and years past 9999 carry a '+' sign.
  Rule: from_unixtime outside the 0001..9999 year range

    @sail-bug
    Scenario Outline: from_unixtime renders <case>
      When query
        """
        SELECT from_unixtime(<sec>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                      | sec           | result                 |
        | the second before year 1  | -62135596801  | 0000-12-31 23:59:59    |
        | the start of year -1      | -62198755200  | -0001-01-01 00:00:00   |
        | the start of year 10000   | 253402300800  | +10000-01-01 00:00:00  |
        | the last TIMESTAMP second | 9223372036854 | +294247-01-10 04:00:54 |

    @sail-bug
    Scenario: from_unixtime renders out-of-range years from a column
      When query
        """
        SELECT from_unixtime(c) AS result
        FROM VALUES (1, CAST(-62135596801 AS BIGINT)), (2, CAST(253402300800 AS BIGINT)), (3, CAST(0 AS BIGINT)) AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result                |
        | 0000-12-31 23:59:59   |
        | +10000-01-01 00:00:00 |
        | 1970-01-01 00:00:00   |

    Scenario: from_unixtime resolves negative seconds from a column
      When query
        """
        SELECT from_unixtime(c, 'yyyy-MM-dd') AS result
        FROM VALUES (1, CAST(0 AS BIGINT)), (2, CAST(-86400 AS BIGINT)), (3, CAST(1230219000 AS BIGINT)), (4, NULL) AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result     |
        | 1970-01-01 |
        | 1969-12-31 |
        | 2008-12-25 |
        | NULL       |

  # FromUnixTime formats the instant with a formatter bound to the session zoneId.
  Rule: from_unixtime renders in the session time zone

    Scenario Outline: from_unixtime renders epoch seconds in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT from_unixtime(c) AS result
        FROM VALUES (1, CAST(0 AS BIGINT)), (2, CAST(1230219000 AS BIGINT)), (3, CAST(1720000000 AS BIGINT)) AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result |
        | <r1>   |
        | <r2>   |
        | <r3>   |

      Examples:
        | zone                | r1                  | r2                  | r3                  |
        | America/Los_Angeles | 1969-12-31 16:00:00 | 2008-12-25 07:30:00 | 2024-07-03 02:46:40 |
        | Asia/Kolkata        | 1970-01-01 05:30:00 | 2008-12-25 21:00:00 | 2024-07-03 15:16:40 |
        | Pacific/Chatham     | 1970-01-01 12:45:00 | 2008-12-26 05:15:00 | 2024-07-03 22:31:40 |
        | Pacific/Pago_Pago   | 1969-12-31 13:00:00 | 2008-12-25 04:30:00 | 2024-07-02 22:46:40 |

    Scenario Outline: from_unixtime renders the epoch literal in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT from_unixtime(0) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | zone                | result              |
        | America/Los_Angeles | 1969-12-31 16:00:00 |
        | Pacific/Pago_Pago   | 1969-12-31 13:00:00 |

    # Two instants an hour apart both render as the repeated local hour of the overlap.
    Scenario Outline: from_unixtime renders both instants of a DST overlap in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT from_unixtime(c) AS result
        FROM VALUES (1, CAST(<first> AS BIGINT)), (2, CAST(<second> AS BIGINT)) AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result   |
        | <local1> |
        | <local2> |

      Examples:
        | zone                | first      | second     | local1              | local2              |
        | America/Los_Angeles | 1730622600 | 1730626200 | 2024-11-03 01:30:00 | 2024-11-03 01:30:00 |
        | Pacific/Chatham     | 1712412000 | 1712415600 | 2024-04-07 02:45:00 | 2024-04-07 03:45:00 |
