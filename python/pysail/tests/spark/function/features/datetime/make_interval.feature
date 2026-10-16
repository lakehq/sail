Feature: make_interval output schema

  @function(nullability) @spark-4
  Rule: Output schema

    Scenario: a non-null literal input to make_interval yields the schema Spark declares
      When query
        """
        SELECT make_interval(100, 11, 1, 1, 12, 30, 01.001001) AS result
        """
      Then query schema
        """
        root
         |-- result: interval (nullable = false)
        """

    Scenario: a non-null column input to make_interval yields the schema Spark declares
      When query
        """
        SELECT make_interval(CAST(id AS INT), 11, 1, 1, 12, 30, 01.001001) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: interval (nullable = false)
        """

    Scenario: a nullable column input to make_interval stays nullable
      When query
        """
        SELECT make_interval(c, 11, 1, 1, 12, 30, 01.001001) AS result FROM VALUES (100), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: interval (nullable = true)
        """

  Rule: Result values (migrated from test_make_interval.txt doctests)

    Scenario: make_interval doctest #1 (result)
      When query
        """
        SELECT y, m, w, d, h, mi, s, make_interval(y, m, w, d, h, mi, s) AS interval FROM VALUES (1, 2, 3, 4, 5, 6, CAST(7.5 AS DOUBLE)), (CAST(NULL AS INT), 2, 3, 4, 5, 6, CAST(7.5 AS DOUBLE)), (1, CAST(NULL AS INT), 3, 4, 5, 6, CAST(7.5 AS DOUBLE)), (1, 2, CAST(NULL AS INT), 4, 5, 6, CAST(7.5 AS DOUBLE)), (1, 2, 3, CAST(NULL AS INT), 5, 6, CAST(7.5 AS DOUBLE)), (1, 2, 3, 4, CAST(NULL AS INT), 6, CAST(7.5 AS DOUBLE)), (1, 2, 3, 4, 5, CAST(NULL AS INT), CAST(7.5 AS DOUBLE)), (1, 2, 3, 4, 5, 6, CAST(NULL AS DOUBLE)), (1, 1, 1, 1, 1, 1, CAST(1.0 AS DOUBLE)), (0, 0, 0, 0, 0, 0, CAST('NaN' AS DOUBLE)), (0, 0, 0, 0, 0, 0, CAST('Infinity' AS DOUBLE)), (0, 0, 0, 0, 0, 0, CAST('-Infinity' AS DOUBLE)) AS t(y, m, w, d, h, mi, s)
        """
      Then query result
        | y    | m    | w    | d    | h    | mi   | s         | interval                                               |
        | 1    | 2    | 3    | 4    | 5    | 6    | 7.5       | 1 years 2 months 25 days 5 hours 6 minutes 7.5 seconds |
        | NULL | 2    | 3    | 4    | 5    | 6    | 7.5       | NULL                                                   |
        | 1    | NULL | 3    | 4    | 5    | 6    | 7.5       | NULL                                                   |
        | 1    | 2    | NULL | 4    | 5    | 6    | 7.5       | NULL                                                   |
        | 1    | 2    | 3    | NULL | 5    | 6    | 7.5       | NULL                                                   |
        | 1    | 2    | 3    | 4    | NULL | 6    | 7.5       | NULL                                                   |
        | 1    | 2    | 3    | 4    | 5    | NULL | 7.5       | NULL                                                   |
        | 1    | 2    | 3    | 4    | 5    | 6    | NULL      | NULL                                                   |
        | 1    | 1    | 1    | 1    | 1    | 1    | 1.0       | 1 years 1 months 8 days 1 hours 1 minutes 1 seconds    |
        | 0    | 0    | 0    | 0    | 0    | 0    | NaN       | NULL                                                   |
        | 0    | 0    | 0    | 0    | 0    | 0    | Infinity  | NULL                                                   |
        | 0    | 0    | 0    | 0    | 0    | 0    | -Infinity | NULL                                                   |

  @function(nullability) @spark-4
  Rule: Nullability follows overflow mode and seconds coercion

    Scenario: Non-null components remain nullable when ANSI is disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_interval(1, 2, 3, 4, 5, 6, 7.5) AS result
        """
      Then query schema
        """
        root
         |-- result: interval (nullable = true)
        """

    Scenario Outline: Floating and wide seconds remain nullable under ANSI: <seconds>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_interval(0, 0, 0, 0, 0, 0, <seconds>) AS result
        """
      Then query schema
        """
        root
         |-- result: interval (nullable = true)
        """

      Examples:
        | seconds                   |
        | 1D                        |
        | CAST(1 AS FLOAT)          |
        | CAST(1 AS BIGINT)         |
        | CAST(1 AS DECIMAL(20,6))  |

    Scenario: Floating column seconds remain nullable under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_interval(0, 0, 0, 0, 0, 0, CAST(id AS DOUBLE)) AS result FROM range(2)
        """
      Then query schema
        """
        root
         |-- result: interval (nullable = true)
        """

  Rule: Fractional seconds preserve their carry

    Scenario: Floating fractional rounding carries into whole seconds
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT i, unix_micros(TIMESTAMP '1970-01-01 00:00:00' + make_interval(0, 0, 0, 0, 0, 0, s)) AS result
        FROM VALUES
          (1, 0.9999999999D),
          (2, -0.9999999999D),
          (3, 1.9999999999D),
          (4, -1.9999999999D) AS t(i, s)
        ORDER BY i
        """
      Then query result ordered
        | i | result   |
        | 1 | 1000000  |
        | 2 | -1000000 |
        | 3 | 2000000  |
        | 4 | -2000000 |

    Scenario: Decimal fractional rounding carries into whole seconds
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT i, unix_micros(TIMESTAMP '1970-01-01 00:00:00' + make_interval(0, 0, 0, 0, 0, 0, s)) AS result
        FROM VALUES
          (1, CAST(0.9999999999 AS DECIMAL(20,10))),
          (2, CAST(-0.9999999999 AS DECIMAL(20,10))) AS t(i, s)
        ORDER BY i
        """
      Then query result ordered
        | i | result   |
        | 1 | 1000000  |
        | 2 | -1000000 |

    @sail-only
    Scenario: Calendar intervals preserve Sail nanosecond precision
      When query
        """
        SELECT i, CAST(make_interval(0, 0, 0, 0, 0, 0, s) AS STRING) AS result
        FROM VALUES
          (1, 0.123456789D),
          (2, -0.123456789D),
          (3, 0.0000000005D),
          (4, -0.0000000005D),
          (5, 99999.9999999995D),
          (6, -99999.9999999995D),
          (7, 100000.0000000005D),
          (8, -100000.0000000005D) AS t(i, s)
        ORDER BY i
        """
      Then query result ordered
        | i | result               |
        | 1 | 0.123456789 seconds  |
        | 2 | -0.123456789 seconds |
        | 3 | 0.000000001 seconds  |
        | 4 | -0.000000001 seconds |
        | 5 | 27 hours 46 minutes 40 seconds |
        | 6 | -27 hours -46 minutes -40 seconds |
        | 7 | 27 hours 46 minutes 40.000000001 seconds |
        | 8 | -27 hours -46 minutes -40.000000001 seconds |

    @sail-only
    Scenario: Wide decimal seconds retain Sail support and nanosecond precision
      When query
        """
        SELECT CAST(make_interval(0, 0, 0, 0, 0, 0,
          CAST(0.123456789 AS DECIMAL(50,15))) AS STRING) AS result
        """
      Then query result
        | result              |
        | 0.123456789 seconds |

  Rule: Checked component arithmetic

    Scenario Outline: Overflow raises when ANSI is enabled: <args>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_interval(<args>) AS result
        """
      Then query error ARITHMETIC_OVERFLOW

      Examples:
        | args               |
        | 2147483647         |
        | 0, 0, 2147483647   |

    Scenario Outline: Overflow becomes null when ANSI is disabled: <args>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_interval(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | args               |
        | 2147483647         |
        | 0, 0, 2147483647   |

    Scenario: A null component suppresses calendar arithmetic overflow
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_interval(2147483647, CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Large opposite components cancel within the supported interval range
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT i, CAST(make_interval(0, 0, 0, 0, h, m, 0) AS STRING) AS result
        FROM VALUES (1, 3000000, -180000000), (2, -3000000, 180000000) AS t(i, h, m)
        ORDER BY i
        """
      Then query result ordered
        | i | result    |
        | 1 | 0 seconds |
        | 2 | 0 seconds |

    @sail-bug
    Scenario: Calendar elapsed hours can exceed Arrow's nanosecond range
      # FIXME: Spark's microsecond calendar interval holds this duration; Arrow's nanosecond field does not.
      When query
        """
        SELECT CAST(make_interval(0, 0, 0, 0, 3000000, 0, 0) AS STRING) AS result
        """
      Then query result
        | result        |
        | 3000000 hours |
