Feature: make_dt_interval output schema

  @function(nullability) @spark-4
  Rule: Output schema

    Scenario: a non-null literal input to make_dt_interval yields the schema Spark declares
      When query
        """
        SELECT make_dt_interval(1, 12, 30, 01.001001) AS result
        """
      Then query schema
        """
        root
         |-- result: interval day to second (nullable = false)
        """

    Scenario: a non-null column input to make_dt_interval yields the schema Spark declares
      When query
        """
        SELECT make_dt_interval(CAST(id AS INT), 12, 30, 01.001001) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: interval day to second (nullable = false)
        """

    Scenario: a nullable column input to make_dt_interval stays nullable
      When query
        """
        SELECT make_dt_interval(c, 12, 30, 01.001001) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: interval day to second (nullable = true)
        """

  Rule: Result values (migrated from test_make_dt_interval.txt doctests)

    Scenario Outline: Doctest (derived column name): <case>
      When query
        """
        SELECT (make_dt_interval(<args>))
        """
      Then query result
        | <name>   |
        | <result> |

      Examples:
        | case                                  | args          | name                            | result                                |
        | make_dt_interval doctest #1 (result)  | null, 0, 0, 0 | make_dt_interval(NULL, 0, 0, 0) | NULL                                  |
        | make_dt_interval doctest #2 (result)  | 0, null, 0, 0 | make_dt_interval(0, NULL, 0, 0) | NULL                                  |
        | make_dt_interval doctest #3 (result)  | 0, 0, null, 0 | make_dt_interval(0, 0, NULL, 0) | NULL                                  |
        | make_dt_interval doctest #4 (result)  | 0, 0, 0, null | make_dt_interval(0, 0, 0, NULL) | NULL                                  |
        | make_dt_interval doctest #10 (result) | 0, 0, 0, 0    | make_dt_interval(0, 0, 0, 0)    | INTERVAL '0 00:00:00' DAY TO SECOND   |
        | make_dt_interval doctest #13 (result) | 0, 0, 0, 0.1  | make_dt_interval(0, 0, 0, 0.1)  | INTERVAL '0 00:00:00.1' DAY TO SECOND |

    Scenario Outline: Doctest (aliased): <case>
      When query
        """
        SELECT (make_dt_interval(<args>)) AS make_dt_interval
        """
      Then query result
        | make_dt_interval |
        | <result>         |

      Examples:
        | case                                 | args       | result                              |
        | make_dt_interval doctest #5 (result) |            | INTERVAL '0 00:00:00' DAY TO SECOND |
        | make_dt_interval doctest #6 (result) | 1          | INTERVAL '1 00:00:00' DAY TO SECOND |
        | make_dt_interval doctest #7 (result) | 1, 1       | INTERVAL '1 01:00:00' DAY TO SECOND |
        | make_dt_interval doctest #8 (result) | 1, 1, 1    | INTERVAL '1 01:01:00' DAY TO SECOND |
        | make_dt_interval doctest #9 (result) | 1, 1, 1, 1 | INTERVAL '1 01:01:01' DAY TO SECOND |

    Scenario Outline: Doctest (bare alias): <case>
      When query
        """
        SELECT (make_dt_interval(<args>)) <alias>
        """
      Then query result
        | <alias>  |
        | <result> |

      Examples:
        | case                                  | args         | alias | result                              |
        | make_dt_interval doctest #11 (result) | -1, 24, 0, 0 | df    | INTERVAL '0 00:00:00' DAY TO SECOND |
        | make_dt_interval doctest #12 (result) | 1, -24, 0, 0 | dt    | INTERVAL '0 00:00:00' DAY TO SECOND |

    Scenario: make_dt_interval doctest #14 (result)
      When query
        """
        SELECT day, hour, `min`, sec, make_dt_interval(day) AS r FROM VALUES (CAST(1 AS BIGINT), CAST(12 AS BIGINT), CAST(30 AS BIGINT), CAST(1.001001 AS DOUBLE)) AS t(day, hour, `min`, sec)
        """
      Then query result
        | day | hour | min | sec      | r                                   |
        | 1   | 12   | 30  | 1.001001 | INTERVAL '1 00:00:00' DAY TO SECOND |

    Scenario: make_dt_interval doctest #15 (result)
      When query
        """
        SELECT day, hour, `min`, sec, make_dt_interval(day, hour) AS r FROM VALUES (CAST(1 AS BIGINT), CAST(12 AS BIGINT), CAST(30 AS BIGINT), CAST(1.001001 AS DOUBLE)) AS t(day, hour, `min`, sec)
        """
      Then query result
        | day | hour | min | sec      | r                                   |
        | 1   | 12   | 30  | 1.001001 | INTERVAL '1 12:00:00' DAY TO SECOND |

    Scenario: make_dt_interval doctest #16 (result)
      When query
        """
        SELECT day, hour, `min`, sec, make_dt_interval(day, hour, `min`) AS r FROM VALUES (CAST(1 AS BIGINT), CAST(12 AS BIGINT), CAST(30 AS BIGINT), CAST(1.001001 AS DOUBLE)) AS t(day, hour, `min`, sec)
        """
      Then query result
        | day | hour | min | sec      | r                                   |
        | 1   | 12   | 30  | 1.001001 | INTERVAL '1 12:30:00' DAY TO SECOND |

    Scenario: make_dt_interval doctest #17 (result)
      When query
        """
        SELECT day, hour, `min`, sec, make_dt_interval(day, hour, `min`, sec) AS r FROM VALUES (CAST(1 AS BIGINT), CAST(12 AS BIGINT), CAST(30 AS BIGINT), CAST(1.001001 AS DOUBLE)) AS t(day, hour, `min`, sec)
        """
      Then query result
        | day | hour | min | sec      | r                                          |
        | 1   | 12   | 30  | 1.001001 | INTERVAL '1 12:30:01.001001' DAY TO SECOND |

  @function(nullability) @spark-4
  Rule: Seconds coercion determines nullability

    Scenario Outline: Numeric seconds declare potential conversion nulls: <seconds>
      When query
        """
        SELECT make_dt_interval(0, 0, 0, <seconds>) AS result
        """
      Then query schema
        """
        root
         |-- result: interval day to second (nullable = true)
        """

      Examples:
        | seconds                   |
        | 1D                        |
        | CAST(1 AS FLOAT)          |
        | CAST(1 AS BIGINT)         |
        | CAST(1 AS DECIMAL(20,6))  |

    Scenario: Non-null floating column seconds remain nullable
      When query
        """
        SELECT make_dt_interval(0, 0, 0, CAST(id AS DOUBLE)) AS result FROM range(2)
        """
      Then query schema
        """
        root
         |-- result: interval day to second (nullable = true)
        """

  Rule: Non-finite seconds produce null intervals

    Scenario: Non-finite column seconds preserve nulls in the result
      When query
        """
        SELECT i, CAST(make_dt_interval(0, 0, 0, s) AS STRING) AS result
        FROM VALUES
          (0, CAST('NaN' AS DOUBLE)),
          (1, CAST('Infinity' AS DOUBLE)),
          (2, CAST('-Infinity' AS DOUBLE)),
          (3, 1D) AS t(i, s)
        ORDER BY i
        """
      Then query result ordered
        | i | result                            |
        | 0 | NULL                              |
        | 1 | NULL                              |
        | 2 | NULL                              |
        | 3 | INTERVAL '0 00:00:01' DAY TO SECOND |

    Scenario: Count excludes intervals from non-finite seconds
      When query
        """
        SELECT count(make_dt_interval(0, 0, 0, s)) AS result
        FROM VALUES (CAST('NaN' AS DOUBLE)), (1D) AS t(s)
        """
      Then query result
        | result |
        | 1      |

  Rule: Seconds round at microsecond boundaries

    Scenario: Floating seconds round half away from zero without binary rounding loss
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT i, unix_micros(TIMESTAMP '1970-01-01 00:00:00' + make_dt_interval(0, 0, 0, s)) AS result
        FROM VALUES
          (1, 0.5000005D),
          (2, -0.5000005D),
          (3, 0.9999995D),
          (4, 1.0000005D),
          (5, 0.0000005D),
          (6, 0.0000004999999999999999D),
          (7, 0.5000004999999998D),
          (8, 0.5000005000000001D),
          (9, 99999999.9999995D),
          (10, -99999999.9999995D),
          (11, 100000000.0000005D),
          (12, -100000000.0000005D) AS t(i, s)
        ORDER BY i
        """
      Then query result ordered
        | i | result  |
        | 1 | 500001  |
        | 2 | -500001 |
        | 3 | 1000000 |
        | 4 | 1000001 |
        | 5 | 1       |
        | 6 | 0       |
        | 7 | 500000  |
        | 8 | 500001  |
        | 9 | 100000000000000  |
        | 10 | -100000000000000 |
        | 11 | 100000000000001  |
        | 12 | -100000000000001 |

    Scenario: Decimal seconds retain exact precision before rounding
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT i, unix_micros(TIMESTAMP '1970-01-01 00:00:00' + make_dt_interval(0, 0, 0, s)) AS result
        FROM VALUES
          (1, CAST(0.5000005 AS DECIMAL(38,18))),
          (2, CAST(-0.5000005 AS DECIMAL(38,18))),
          (3, CAST(0.999999999999999999 AS DECIMAL(38,18))),
          (4, CAST(1000000.0000005 AS DECIMAL(38,18))) AS t(i, s)
        ORDER BY i
        """
      Then query result ordered
        | i | result        |
        | 1 | 500001        |
        | 2 | -500001       |
        | 3 | 1000000       |
        | 4 | 1000000000001 |

    @sail-only
    Scenario: Wide decimal seconds retain Sail support and exact microseconds
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT unix_micros(TIMESTAMP '1970-01-01 00:00:00' +
          make_dt_interval(0, 0, 0, CAST(0.5000005 AS DECIMAL(50,15)))) AS result
        """
      Then query result
        | result |
        | 500001 |

  Rule: Checked duration arithmetic uses the microsecond range

    Scenario: Wide day counts remain valid through column evaluation
      When query
        """
        SELECT CAST(make_dt_interval(CAST(id + 1500000 AS INT)) AS STRING) AS result
        FROM range(1)
        """
      Then query result
        | result                                  |
        | INTERVAL '1500000 00:00:00' DAY TO SECOND |

    Scenario Outline: A duration outside the microsecond range raises in ANSI mode <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT make_dt_interval(2147483647) AS result
        """
      Then query error INTERVAL_ARITHMETIC_OVERFLOW

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario: A null component suppresses interval arithmetic overflow
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_dt_interval(2147483647, CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario Outline: Unrepresentable finite seconds raise under ANSI: <seconds>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_dt_interval(0, 0, 0, <seconds>) AS result
        """
      Then query error NUMERIC_VALUE_OUT_OF_RANGE

      Examples:
        | seconds |
        | 1e100   |
        | -1e100  |
        | 9223372036855D |
        | -9223372036855D |

    Scenario Outline: Unrepresentable finite seconds are null without ANSI: <seconds>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_dt_interval(0, 0, 0, <seconds>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | seconds |
        | 1e100   |
        | -1e100  |
        | 9223372036855D |
        | -9223372036855D |

    @sail-only
    Scenario: Sail accepts representable seconds beyond Spark's decimal coercion range
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(make_dt_interval(0, 0, 0, 9000000000000L) AS STRING) AS result
        """
      Then query result
        | result                                    |
        | INTERVAL '104166666 16:00:00' DAY TO SECOND |

    @sail-only
    Scenario Outline: Sail preserves wide seconds with cancelling components in ANSI mode <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT i, CAST(make_dt_interval(d, h, m, s) AS STRING) AS result
        FROM VALUES
          (1, 1500000, -36000000, 0, 9220000000000D),
          (2, -1500000, 36000000, 0, -9220000000000D),
          (3, 1000000, 0, -1440000000, 9220000000000D),
          (4, 0, 0, -1, 9223372036855D),
          (5, 0, 0, 1, -9223372036855D),
          (6, CAST(NULL AS INT), 0, -1, 9223372036855D)
        AS t(i, d, h, m, s)
        ORDER BY i
        """
      Then query result ordered
        | i | result                                     |
        | 1 | INTERVAL '106712962 23:06:40' DAY TO SECOND  |
        | 2 | INTERVAL '-106712962 23:06:40' DAY TO SECOND |
        | 3 | INTERVAL '106712962 23:06:40' DAY TO SECOND  |
        | 4 | INTERVAL '106751991 03:59:55' DAY TO SECOND  |
        | 5 | INTERVAL '-106751991 03:59:55' DAY TO SECOND |
        | 6 | NULL                                      |

      Examples:
        | ansi  |
        | true  |
        | false |
