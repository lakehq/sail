Feature: scaling a legacy calendar interval by a number, vs Spark 4.2.0

  # `BinaryArithmeticWithDatetimeResolver.scala:150-151,166` sends a CalendarIntervalType operand
  # to `MultiplyInterval`/`DivideInterval`, which Sail did not have: DataFusion cannot coerce
  # `Interval(MonthDayNano)` against a number, so `*` was REFUSED outright and `/` was worse -- it
  # fell through to the generic branch and answered a DOUBLE, a wrong type nobody could see
  # because both engines returned a value.
  #
  # This interval is NOT an ANSI interval and it does not round like one. Spark truncates the
  # months and the days toward zero, folds the fraction of a day it threw away into the time part,
  # and rounds only that -- with Scala's `Double.round`, which is `floor(x + 0.5)` and therefore
  # sends a tie toward POSITIVE INFINITY (`IntervalUtils.scala:634-658`). Measured, not assumed:
  # `1 microsecond * -0.5` is `0` here, where the ANSI rule of
  # `arithmetic_ym_interval_scaling.feature` gives `-1`. Getting those two mixed up is the bug this
  # file exists to catch.

  Rule: every numeric type scales a calendar interval

    Scenario Outline: scaling a calendar interval: <case>
      When query
        """
        SELECT CAST(<expression> AS STRING) AS v
        """
      Then query result
        | v       |
        | <value> |

      Examples:
        | case           | expression                                                      | value                                                                       |
        | cal * tinyint  | make_interval(1, 2, 0, 4, 5, 6, 7) * CAST(3 AS TINYINT)         | 3 years 6 months 12 days 15 hours 18 minutes 21 seconds                     |
        | tinyint * cal  | CAST(3 AS TINYINT) * make_interval(1, 2, 0, 4, 5, 6, 7)         | 3 years 6 months 12 days 15 hours 18 minutes 21 seconds                     |
        | cal / tinyint  | make_interval(1, 2, 0, 4, 5, 6, 7) / CAST(3 AS TINYINT)         | 4 months 1 days 9 hours 42 minutes 2.333333 seconds                         |
        | cal * smallint | make_interval(1, 2, 0, 4, 5, 6, 7) * CAST(3 AS SMALLINT)        | 3 years 6 months 12 days 15 hours 18 minutes 21 seconds                     |
        | smallint * cal | CAST(3 AS SMALLINT) * make_interval(1, 2, 0, 4, 5, 6, 7)        | 3 years 6 months 12 days 15 hours 18 minutes 21 seconds                     |
        | cal / smallint | make_interval(1, 2, 0, 4, 5, 6, 7) / CAST(3 AS SMALLINT)        | 4 months 1 days 9 hours 42 minutes 2.333333 seconds                         |
        | cal * int      | make_interval(1, 2, 0, 4, 5, 6, 7) * CAST(3 AS INT)             | 3 years 6 months 12 days 15 hours 18 minutes 21 seconds                     |
        | int * cal      | CAST(3 AS INT) * make_interval(1, 2, 0, 4, 5, 6, 7)             | 3 years 6 months 12 days 15 hours 18 minutes 21 seconds                     |
        | cal / int      | make_interval(1, 2, 0, 4, 5, 6, 7) / CAST(3 AS INT)             | 4 months 1 days 9 hours 42 minutes 2.333333 seconds                         |
        | cal * bigint   | make_interval(1, 2, 0, 4, 5, 6, 7) * CAST(3 AS BIGINT)          | 3 years 6 months 12 days 15 hours 18 minutes 21 seconds                     |
        | bigint * cal   | CAST(3 AS BIGINT) * make_interval(1, 2, 0, 4, 5, 6, 7)          | 3 years 6 months 12 days 15 hours 18 minutes 21 seconds                     |
        | cal / bigint   | make_interval(1, 2, 0, 4, 5, 6, 7) / CAST(3 AS BIGINT)          | 4 months 1 days 9 hours 42 minutes 2.333333 seconds                         |
        | cal * float    | make_interval(1, 2, 0, 4, 5, 6, 7) * CAST(1.5 AS FLOAT)         | 1 years 9 months 6 days 7 hours 39 minutes 10.5 seconds                     |
        | float * cal    | CAST(1.5 AS FLOAT) * make_interval(1, 2, 0, 4, 5, 6, 7)         | 1 years 9 months 6 days 7 hours 39 minutes 10.5 seconds                     |
        | cal / float    | make_interval(1, 2, 0, 4, 5, 6, 7) / CAST(1.5 AS FLOAT)         | 9 months 2 days 19 hours 24 minutes 4.666667 seconds                        |
        | cal * double   | make_interval(1, 2, 0, 4, 5, 6, 7) * CAST(2.5 AS DOUBLE)        | 2 years 11 months 10 days 12 hours 45 minutes 17.5 seconds                  |
        | double * cal   | CAST(2.5 AS DOUBLE) * make_interval(1, 2, 0, 4, 5, 6, 7)        | 2 years 11 months 10 days 12 hours 45 minutes 17.5 seconds                  |
        | cal / double   | make_interval(1, 2, 0, 4, 5, 6, 7) / CAST(2.5 AS DOUBLE)        | 5 months 1 days 16 hours 26 minutes 26.8 seconds                            |
        | cal * decimal  | make_interval(1, 2, 0, 4, 5, 6, 7) * CAST(1.5 AS DECIMAL(10,2)) | 1 years 9 months 6 days 7 hours 39 minutes 10.5 seconds                     |
        | decimal * cal  | CAST(1.5 AS DECIMAL(10,2)) * make_interval(1, 2, 0, 4, 5, 6, 7) | 1 years 9 months 6 days 7 hours 39 minutes 10.5 seconds                     |
        | cal / decimal  | make_interval(1, 2, 0, 4, 5, 6, 7) / CAST(1.5 AS DECIMAL(10,2)) | 9 months 2 days 19 hours 24 minutes 4.666667 seconds                        |
        | cal * 0.5      | make_interval(0, 1, 0, 1, 0, 0, 0) * CAST(0.5 AS DOUBLE)        | 12 hours                                                                    |

  Rule: months and days truncate, the time part rounds toward positive infinity

    # The asymmetry, one row per direction. A tie on the time part goes UP even when the value is
    # negative, which is the opposite of what the ANSI intervals do.
    Scenario Outline: rounding a scaled calendar interval: <case>
      When query
        """
        SELECT CAST(<expression> AS STRING) AS v
        """
      Then query result
        | v       |
        | <value> |

      Examples:
        | case                  | expression                                                | value           |
        | half a micro rounds up | make_interval(0,0,0,0,0,0,0.000001) * CAST(0.5 AS DOUBLE) | 0.000001 seconds |
        | a negative tie goes up | make_interval(0,0,0,0,0,0,0.000001) * CAST(-0.5 AS DOUBLE) | 0 seconds      |
        | and so does this one   | make_interval(0,0,0,0,0,0,-0.000001) * CAST(0.5 AS DOUBLE) | 0 seconds      |
        | half a day is 12 hours | make_interval(0,0,0,1,0,0,0) * CAST(0.5 AS DOUBLE)        | 12 hours        |
        | a day and a half       | make_interval(0,0,0,1,0,0,0) * CAST(1.5 AS DOUBLE)        | 1 days 12 hours |
        | months truncate        | make_interval(0,1,0,0,0,0,0) * CAST(1.9 AS DOUBLE)        | 1 months        |
        | and truncate downward  | make_interval(0,1,0,0,0,0,0) * CAST(-1.9 AS DOUBLE)       | -1 months       |

  Rule: a calendar interval scaled by a number is still a calendar interval

    Scenario Outline: the type of a scaled calendar interval: <case>
      When query
        """
        SELECT typeof(<expression>) AS t
        """
      Then query result
        | t        |
        | interval |

      Examples:
        | case         | expression                                        |
        | multiplied   | make_interval(1,2,0,4,5,6,7) * CAST(2 AS INT)     |
        | divided      | make_interval(1,2,0,4,5,6,7) / CAST(2 AS INT)     |
        | by a decimal | make_interval(1,2,0,4,5,6,7) * CAST(1.5 AS DECIMAL(10,2)) |
        | a NULL scale | make_interval(1,2,0,4,5,6,7) * CAST(NULL AS INT)  |

  Rule: this one DOES read the ANSI flag

    # Unlike the ANSI intervals, `MultiplyInterval` and `DivideInterval` carry `failOnError =
    # SQLConf.get.ansiEnabled` (`intervalExpressions.scala:597-601`), so the flag decides between
    # raising and giving back a NULL.
    Scenario: a calendar interval divided by zero is NULL with ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_interval(0,0,0,1,0,0,0) / CAST(0 AS INT) IS NULL AS v
        """
      Then query result
        | v    |
        | true |

    # A PySpark 3.5 client cannot receive a `calendar_interval` column at all (`Unsupported data type
    # calendar_interval`), so the bare interval is only asserted from PySpark 4; the cast to STRING
    # keeps the same division reachable from every client.
    @spark-4.0
    Scenario: a calendar interval divided by zero raises with ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_interval(0,0,0,1,0,0,0) / CAST(0 AS INT) AS v
        """
      Then query error (?i)division by zero

    Scenario: a calendar interval divided by zero raises with ANSI on, read as a string
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(make_interval(0,0,0,1,0,0,0) / CAST(0 AS INT) AS STRING) AS v
        """
      Then query error (?i)division by zero

  Rule: the time part rounds with Java's Math.round, and a NaN field is zero even with ANSI on

    # `fromDoubles` (`IntervalUtils.scala:634-642`) is `toIntExact(x.toLong)` per field, and
    # `NaN.toLong` is 0, so NaN does not raise; the time part is `Math.round`, the exact floor(x+1/2),
    # rounded to a whole microsecond BEFORE Sail's step to nanoseconds.
    Scenario Outline: scaling a calendar interval exactly: <case> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(<expression> AS STRING) AS v
        """
      Then query result
        | v       |
        | <value> |

      Examples:
        | case                   | ansi  | expression                                                                     | value                                    |
        | a wide time part       | false | make_interval(0, 0, 0, 0, 27777, 46, 40.000001) * CAST(1 AS INT)               | 27777 hours 46 minutes 40.000001 seconds |
        | a wide time part       | true  | make_interval(0, 0, 0, 0, 27777, 46, 40.000001) * CAST(1 AS INT)               | 27777 hours 46 minutes 40.000001 seconds |
        | an odd count past 2^52 | false | make_interval(0, 0, 0, 0, 1251000, 0, 0.000001) * CAST(1 AS INT)               | 1251000 hours 0.000001 seconds           |
        | just under a half      | false | make_interval(0, 0, 0, 0, 0, 0, 0.000001) * CAST(0.49999999999999994 AS DOUBLE) | 0 seconds                                |
        | a NaN factor           | true  | make_interval(0, 1, 0, 1, 0, 0, 0) * CAST('NaN' AS DOUBLE)                     | 0 seconds                                |
        | a NaN divisor          | true  | make_interval(0, 1, 0, 1, 0, 0, 0) / CAST('NaN' AS DOUBLE)                     | 0 seconds                                |

  Rule: the time part of a calendar interval is narrower in Sail than in Spark

    # Spark stores the time part of a calendar interval as MICROSECONDS and Sail as NANOSECONDS,
    # both in an `i64`, so Sail runs out a thousand times sooner -- at about 292 years instead of
    # 292 thousand. This is not a rounding bug and no arithmetic fixes it: the value simply has no
    # representation in `Interval(MonthDayNano)`. Pinned so the limit is a decision on record.
    @sail-bug
    Scenario Outline: a calendar interval past the nanosecond range keeps Spark's value: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(<expression> AS STRING) AS v
        """
      Then query result
        | v       |
        | <value> |

      Examples:
        | case            | expression                                                | value                                                                       |
        | past the range  | make_interval(0,0,0,0,0,0,9223372036) * CAST(2 AS INT)    | 5124095 hours 34 minutes 32 seconds                                         |
        | far past it     | make_interval(1, 2, 0, 4, 5, 6, 7) * CAST(1e18 AS DOUBLE) | 178956970 years 7 months 2147483647 days 2562047788 hours 54.775807 seconds |
