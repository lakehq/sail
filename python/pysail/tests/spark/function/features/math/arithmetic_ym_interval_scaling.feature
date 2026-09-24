Feature: scaling a year-month interval by a number, vs Spark 4.2.0

  # `BinaryArithmeticWithDatetimeResolver.scala:154-155,167` sends `<year-month> * <number>` to
  # `MultiplyYMInterval` and `<year-month> / <number>` to `DivideYMInterval`, either operand order
  # for `*`. Sail had neither: DataFusion cannot coerce `Interval(YearMonth)` against a number, so
  # all 21 cells of the matrix were REFUSED -- queries Spark answers.
  #
  # A year-month interval is a count of months and the result is a count of months too, so a
  # fractional operand is ROUNDED, HALF_UP (`intervalExpressions.scala:615,621`). That is the part
  # a naive implementation gets wrong, so the rounding is pinned case by case below.
  #
  # ANSI is NOT an axis: every row was measured on the JVM under both modes and neither the value
  # nor the error changes -- an interval divided by zero raises with ANSI off too.

  Rule: every numeric type scales a year-month interval

    # The 21 cells of the matrix, whole: seven numeric types times `iv * n`, `n * iv` and `iv / n`.
    # `INTERVAL '1-2' YEAR TO MONTH` is 14 months, so `/ 3` is 4.67 rounded to 5 and `/ 2.5` is 5.6
    # rounded to 6 -- the integral and the fractional roundings both show up in the table.
    Scenario Outline: scaling a year-month interval: <case>
      When query
        """
        SELECT CAST(<expression> AS STRING) AS v
        """
      Then query result
        | v       |
        | <value> |

      Examples:
        | case          | expression                                                | value                         |
        | ym * tinyint  | INTERVAL '1-2' YEAR TO MONTH * CAST(3 AS TINYINT)         | INTERVAL '3-6' YEAR TO MONTH  |
        | tinyint * ym  | CAST(3 AS TINYINT) * INTERVAL '1-2' YEAR TO MONTH         | INTERVAL '3-6' YEAR TO MONTH  |
        | ym / tinyint  | INTERVAL '1-2' YEAR TO MONTH / CAST(3 AS TINYINT)         | INTERVAL '0-5' YEAR TO MONTH  |
        | ym * smallint | INTERVAL '1-2' YEAR TO MONTH * CAST(3 AS SMALLINT)        | INTERVAL '3-6' YEAR TO MONTH  |
        | smallint * ym | CAST(3 AS SMALLINT) * INTERVAL '1-2' YEAR TO MONTH        | INTERVAL '3-6' YEAR TO MONTH  |
        | ym / smallint | INTERVAL '1-2' YEAR TO MONTH / CAST(3 AS SMALLINT)        | INTERVAL '0-5' YEAR TO MONTH  |
        | ym * int      | INTERVAL '1-2' YEAR TO MONTH * CAST(3 AS INT)             | INTERVAL '3-6' YEAR TO MONTH  |
        | int * ym      | CAST(3 AS INT) * INTERVAL '1-2' YEAR TO MONTH             | INTERVAL '3-6' YEAR TO MONTH  |
        | ym / int      | INTERVAL '1-2' YEAR TO MONTH / CAST(3 AS INT)             | INTERVAL '0-5' YEAR TO MONTH  |
        | ym * bigint   | INTERVAL '1-2' YEAR TO MONTH * CAST(3 AS BIGINT)          | INTERVAL '3-6' YEAR TO MONTH  |
        | bigint * ym   | CAST(3 AS BIGINT) * INTERVAL '1-2' YEAR TO MONTH          | INTERVAL '3-6' YEAR TO MONTH  |
        | ym / bigint   | INTERVAL '1-2' YEAR TO MONTH / CAST(3 AS BIGINT)          | INTERVAL '0-5' YEAR TO MONTH  |
        | ym * float    | INTERVAL '1-2' YEAR TO MONTH * CAST(1.5 AS FLOAT)         | INTERVAL '1-9' YEAR TO MONTH  |
        | float * ym    | CAST(1.5 AS FLOAT) * INTERVAL '1-2' YEAR TO MONTH         | INTERVAL '1-9' YEAR TO MONTH  |
        | ym / float    | INTERVAL '1-2' YEAR TO MONTH / CAST(1.5 AS FLOAT)         | INTERVAL '0-9' YEAR TO MONTH  |
        | ym * double   | INTERVAL '1-2' YEAR TO MONTH * CAST(2.5 AS DOUBLE)        | INTERVAL '2-11' YEAR TO MONTH |
        | double * ym   | CAST(2.5 AS DOUBLE) * INTERVAL '1-2' YEAR TO MONTH        | INTERVAL '2-11' YEAR TO MONTH |
        | ym / double   | INTERVAL '1-2' YEAR TO MONTH / CAST(2.5 AS DOUBLE)        | INTERVAL '0-6' YEAR TO MONTH  |
        | ym * decimal  | INTERVAL '1-2' YEAR TO MONTH * CAST(1.5 AS DECIMAL(10,2)) | INTERVAL '1-9' YEAR TO MONTH  |
        | decimal * ym  | CAST(1.5 AS DECIMAL(10,2)) * INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1-9' YEAR TO MONTH  |
        | ym / decimal  | INTERVAL '1-2' YEAR TO MONTH / CAST(1.5 AS DECIMAL(10,2)) | INTERVAL '0-9' YEAR TO MONTH  |

  Rule: the months are rounded HALF_UP, away from zero

    Scenario Outline: rounding a scaled year-month interval: <case>
      When query
        """
        SELECT CAST(<expression> AS STRING) AS v
        """
      Then query result
        | v       |
        | <value> |

      Examples:
        | case                 | expression                                      | value                          |
        | exactly a half up    | INTERVAL '1' MONTH * CAST(2.5 AS DOUBLE)        | INTERVAL '0-3' YEAR TO MONTH   |
        | half a month is one  | INTERVAL '1' MONTH * CAST(0.5 AS DOUBLE)        | INTERVAL '0-1' YEAR TO MONTH   |
        | negative rounds away | INTERVAL '1' MONTH * CAST(-0.5 AS DOUBLE)       | INTERVAL '-0-1' YEAR TO MONTH  |
        | a decimal factor     | INTERVAL '1' MONTH * CAST(1.5 AS DECIMAL(10,2)) | INTERVAL '0-2' YEAR TO MONTH   |
        | one month halved     | INTERVAL '1' MONTH / CAST(2 AS INT)             | INTERVAL '0-1' YEAR TO MONTH   |
        | three months halved  | INTERVAL '3' MONTH / CAST(2 AS INT)             | INTERVAL '0-2' YEAR TO MONTH   |
        | divided by a half    | INTERVAL '1' MONTH / CAST(0.5 AS DOUBLE)        | INTERVAL '0-2' YEAR TO MONTH   |
        | a negative interval  | INTERVAL '-1-2' YEAR TO MONTH * CAST(2 AS INT)  | INTERVAL '-2-4' YEAR TO MONTH  |
        | times zero           | INTERVAL '1-2' YEAR TO MONTH * CAST(0 AS INT)   | INTERVAL '0-0' YEAR TO MONTH   |
        | wide but in range    | INTERVAL '1' MONTH * CAST(2000000000 AS INT)    | INTERVAL '166666666-8' YEAR TO MONTH |

  Rule: a scaled year-month interval is a year-month interval

    Scenario Outline: the type of a scaled year-month interval: <case>
      When query
        """
        SELECT typeof(<expression>) AS t
        """
      Then query result
        | t                      |
        | interval year to month |

      Examples:
        | case              | expression                                       |
        | multiplied        | INTERVAL '1-2' YEAR TO MONTH * CAST(2 AS INT)    |
        | divided           | INTERVAL '1-2' YEAR TO MONTH / CAST(2 AS INT)    |
        | a NULL factor     | INTERVAL '1-2' YEAR TO MONTH * CAST(NULL AS INT) |
        | a NULL divisor    | INTERVAL '1-2' YEAR TO MONTH / CAST(NULL AS INT) |

    Scenario Outline: a NULL operand scales to NULL: <case>
      When query
        """
        SELECT <expression> IS NULL AS v
        """
      Then query result
        | v    |
        | true |

      Examples:
        | case           | expression                                       |
        | a NULL factor  | INTERVAL '1-2' YEAR TO MONTH * CAST(NULL AS INT) |
        | a NULL divisor | INTERVAL '1-2' YEAR TO MONTH / CAST(NULL AS INT) |

  Rule: scaling a year-month interval past its bounds is an error, ANSI or not

    # The interval divisions do not read the ANSI flag at all (`IntervalDivide`), so
    # `INTERVAL_DIVIDED_BY_ZERO` is raised with ANSI off too -- unlike a numeric `/`, which returns
    # NULL there. Both modes are asserted for exactly that reason.
    Scenario Outline: scaling out of bounds: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expression> AS v
        """
      Then query error <error>

      Examples:
        | case                   | ansi  | expression                                      | error                |
        | divided by zero off    | false | INTERVAL '1' MONTH / CAST(0 AS INT)             | (?i)division by zero |
        | divided by zero on     | true  | INTERVAL '1' MONTH / CAST(0 AS INT)             | (?i)division by zero |
        | divided by zero double | false | INTERVAL '1' MONTH / CAST(0 AS DOUBLE)          | (?i)division by zero |
        | divided by zero on dbl | true  | INTERVAL '1' MONTH / CAST(0 AS DOUBLE)          | (?i)division by zero |
        | overflowing bigint off | false | INTERVAL '10' YEAR * CAST(9000000000 AS BIGINT) | (?i)overflow         |
        | overflowing bigint on  | true  | INTERVAL '10' YEAR * CAST(9000000000 AS BIGINT) | (?i)overflow         |
        | out of range double    | false | INTERVAL '1' MONTH * CAST(1e18 AS DOUBLE)       | (?i)out of range     |
        | out of range on        | true  | INTERVAL '1' MONTH * CAST(1e18 AS DOUBLE)       | (?i)out of range     |
        | a NaN factor           | false | INTERVAL '1' MONTH * CAST('NaN' AS DOUBLE)      | (?i)infinite or NaN  |
        | a NaN factor on        | true  | INTERVAL '1' MONTH * CAST('NaN' AS DOUBLE)      | (?i)infinite or NaN  |

  Rule: a day-time interval scales by the same rule as a year-month one

    Scenario: scaling a non-null day-time interval keeps Spark's non-nullable schema
      When query
        """
        SELECT INTERVAL '1' DAY * 2 AS value
        """
      Then query schema
        """
        root
         |-- value: interval day to second (nullable = false)
        """

    # `MultiplyDTInterval`/`DivideDTInterval` (`BinaryArithmeticWithDatetimeResolver.scala:156-157,
    # 169`) scale the MICROS and round HALF_UP, exactly as the year-month pair scales the months.
    # Sail spelled this one as an Arrow `Duration` and let DataFusion do it, which was wrong three
    # different ways: a DECIMAL divisor was refused outright, the product was TRUNCATED instead of
    # rounded, and a zero divisor gave NULL with ANSI off where Spark raises. The first was a
    # rejection; the other two answered, wrongly.
    Scenario Outline: scaling a day-time interval: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(<expression> AS STRING) AS v
        """
      Then query result
        | v       |
        | <value> |

      Examples:
        | case                | ansi  | expression                                                        | value                                    |
        | day over decimal    | false | INTERVAL '1' DAY / CAST(1.5 AS DECIMAL(10,2))                     | INTERVAL '0 16:00:00' DAY TO SECOND      |
        | day over decimal on | true  | INTERVAL '1' DAY / CAST(1.5 AS DECIMAL(10,2))                     | INTERVAL '0 16:00:00' DAY TO SECOND      |
        | dts over decimal    | false | INTERVAL '1 02:03:04' DAY TO SECOND / CAST(1.5 AS DECIMAL(10,2))  | INTERVAL '0 17:22:02.666667' DAY TO SECOND |
        | hts over decimal    | false | INTERVAL '02:03:04' HOUR TO SECOND / CAST(1.5 AS DECIMAL(10,2))   | INTERVAL '0 01:22:02.666667' DAY TO SECOND |
        | day over decimal 38 | false | INTERVAL '1' DAY / CAST(3 AS DECIMAL(38,0))                       | INTERVAL '0 08:00:00' DAY TO SECOND      |
        | day times decimal   | false | INTERVAL '1' DAY * CAST(1.5 AS DECIMAL(10,2))                     | INTERVAL '1 12:00:00' DAY TO SECOND      |
        | decimal times day   | false | CAST(1.5 AS DECIMAL(10,2)) * INTERVAL '1' DAY                     | INTERVAL '1 12:00:00' DAY TO SECOND      |
        | a micro halved up   | false | INTERVAL '0.000001' SECOND / CAST(2 AS DECIMAL(10,2))             | INTERVAL '0 00:00:00.000001' DAY TO SECOND |
        | half a micro is one | false | INTERVAL '0.000001' SECOND * CAST(0.5 AS DECIMAL(10,2))           | INTERVAL '0 00:00:00.000001' DAY TO SECOND |
        | a third of a micro  | false | INTERVAL '0.000001' SECOND / CAST(3 AS INT)                       | INTERVAL '0 00:00:00' DAY TO SECOND      |
        | two fifths of one   | false | INTERVAL '0.000001' SECOND * CAST(0.4 AS DOUBLE)                  | INTERVAL '0 00:00:00' DAY TO SECOND      |

    # The one that answered instead of raising: an interval divided by zero is an error in BOTH
    # ANSI modes, where a numeric `/` returns NULL with ANSI off.
    Scenario Outline: a day-time interval divided by zero raises with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT INTERVAL '1' DAY / CAST(0 AS DECIMAL(10,2)) AS v
        """
      Then query error (?i)division by zero

      Examples:
        | ansi  |
        | false |
        | true  |

  Rule: a DECIMAL scales an interval exactly, not through a DOUBLE

    # Each of the four expressions has its own `DecimalType` arm (`intervalExpressions.scala:616-618,
    # 665-667,756-758,835-837`): exact `Decimal` arithmetic, then `setScale(0, HALF_UP)`. `45 * 0.70`
    # is exactly 31.5 and rounds to 32; through a DOUBLE it is 31.499999999999996 and rounds to 31.
    # The DOUBLE row is the control: there Spark does go through a double.
    Scenario Outline: scaling an interval by a decimal is exact: <case> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(<expression> AS STRING) AS v
        """
      Then query result
        | v       |
        | <value> |

      Examples:
        | case                     | ansi  | expression                                                                 | value                                           |
        | ym times a decimal tie   | false | INTERVAL '45' MONTH * CAST(0.7 AS DECIMAL(10,2))                           | INTERVAL '2-8' YEAR TO MONTH                    |
        | ym times a decimal tie   | true  | INTERVAL '45' MONTH * CAST(0.7 AS DECIMAL(10,2))                           | INTERVAL '2-8' YEAR TO MONTH                    |
        | a decimal literal        | false | INTERVAL '45' MONTH * 0.7                                                  | INTERVAL '2-8' YEAR TO MONTH                    |
        | a negative tie           | false | INTERVAL '-45' MONTH * CAST(0.7 AS DECIMAL(10,2))                          | INTERVAL '-2-8' YEAR TO MONTH                   |
        | ym over a decimal tie    | false | INTERVAL '7' MONTH / CAST(0.56 AS DECIMAL(10,2))                           | INTERVAL '1-1' YEAR TO MONTH                    |
        | a double stays a double  | false | INTERVAL '45' MONTH * CAST(0.7 AS DOUBLE)                                  | INTERVAL '2-7' YEAR TO MONTH                    |
        | dt times a decimal tie   | false | INTERVAL '0.000045' SECOND * CAST(0.7 AS DECIMAL(10,2))                    | INTERVAL '0 00:00:00.000032' DAY TO SECOND      |
        | dt over a decimal tie    | false | INTERVAL '0.000007' SECOND / CAST(0.56 AS DECIMAL(10,2))                   | INTERVAL '0 00:00:00.000013' DAY TO SECOND      |
        | a wide dt keeps its micro| false | INTERVAL '200000 00:00:00.000001' DAY TO SECOND * CAST(1 AS DECIMAL(10,2)) | INTERVAL '200000 00:00:00.000001' DAY TO SECOND |

  Rule: a day-time interval rounded onto 2^63 microseconds is out of range

    # `DoubleMath.roundToLong` (`intervalExpressions.scala:669,839`) accepts -2^63 but not 2^63, and
    # `Long.MaxValue` micros is 2^63 once it is a DOUBLE -- so this raises instead of saturating.
    Scenario Outline: a day-time interval scaled onto 2^63 raises: <case> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(<expression> AS STRING) AS v
        """
      Then query error (?i)out of range

      Examples:
        | case                 | ansi  | expression                                                             |
        | the max times one    | false | INTERVAL '106751991 04:00:54.775807' DAY TO SECOND * CAST(1 AS DOUBLE) |
        | the max times one    | true  | INTERVAL '106751991 04:00:54.775807' DAY TO SECOND * CAST(1 AS DOUBLE) |
        | the max over one     | false | INTERVAL '106751991 04:00:54.775807' DAY TO SECOND / CAST(1 AS DOUBLE) |
        | half the max doubled | true  | INTERVAL '53375995 14:00:27.387904' DAY TO SECOND * CAST(2 AS DOUBLE)  |
