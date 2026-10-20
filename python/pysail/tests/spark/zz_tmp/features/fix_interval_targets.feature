Feature: an interval keeps the field range Spark declares for it

  # Targets for `fix/interval`, taken from PR #2526 (`poc/aritmetic_reject`), where every one of them
  # is pinned `@sail-bug` because an Arrow type cannot carry `start_field`/`end_field`. They are
  # grouped by what `fix/interval` has to do for them to pass. Every expected value was measured on
  # the Spark 4.2 JVM when the scenario was written; re-run them there before trusting them.
  #
  # When `fix/interval` lands AFTER PR #2526, the same scenarios in the PR's files XPASS, and a
  # strict xfail that passes is a hard CI failure: remove `@sail-bug` from them in the same change.
  # Where they live in PR #2526:
  #   math/arithmetic_result_type.feature       -- groups A and B
  #   math/arithmetic_operand_rejection.feature -- group A (the two type-name scenarios)
  #   math/arithmetic_operand_resolution.feature, arithmetic_string_promotion.feature,
  #   arithmetic_time_subtraction.feature        -- group B
  #   math/arithmetic_derived_operand.feature    -- groups C and D

  Rule: A -- a declared interval keeps its range (the metadata on a declared type or literal)

    # Spark's `YearMonthIntervalType`/`DayTimeIntervalType` carry `start_field` and `end_field`
    # (`YearMonthIntervalType.scala:50-59`, `DayTimeIntervalType.scala:54-63`) and name the range
    # they were declared with. Adding two intervals of the same range keeps it.
    Scenario Outline: <case> keeps its field range
      When query
        """
        SELECT typeof(<expression>) AS result
        """
      Then query result
        | result |
        | <type> |

      Examples:
        | case          | expression                              | type                 |
        | month + month | INTERVAL '2' MONTH + INTERVAL '1' MONTH | interval month       |
        | year + year   | INTERVAL '2' YEAR + INTERVAL '1' YEAR   | interval year        |
        | day + day     | INTERVAL '2' DAY + INTERVAL '1' DAY     | interval day         |
        | hour + hour   | INTERVAL '25' HOUR + INTERVAL '1' HOUR  | interval hour        |
        | day + hour    | INTERVAL '2' DAY + INTERVAL '25' HOUR   | interval day to hour |

    # The arithmetic rejection names the operand by its declared range. Day-time intervals cannot
    # diverge in this message: Spark rewrites `x + <day-time>` through the datetime resolver and never
    # names the interval type.
    Scenario: a year-only INTERVAL operand keeps its field range in the type name
      When query
        """
        SELECT array(1) + INTERVAL '2' YEAR
        """
      Then query error (?i:cannot resolve).*INTERVAL YEAR(?! TO)

    Scenario: a month-only INTERVAL operand keeps its field range in the type name
      When query
        """
        SELECT array(1) + INTERVAL '2' MONTH
        """
      Then query error (?i:cannot resolve).*INTERVAL MONTH

  Rule: B -- a COMPUTED interval gets the range Spark gives it (metadata on an expression's result)

    # `SubtractDates` returns `DayTimeIntervalType(DAY)` (`datetimeExpressions.scala:3616-3617`).
    # In PR #2526 `date - date` is an INT day count, because a `Duration` without a range is read by
    # seconds downstream; this group needs `date - date` to be a `Duration` carrying DAY.
    Scenario: a date minus a date is an interval with Spark's field range
      When query
        """
        SELECT typeof(DATE'2024-01-15' - DATE'2024-01-01') AS result
        """
      Then query result
        | result       |
        | interval day |

    # `SubtractTimes` returns `DayTimeIntervalType(HOUR, SECOND)` (`timeExpressions.scala:626`).
    @spark-4.1
    Scenario: TIME subtraction preserves Spark's HOUR TO SECOND interval subtype
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT
          typeof(
            CAST(TIME '12:00:00.000001' AS TIME(6)) -
            CAST(TIME '12:00:01.250' AS TIME(3))
          ) AS result_type,
          CAST(
            CAST(TIME '12:00:00.000001' AS TIME(6)) -
            CAST(TIME '12:00:01.250' AS TIME(3))
            AS STRING
          ) AS result
        """
      Then query result
        | result_type             | result                                     |
        | interval hour to second | INTERVAL '-00:00:01.249999' HOUR TO SECOND |

    # A string read as a date or a TIME beside one subtracts like the typed pair.
    @spark-4.1
    Scenario Outline: a string as a datetime keeps Spark's field range: <case> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(<expression>) AS t
        """
      Then query result
        | t      |
        | <type> |

      Examples:
        | case       | ansi  | expression                      | type                    |
        | str - date | false | '2024-01-15' - DATE'2024-01-01' | interval day            |
        | str - date | true  | '2024-01-15' - DATE'2024-01-01' | interval day            |
        | date - str | true  | DATE'2024-01-15' - '2024-01-01' | interval day            |
        | str - time | true  | '06:00:00' - TIME'01:00:00'     | interval hour to second |
        | time - str | true  | TIME'06:00:00' - '01:00:00'     | interval hour to second |

  Rule: C -- a day-time interval cast to a number is read by its END field

    # `IntervalUtils.scala:921-928`: a `DayTimeIntervalType(DAY)` cast to a number is a day count.
    # A `Duration` carries no field, so Sail reads seconds (1209600). Group B cannot land without this
    # one, or `CAST(date - date AS INT)` regresses from 14 to 1209600.
    Scenario: a DAY interval cast to INT is its day count
      When query
        """
        SELECT CAST(INTERVAL '14' DAY AS INT) AS result
        """
      Then query result
        | result |
        | 14     |

    Scenario: a date difference cast to INT stays a day count
      When query
        """
        SELECT CAST(DATE'2024-01-15' - DATE'2024-01-01' AS INT) AS result
        """
      Then query result
        | result |
        | 14     |

  Rule: D -- the arithmetic PR #2526 left refusing, and its consumers (needs B and C)

    # The only arithmetic pair where PR #2526 refuses what Spark accepts: with `date - date` an INT,
    # the outer operator sees INT beside an interval or a timestamp and refuses, as Spark would for
    # those operands. With `date - date` a DAY interval, both resolve.
    Scenario Outline: <case> resolves
      When query
        """
        SELECT CAST(<expression> AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                               | expression                                                             | result              |
        | a date difference plus an interval | (DATE'2024-01-15' - DATE'2024-01-01') + INTERVAL '2' DAY               | INTERVAL '16' DAY   |
        | a timestamp plus a date difference | TIMESTAMP'2024-01-01 00:00:00' + (DATE'2024-01-15' - DATE'2024-01-01') | 2024-01-15 00:00:00 |

    Scenario Outline: a date difference <case>
      When query
        """
        SELECT <expression> AS result
        """
      Then query result
        | result  |
        | <value> |

      Examples:
        | case                      | expression                                                            | value |
        | compares with an interval | (DATE'2024-01-15' - DATE'2024-01-01') < INTERVAL '15' DAY             | true  |
        | extracts its days         | extract(DAY FROM DATE'2024-01-15' - DATE'2024-01-01')                 | 14    |
        | subtracts an hour         | (DATE'2024-01-15' - DATE'2024-01-01') - INTERVAL '1' HOUR IS NOT NULL | true  |

    # The other side of the same root: once `date - date` is an interval, adding an INT to it must be
    # REFUSED, as Spark refuses it. PR #2526 accepts both of these today.
    Scenario Outline: <case> is refused
      When query
        """
        SELECT <expression> AS result
        """
      Then query error (?i)cannot resolve

      Examples:
        | case                         | expression                                               |
        | a date difference plus an INT | (DATE'2024-01-15' - DATE'2024-01-01') + CAST(2 AS INT)  |
        | a shifted date plus an INT    | (DATE'2024-01-15' + INTERVAL '25' HOUR) + CAST(2 AS INT) |
