Feature: a day-time interval scaled by a number, vs Spark 4.2.0

  # `MultiplyDTInterval` and `DivideDTInterval` (`intervalExpressions.scala`) scale the microseconds
  # and round HALF_UP, away from zero on a tie; a zero divisor is `INTERVAL_DIVIDED_BY_ZERO`. The
  # result is `DayTimeIntervalType()`, the full DAY TO SECOND range, whatever the operand's range was.
  # Every row was measured on the Spark 4.2 JVM under both ANSI modes, and none changes with ANSI.

  Rule: the scaled interval keeps the value Spark computes

    Scenario Outline: <expression> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(<expression>) AS t, CAST(<expression> AS STRING) AS v
        """
      Then query result
        | t                      | v       |
        | interval day to second | <value> |

      Examples:
        | ansi  | expression                                        | value                                       |
        | false | INTERVAL '3' DAY * 2                              | INTERVAL '6 00:00:00' DAY TO SECOND         |
        | true  | 2 * INTERVAL '3' DAY                              | INTERVAL '6 00:00:00' DAY TO SECOND         |
        | false | INTERVAL '3' HOUR * CAST(2 AS BIGINT)             | INTERVAL '0 06:00:00' DAY TO SECOND         |
        | true  | INTERVAL '1 02:03:04.5' DAY TO SECOND * 3         | INTERVAL '3 06:09:13.5' DAY TO SECOND       |
        | true  | INTERVAL '10' MINUTE * NULL                       | NULL                                        |
        | false | INTERVAL '7' DAY / 2                              | INTERVAL '3 12:00:00' DAY TO SECOND         |
        | false | INTERVAL '7' DAY / CAST(4 AS DOUBLE)              | INTERVAL '1 18:00:00' DAY TO SECOND         |
        | true  | INTERVAL '0.000001' SECOND * 0.5                  | INTERVAL '0 00:00:00.000001' DAY TO SECOND  |
        | false | INTERVAL '-0.000001' SECOND * 0.5                 | INTERVAL '-0 00:00:00.000001' DAY TO SECOND |
        | true  | INTERVAL '0.000001' SECOND * CAST(2.5 AS DOUBLE)  | INTERVAL '0 00:00:00.000003' DAY TO SECOND  |
        | false | INTERVAL '-0.000001' SECOND * CAST(2.5 AS DOUBLE) | INTERVAL '-0 00:00:00.000003' DAY TO SECOND |
        | false | INTERVAL '10' MINUTE * '2'                        | INTERVAL '0 00:20:00' DAY TO SECOND         |
        | false | INTERVAL '0.000003' SECOND / 2                    | INTERVAL '0 00:00:00.000002' DAY TO SECOND  |
        | true  | INTERVAL '-0.000003' SECOND / 2                   | INTERVAL '-0 00:00:00.000002' DAY TO SECOND |
        | true  | INTERVAL '7' DAY / 0.5                            | INTERVAL '14 00:00:00' DAY TO SECOND        |
        | true  | INTERVAL '1' HOUR / '4'                           | INTERVAL '0 00:15:00' DAY TO SECOND         |

  Rule: a zero divisor is an error in both modes

    # The error is raised when the row is evaluated, not when the query is planned: a division that
    # is never evaluated does not fail.
    Scenario Outline: a zero divisor that is not evaluated with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(if(false, INTERVAL '1' DAY / 0, NULL) AS STRING) AS v
        """
      Then query result
        | v    |
        | NULL |

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario Outline: <expression> with ANSI <ansi> is refused at run time
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expression> AS v
        """
      Then query error INTERVAL_DIVIDED_BY_ZERO

      Examples:
        | ansi  | expression                           |
        | false | INTERVAL '1' DAY / 0                 |
        | true  | INTERVAL '1' DAY / 0                 |
        | false | INTERVAL '1' DAY / CAST(0 AS DOUBLE) |
        | true  | INTERVAL '1' DAY / 0.0               |
