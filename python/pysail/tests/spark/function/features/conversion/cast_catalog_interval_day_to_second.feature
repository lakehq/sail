# CAST scenarios imported from test/bug_catalog (0886a9e7f): datetime/interval_day_to_second.feature
Feature: Additional CAST coverage from interval_day_to_second

  Rule: A string casts to a day-time interval

    @sail-bug
    Scenario Outline: interval_day_to_second catalog: string to day-time interval cast: <case>
      When query
        """
        SELECT CAST(<expr> AS INTERVAL DAY TO SECOND) AS result FROM VALUES ('1 02:03:04'), ('-2 10:00:00.5') AS t(s)
        """
      Then query result
        | result   |
        | <r1>     |
        | <r2>     |

      Examples:
        | case                          | expr                    | r1                                         | r2                                         |
        | column                        | s                       | INTERVAL '1 02:03:04' DAY TO SECOND        | INTERVAL '-2 10:00:00.5' DAY TO SECOND     |
        | literal drops sub-micro digits | '1 02:03:04.123456789' | INTERVAL '1 02:03:04.123456' DAY TO SECOND | INTERVAL '1 02:03:04.123456' DAY TO SECOND |

    @sail-bug
    Scenario Outline: interval_day_to_second catalog: a malformed string to day-time interval cast: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(s AS INTERVAL DAY TO SECOND) AS result FROM VALUES ('abc'), ('1 02:03:04') AS t(s)
        """
      Then query error \[INVALID_INTERVAL_FORMAT\.UNMATCHED_FORMAT_STRING_WITH_NOTICE\]

      Examples:
        | case     | ansi  |
        | ANSI on  | true  |
        | ANSI off | false |

    @sail-bug
    Scenario: interval_day_to_second catalog: try_cast of a malformed string to a day-time interval returns NULL
      When query
        """
        SELECT try_cast(s AS INTERVAL DAY TO SECOND) AS result FROM VALUES ('abc'), ('1 02:03:04') AS t(s)
        """
      Then query result
        | result                              |
        | NULL                                |
        | INTERVAL '1 02:03:04' DAY TO SECOND |

  # Spark 4.2.0 Cast.scala: a day-time interval casts to an integral in units of its END
  # field (IntervalUtils.dayTimeIntervalToLong); a number casts to a day-time interval in
  # units of the target's end field and raises CAST_OVERFLOW (both ANSI modes) when it
  # does not fit. End-field integral casts are also covered in
  # cast_day_time_interval_integral.feature; numeric-to-interval overflow remains separate.

  Rule: Casts between day-time intervals and numbers

    Scenario: interval_day_to_second catalog: a DAY TO SECOND interval casts to BIGINT seconds
      When query
        """
        SELECT CAST(INTERVAL '1 02:03:04' DAY TO SECOND AS BIGINT) AS result
        """
      Then query result
        | result |
        | 93784  |

    Scenario Outline: interval_day_to_second catalog: a single-unit day-time interval casts to INT in its own unit: <case>
      When query
        """
        SELECT CAST(<expr> AS INT) AS result FROM VALUES (INTERVAL '2' DAY), (INTERVAL '-5' DAY) AS t(i)
        """
      Then query result
        | result |
        | <r1>   |
        | <r2>   |

      Examples:
        | case            | expr                | r1 | r2 |
        | day column      | i                   | 2  | -5 |
        | hour literal    | INTERVAL '2' HOUR   | 2  | 2  |
        | minute literal  | INTERVAL '90' MINUTE | 90 | 90 |

    Scenario: interval_day_to_second catalog: a number casts to DAY TO SECOND as seconds
      When query
        """
        SELECT CAST(CAST(n AS INTERVAL DAY TO SECOND) AS STRING) AS result FROM VALUES (2), (-3) AS t(n)
        """
      Then query result
        | result                               |
        | INTERVAL '0 00:00:02' DAY TO SECOND  |
        | INTERVAL '-0 00:00:03' DAY TO SECOND |

    Scenario Outline: interval_day_to_second catalog: a BIGINT that does not fit INTERVAL DAY: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(n AS INTERVAL DAY) AS result
        FROM VALUES (CAST(1 AS BIGINT)), (CAST(9223372036854775807 AS BIGINT)) AS t(n)
        """
      Then query error \[CAST_OVERFLOW\]

      Examples:
        | case     | ansi  |
        | ANSI on  | true  |
        | ANSI off | false |

    Scenario: interval_day_to_second catalog: try_cast of a BIGINT that does not fit INTERVAL DAY returns NULL
      When query
        """
        SELECT CAST(try_cast(CAST(9223372036854775807 AS BIGINT) AS INTERVAL DAY) AS STRING) AS result
        """
      Then query result
        | result |
        | NULL   |
