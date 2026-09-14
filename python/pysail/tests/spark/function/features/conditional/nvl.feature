Feature: nvl output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to nvl yields the schema Spark declares
      When query
        """
        SELECT nvl(NULL, array('2')) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = false)
        """

  Rule: a string beside a date or timestamp takes the datetime type with ANSI on

    # `Nvl` is `Coalesce(Seq(left, right))` (`nullExpressions.scala:246`), so it types the pair the way
    # `coalesce` does: with ANSI on `AnsiTypeCoercion` widens a STRING beside a DATE or TIMESTAMP to that
    # datetime type, and with it off the datetime becomes a STRING. The type decides the arithmetic.
    Scenario Outline: nvl of <case> is <type> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(nvl(<left>, <right>)) AS t
        """
      Then query result
        | t      |
        | <type> |

      Examples:
        | case                  | ansi  | left                           | right                          | type      |
        | a date and a string   | true  | DATE'2024-01-15'               | '2024-01-16'                   | date      |
        | a string and a date   | true  | '2024-01-16'                   | DATE'2024-01-15'               | date      |
        | a timestamp, a string | true  | TIMESTAMP'2024-01-15 01:00:00' | '2024-01-16'                   | timestamp |
        | a date and a string   | false | DATE'2024-01-15'               | '2024-01-16'                   | string    |
        | a string and a NULL   | true  | '2'                            | NULL                           | string    |

    Scenario: nvl of a date and a string is refused beside an INT with ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT nvl(DATE'2024-01-15', '2024-01-16') * 2 AS v
        """
      Then query error (?i)cannot resolve

    Scenario: nvl of a date and a string shifts by a year-month interval with ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(nvl(DATE'2024-01-15', '2024-01-16') + INTERVAL '1-2' YEAR TO MONTH AS STRING) AS v
        """
      Then query result
        | v          |
        | 2025-03-15 |

    # `coalesce` cannot type a TIMESTAMP beside a DATE yet, so that pair stays on `nvl`; it must resolve.
    Scenario Outline: nvl of <case> resolves with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT nvl(<left>, <right>) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case                 | ansi  | left                              | right                          |
        | a timestamp, a date  | false | TIMESTAMP'2024-01-15 10:00:00'    | DATE'2024-01-16'               |
        | a date, a timestamp  | true  | DATE'2024-01-16'                  | TIMESTAMP'2024-01-15 10:00:00' |
        | an ntz, a date       | false | TIMESTAMP_NTZ'2024-01-15 10:00:00' | DATE'2024-01-16'              |
        | two dates            | true  | CAST(NULL AS DATE)                | DATE'2024-01-16'               |

  Rule: an interval or a TIME keeps its type through nvl

    # `Nvl` is `Coalesce(Seq(left, right))` (`nullExpressions.scala:246`), so an INTERVAL or a TIME
    # stays one. DataFusion's `nvl` made it a STRING: the date shift was refused, and the scaling
    # answered NULL with ANSI off once string promotion read that STRING with `try_cast`.
    Scenario Outline: <case> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(<expression> AS STRING) AS result
        """
      Then query result
        | result     |
        | <expected> |

      Examples:
        | case                                  | ansi  | expression                                          | expected                            |
        | a date shifted by a day-time interval | false | DATE'2024-01-01' + nvl(NULL, INTERVAL '1' DAY)      | 2024-01-02                          |
        | a date shifted by a day-time interval | true  | DATE'2024-01-01' + nvl(NULL, INTERVAL '1' DAY)      | 2024-01-02                          |
        | a scaled day-time interval            | false | nvl(NULL, INTERVAL '1' DAY) * 2                     | INTERVAL '2 00:00:00' DAY TO SECOND |
        | a scaled day-time interval            | true  | nvl(NULL, INTERVAL '1' DAY) * 2                     | INTERVAL '2 00:00:00' DAY TO SECOND |
        | a date shifted by ifnull of a month   | false | DATE'2024-01-01' + ifnull(NULL, INTERVAL '1' MONTH) | 2024-02-01                          |
        | a scaled calendar interval            | false | nvl(NULL, make_interval(0, 1, 0, 1, 0, 0, 0)) * 2   | 2 months 2 days                     |
        | a scaled calendar interval            | true  | nvl(NULL, make_interval(0, 1, 0, 1, 0, 0, 0)) * 2   | 2 months 2 days                     |

    @spark-4.1
    Scenario Outline: a TIME from nvl shifts by an interval with ANSI <ansi>
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(nvl(NULL, TIME '01:00:00') + INTERVAL '1' HOUR AS STRING) AS result
        """
      Then query result
        | result   |
        | 02:00:00 |

      Examples:
        | ansi  |
        | false |
        | true  |

    # TODO: a TIMESTAMP beside a DATE stays on DataFusion's `nvl`, which makes the pair a STRING;
    #  Spark widens it to a TIMESTAMP, so the difference below resolves there.
    @sail-bug
    Scenario: nvl of a timestamp and a date is a timestamp operand with ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(nvl(TIMESTAMP'2024-01-15 00:00:00', DATE'2024-01-16') - TIMESTAMP'2024-01-01 00:00:00' AS STRING) AS result
        """
      Then query result
        | result                               |
        | INTERVAL '14 00:00:00' DAY TO SECOND |

