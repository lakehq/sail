# Moved from features/timestampdiff.feature by the datetime/ layout reorganisation.
Feature: timestampdiff calendar units

  Rule: timestampdiff uses calendar-aware month, quarter, and year units

    Scenario Outline: Calendar unit: <case>
      When query
        """
        SELECT timestampdiff(<unit>, TIMESTAMP '<start>', TIMESTAMP '<end>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                                          | unit    | start               | end                 | result |
        | timestampdiff MONTH counts a leap February calendar month                     | MONTH   | 2024-02-01 00:00:00 | 2024-03-01 00:00:00 | 1      |
        | timestampdiff MONTH truncates incomplete trailing months                      | MONTH   | 2024-01-31 10:00:00 | 2024-02-29 09:59:59 | 0      |
        | timestampdiff MONTH includes complete trailing month at matching day and time | MONTH   | 2024-02-29 10:00:00 | 2024-03-29 10:00:00 | 1      |
        | timestampdiff QUARTER counts calendar quarters                                | QUARTER | 2024-01-01 00:00:00 | 2024-04-01 00:00:00 | 1      |
        | timestampdiff YEAR counts completed calendar years                            | YEAR    | 2020-02-29 12:00:00 | 2021-02-28 11:59:59 | 0      |
        | timestampdiff MONTH truncates negative intervals toward zero                  | MONTH   | 2024-03-01 00:00:00 | 2024-02-01 00:00:01 | 0      |

    Scenario: date_diff and datediff use the same calendar month behavior
      When query
        """
        SELECT
          date_diff(MONTH, TIMESTAMP '2024-02-01 00:00:00', TIMESTAMP '2024-03-01 00:00:00') AS date_diff_result,
          datediff(MONTH, TIMESTAMP '2024-02-01 00:00:00', TIMESTAMP '2024-03-01 00:00:00') AS datediff_result
        """
      Then query result
        | date_diff_result | datediff_result |
        | 1                | 1               |

  Rule: the unit form of a date difference is a BIGINT

    # `datediff(DAY, s, e)`, `date_diff(DAY, s, e)` and `timestampdiff(DAY, s, e)` all parse to
    # `TimestampDiff` (`SqlBaseParser.g4:1328`, `AstBuilder.scala:7106`), whose `dataType` is
    # `LongType` (`datetimeExpressions.scala:3867`); only the two-argument `datediff` is an INT.
    Scenario Outline: <function> with a DAY unit is typed bigint
      When query
        """
        SELECT typeof(<function>(DAY, DATE'2024-01-01', DATE'2024-01-15')) AS t
        """
      Then query result
        | t      |
        | bigint |

      Examples:
        | function      |
        | datediff      |
        | date_diff     |
        | timestampdiff |

    # A BIGINT is not a date offset: `DateAdd` takes only INT, SMALLINT or TINYINT.
    Scenario Outline: a date shifted by a DAY-unit difference is refused with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT DATE'2024-01-01' + timestampdiff(DAY, DATE'2024-01-01', DATE'2024-01-15') AS result
        """
      Then query error (?i)cannot resolve

      Examples:
        | ansi  |
        | false |
        | true  |
