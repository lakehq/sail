Feature: ifnull output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to ifnull yields the schema Spark declares
      When query
        """
        SELECT ifnull(NULL, array('2')) AS result
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
    Scenario Outline: ifnull of <case> is <type> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(ifnull(<left>, <right>)) AS t
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

    Scenario: ifnull of a date and a string is refused beside an INT with ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT ifnull(DATE'2024-01-15', '2024-01-16') * 2 AS v
        """
      Then query error (?i)cannot resolve

    Scenario: ifnull of a date and a string shifts by a year-month interval with ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(ifnull(DATE'2024-01-15', '2024-01-16') + INTERVAL '1-2' YEAR TO MONTH AS STRING) AS v
        """
      Then query result
        | v          |
        | 2025-03-15 |
