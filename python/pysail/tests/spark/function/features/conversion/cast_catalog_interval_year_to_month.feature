# CAST scenarios imported from test/bug_catalog (0886a9e7f): datetime/interval_year_to_month.feature
Feature: Additional CAST coverage from interval_year_to_month

  Rule: Casts between year-month intervals and numbers or strings

    Scenario Outline: interval_year_to_month catalog: a year-month interval casts to an integral: <case>
      When query
        """
        SELECT CAST(<expr> AS <type>) AS result FROM VALUES (make_ym_interval(1, 2)), (make_ym_interval(-3, 0)) AS t(i)
        """
      Then query result
        | result |
        | <r1>   |
        | <r2>   |

      Examples:
        | case                     | expr                            | type   | r1  | r2  |
        | column to INT in months  | i                               | INT    | 14  | -36 |
        | literal to BIGINT        | INTERVAL '-1-2' YEAR TO MONTH   | BIGINT | -14 | -14 |
        | year literal to INT      | INTERVAL '3' YEAR               | INT    | 3   | 3   |

    Scenario: interval_year_to_month catalog: a number casts to YEAR TO MONTH as months
      When query
        """
        SELECT CAST(CAST(n AS INTERVAL YEAR TO MONTH) AS STRING) AS result FROM VALUES (2), (-3) AS t(n)
        """
      Then query result
        | result                        |
        | INTERVAL '0-2' YEAR TO MONTH  |
        | INTERVAL '-0-3' YEAR TO MONTH |

    Scenario: interval_year_to_month catalog: a number casts to INTERVAL YEAR as years
      When query
        """
        SELECT CAST(CAST(CAST(n AS INTERVAL YEAR) AS INTERVAL YEAR TO MONTH) AS STRING) AS result
        FROM VALUES (2), (-3) AS t(n)
        """
      Then query result
        | result                        |
        | INTERVAL '2-0' YEAR TO MONTH  |
        | INTERVAL '-3-0' YEAR TO MONTH |

    Scenario Outline: interval_year_to_month catalog: a number that does not fit INTERVAL YEAR: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(CAST(n AS INTERVAL YEAR) AS STRING) AS result FROM VALUES (1), (200000000) AS t(n)
        """
      Then query error \[CAST_OVERFLOW\]

      Examples:
        | case     | ansi  |
        | ANSI on  | true  |
        | ANSI off | false |

    Scenario: interval_year_to_month catalog: a string casts to a year-month interval
      When query
        """
        SELECT CAST(CAST(s AS INTERVAL YEAR TO MONTH) AS STRING) AS result FROM VALUES ('1-2'), ('-3-4') AS t(s)
        """
      Then query result
        | result                        |
        | INTERVAL '1-2' YEAR TO MONTH  |
        | INTERVAL '-3-4' YEAR TO MONTH |
