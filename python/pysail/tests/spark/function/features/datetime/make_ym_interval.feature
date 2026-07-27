@make_ym_interval
Feature: make_ym_interval builds a year-month interval from years and months

  Rule: A NULL in any argument yields NULL (Spark MakeYMInterval is null-intolerant)
    Scenario Outline: NULL argument: <case>
      When query
        """
        SELECT make_ym_interval(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                            | args                 |
        | NULL months yields NULL         | 1, NULL              |
        | NULL years yields NULL          | NULL, 6              |
        | both arguments NULL yields NULL | NULL, NULL           |
        | typed NULL argument yields NULL | CAST(NULL AS INT), 6 |

    Scenario: NULL propagates per row over a column
      When query
        """
        SELECT make_ym_interval(y, m) AS result
        FROM VALUES (1, 6), (CAST(NULL AS INT), 3), (2, CAST(NULL AS INT)) AS t(y, m)
        """
      Then query result
        | result                       |
        | INTERVAL '1-6' YEAR TO MONTH |
        | NULL                         |
        | NULL                         |

  Rule: Non-NULL arguments build a year-month interval
    Scenario Outline: Build: <case>
      When query
        """
        SELECT make_ym_interval(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                     | args   | result                         |
        | years and months combine                                 | 1, 6   | INTERVAL '1-6' YEAR TO MONTH   |
        | zero years and months                                    | 0, 0   | INTERVAL '0-0' YEAR TO MONTH   |
        | negative years and months                                | -1, -6 | INTERVAL '-1-6' YEAR TO MONTH  |
        | months overflowing into years are normalized             | 2, 13  | INTERVAL '3-1' YEAR TO MONTH   |
        | negative years with positive months normalize below zero | -1, 1  | INTERVAL '-0-11' YEAR TO MONTH |

  Rule: Omitted arguments default to zero
    Scenario Outline: Omitted argument: <case>
      When query
        """
        SELECT make_ym_interval(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                         | args | result                       |
        | no arguments builds a zero interval          |      | INTERVAL '0-0' YEAR TO MONTH |
        | single argument builds a whole-year interval | 2    | INTERVAL '2-0' YEAR TO MONTH |
        | single NULL argument yields NULL             | NULL | NULL                         |

  Rule: More than two arguments is an error
    Scenario: three arguments is rejected
      When query
        """
        SELECT make_ym_interval(1, 2, 3) AS result
        """
      Then query error make_ym_interval

  Rule: Integer overflow is an error regardless of ANSI mode
    Scenario Outline: Integer overflow is an error regardless of ANSI mode: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT make_ym_interval(200000000, 0) AS result
        """
      Then query error INTERVAL_ARITHMETIC_OVERFLOW

      Examples:
        | case                               | ansi  |
        | overflow errors with ANSI enabled  | true  |
        | overflow errors with ANSI disabled | false |

  @spark_null @spark-4
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to make_ym_interval yields the schema Spark declares
      When query
        """
        SELECT make_ym_interval(1, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: interval year to month (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to make_ym_interval yields the schema Spark declares
      When query
        """
        SELECT make_ym_interval(CAST(id AS INT), 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: interval year to month (nullable = false)
        """

    Scenario: a nullable column input to make_ym_interval stays nullable
      When query
        """
        SELECT make_ym_interval(c, 2) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: interval year to month (nullable = true)
        """
