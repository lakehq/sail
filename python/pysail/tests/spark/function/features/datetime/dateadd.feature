Feature: dateadd function

  Scenario Outline: three-argument dateadd: <case>
    When query
      """
      SELECT dateadd(<unit>, <quantity>, <value>) AS result
      """
    Then query result
      | result   |
      | <result> |

    Examples:
      | case                     | unit | quantity | value                           | result              |
      | lowercase unit           | week | 2        | DATE '2024-01-01'               | 2024-01-15 00:00:00 |
      | uppercase negative unit  | WEEK | -1       | DATE '2024-01-15'               | 2024-01-08 00:00:00 |
      | timestamp sub-day unit   | HOUR | 2        | TIMESTAMP '2024-01-15 10:00:00' | 2024-01-15 12:00:00 |

  Scenario: three-argument dateadd supports column input
    When query
      """
      SELECT dateadd(DAY, id, DATE '2024-01-15') AS result FROM range(3)
      """
    Then query result
      | result              |
      | 2024-01-15 00:00:00 |
      | 2024-01-16 00:00:00 |
      | 2024-01-17 00:00:00 |

  Scenario: two-argument dateadd remains date arithmetic
    When query
      """
      SELECT dateadd(DATE '2024-01-15', 2) AS result
      """
    Then query result
      | result     |
      | 2024-01-17 |

  @function(nullability)
  Rule: Output schema

    Scenario Outline: three-argument <function> with a date returns a timestamp
      When query
        """
        SELECT <function>(WEEK, 1, DATE '2024-01-15')
        """
      Then query schema
        """
        root
         |-- timestampadd(WEEK, 1, DATE '2024-01-15'): timestamp (nullable = false)
        """

      Examples:
        | function |
        | dateadd  |
        | date_add |

    Scenario: two-argument dateadd returns a date
      When query
        """
        SELECT dateadd(DATE '2024-01-15', 1) AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """

  # add_years is a Sail extension. The equivalent Spark timestampadd(YEAR, ...)
  # preserves NULL dates and unselected branches, and rejects evaluated overflow.
  @sail-only
  Rule: Year overflow is evaluated only for selected non-NULL dates

    Scenario Outline: Overflowing years preserve NULLs and unused branches with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT add_years(CAST(NULL AS DATE), <years>) AS literal_null,
               add_years(CASE WHEN 2 > id THEN CAST(NULL AS DATE)
                              ELSE DATE '2000-01-01' END, <years>) AS column_null,
               CASE WHEN false THEN add_years(DATE '2000-01-01', <years>)
                    ELSE DATE '2001-02-03' END AS constant_branch,
               CASE WHEN 0 > id THEN add_years(DATE '2000-01-01', <years>)
                    ELSE DATE '2001-02-03' END AS row_branch
        FROM range(2)
        """
      Then query result
        | literal_null | column_null | constant_branch | row_branch |
        | NULL         | NULL        | 2001-02-03      | 2001-02-03 |
        | NULL         | NULL        | 2001-02-03      | 2001-02-03 |

      Examples:
        | years      | ansi  |
        | 357913941  | true  |
        | 357913941  | false |
        | -357913941 | true  |
        | -357913941 | false |

    Scenario Outline: Overflowing years preserve empty scalar subqueries with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT add_years((SELECT DATE '2000-01-01' FROM range(0)), <years>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | years      | ansi  |
        | 357913941  | true  |
        | 357913941  | false |
        | -357913941 | true  |
        | -357913941 | false |

    Scenario Outline: A selected non-NULL date still overflows in a nullable batch with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT add_years(d, <years>) AS result
        FROM VALUES (CAST(NULL AS DATE)), (DATE '2000-01-01') AS t(d)
        """
      Then query error (?i)year-month interval overflow

      Examples:
        | years      | ansi  |
        | 357913941  | true  |
        | 357913941  | false |
        | -357913941 | true  |
        | -357913941 | false |

    Scenario Outline: Evaluated year overflow still errors with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT add_years(DATE '2000-01-01', <years>) AS result
        """
      Then query error (?i)year-month interval overflow

      Examples:
        | years      | ansi  |
        | 357913941  | true  |
        | 357913941  | false |
        | -357913941 | true  |
        | -357913941 | false |
