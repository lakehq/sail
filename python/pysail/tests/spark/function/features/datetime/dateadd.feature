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
