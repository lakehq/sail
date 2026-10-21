Feature: make_date output schema

  Scenario: direct NULL components suppress other component expressions
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT make_date(NULL, CAST(1 / id AS INT), 1) AS null_year,
             make_date(CAST(1 / id AS INT), 1, NULL) AS null_day
      FROM range(1)
      """
    # Spark's NULL propagation removes these calls before evaluating their arguments.
    # Sail preserves ordinary argument evaluation, including division errors in ANSI mode.
    Then query result for Spark
      | null_year | null_day |
      | NULL      | NULL     |
    Then query error for Sail (?i)divi(?:de|sion).*zero

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to make_date yields the schema Spark declares
      When query
        """
        SELECT make_date(2013, 7, 15) AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """

    Scenario: a non-null column input to make_date yields the schema Spark declares
      When query
        """
        SELECT make_date(CAST(id AS INT), 7, 15) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """

    Scenario: a nullable column input to make_date stays nullable
      When query
        """
        SELECT make_date(c, 7, 15) AS result FROM VALUES (2013), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

    Scenario: string component coercion stays nullable under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_date('2024', 1, 1) AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

  Rule: Invalid dates in legacy mode

    Scenario: make_date returns NULL for an invalid date when ANSI is disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_date(2024, 2, 30) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: invalid date components become NULL independently for each row
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT i, make_date(y, m, d) AS result
        FROM VALUES
          (1, 2024, 2, 29),
          (2, 2024, 2, 30),
          (3, 2024, -1, 1),
          (4, 2024, 1, -1),
          (5, CAST(NULL AS INT), 13, 1)
        AS t(i, y, m, d)
        ORDER BY i
        """
      Then query result ordered
        | i | result     |
        | 1 | 2024-02-29 |
        | 2 | NULL       |
        | 3 | NULL       |
        | 4 | NULL       |
        | 5 | NULL       |

    @function(nullability)
    Scenario: legacy make_date stays nullable for non-null literal and column inputs
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_date(2024, 1, 1) AS literal_date,
               make_date(CAST(id AS INT), 1, 1) AS column_date
        FROM range(3)
        """
      Then query schema
        """
        root
         |-- literal_date: date (nullable = true)
         |-- column_date: date (nullable = true)
        """

    Scenario: malformed component strings become NULL in legacy mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT i, make_date(y, 1, 1) AS result
        FROM VALUES (1, '2024'), (2, 'bad'), (3, CAST(NULL AS STRING)) AS t(i, y)
        ORDER BY i
        """
      Then query result ordered
        | i | result     |
        | 1 | 2024-01-01 |
        | 2 | NULL       |
        | 3 | NULL       |

  Rule: Valid dates and ANSI errors

    Scenario: make_date preserves year zero, negative years, and expanded years
      When query
        """
        SELECT CAST(make_date(y, 1, 1) AS STRING) AS result
        FROM VALUES (-1), (0), (10000) AS t(y)
        ORDER BY y
        """
      Then query result ordered
        | result       |
        | -0001-01-01  |
        | 0000-01-01   |
        | +10000-01-01 |

    Scenario Outline: invalid date components still raise under ANSI: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_date(<args>)
        """
      Then query error (?i)(invalid|out.of.range|unable to parse date)

      Examples:
        | case           | args        |
        | invalid day    | 2024, 2, 30 |
        | negative month | 2024, -1, 1 |

    Scenario: malformed component strings still raise under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_date('bad', 1, 1)
        """
      Then query error (?i)(CAST_INVALID_INPUT|cast.*bad|bad.*cast|parse.*bad)
