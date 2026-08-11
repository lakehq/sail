Feature: sum

  Rule: String inputs are implicitly cast to double

    Scenario: sum groups numeric strings
      When query
        """
        SELECT group_name, sum(value) AS total
        FROM VALUES
          ('a', '1'),
          ('a', '2'),
          ('b', '3')
          AS tab(group_name, value)
        GROUP BY group_name
        ORDER BY group_name
        """
      Then query result ordered
        | group_name | total |
        | a          | 3.0   |
        | b          | 3.0   |

    @function(nullability)
    Scenario: sum of strings returns nullable double
      When query
        """
        SELECT sum(value) AS total
        FROM VALUES ('1'), ('2') AS tab(value)
        """
      Then query schema
        """
        root
         |-- total: double (nullable = true)
        """

    Scenario: sum skips null string values
      When query
        """
        SELECT sum(value) AS total
        FROM VALUES ('1'), (CAST(NULL AS STRING)), ('2') AS tab(value)
        """
      Then query result
        | total |
        | 3.0   |

    Scenario: sum applies distinct after converting strings to numbers
      When query
        """
        SELECT sum(DISTINCT value) AS total
        FROM VALUES ('1'), ('1.0'), ('2') AS tab(value)
        """
      Then query result
        | total |
        | 3.0   |

  Rule: Invalid strings follow ANSI mode

    Scenario: invalid strings become null when ANSI mode is disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT sum(value) AS total
        FROM VALUES ('1'), ('invalid'), ('2') AS tab(value)
        """
      Then query result
        | total |
        | 3.0   |

    Scenario: invalid strings fail when ANSI mode is enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT sum(value) AS total
        FROM VALUES ('1'), ('invalid'), ('2') AS tab(value)
        """
      Then query error .*

  Rule: Numeric inputs retain their Spark sum type

    Scenario: sum of integers remains a long
      When query
        """
        SELECT typeof(sum(value)) AS result_type, sum(value) AS total
        FROM VALUES (1), (2), (3) AS tab(value)
        """
      Then query result
        | result_type | total |
        | bigint      | 6     |
