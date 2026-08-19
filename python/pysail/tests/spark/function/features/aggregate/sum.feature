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

    Scenario: window sum respects partitions and explicit frames
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT group_name, id,
          sum(value) OVER (
            PARTITION BY group_name
            ORDER BY id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ) AS total
        FROM VALUES
          ('a', 1, '1'),
          ('a', 2, '2'),
          ('b', 1, '3')
          AS tab(group_name, id, value)
        ORDER BY group_name, id
        """
      Then query result ordered
        | group_name | id | total |
        | a          | 1  | 1.0   |
        | a          | 2  | 3.0   |
        | b          | 1  | 3.0   |

    @function(nullability)
    Scenario: window sum of strings returns nullable double
      When query
        """
        SELECT sum(value) OVER (
          PARTITION BY group_name
          ORDER BY id
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS total
        FROM VALUES ('a', 1, '1') AS tab(group_name, id, value)
        """
      Then query schema
        """
        root
         |-- total: double (nullable = true)
        """

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

    Scenario: invalid strings become null in a legacy window sum
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT id, sum(value) OVER (
          ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS total
        FROM VALUES (1, '1'), (2, 'invalid'), (3, '2') AS tab(id, value)
        ORDER BY id
        """
      Then query result ordered
        | id | total |
        | 1  | 1.0   |
        | 2  | 1.0   |
        | 3  | 3.0   |

    Scenario: invalid strings fail in an ANSI window sum
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT sum(value) OVER (
          ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS total
        FROM VALUES (1, '1'), (2, 'invalid') AS tab(id, value)
        """
      Then query error .*

  Rule: Filtered strings are cast only after excluded rows are masked

    Scenario: ANSI global sum filters malformed strings before casting
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT sum(value) FILTER (WHERE included) AS total
        FROM VALUES
          ('1', true),
          ('invalid', false),
          ('2', true)
          AS tab(value, included)
        """
      Then query result
        | total |
        | 3.0   |

    Scenario: ANSI grouped distinct sum filters malformed strings before casting
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT group_name, sum(DISTINCT value) FILTER (WHERE included) AS total
        FROM VALUES
          ('a', '1', true),
          ('a', '1.0', true),
          ('a', 'invalid', false),
          ('a', '2', true),
          ('b', 'invalid', false),
          ('b', '4', true)
          AS tab(group_name, value, included)
        GROUP BY group_name
        ORDER BY group_name
        """
      Then query result ordered
        | group_name | total |
        | a          | 3.0   |
        | b          | 4.0   |

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
