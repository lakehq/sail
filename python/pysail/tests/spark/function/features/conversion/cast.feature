Feature: CAST expressions

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to cast yields the schema Spark declares
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT cast('10' as int) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Legacy STRING to INT casts

    Scenario: decimal strings truncate and overflowing strings return NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT id, CAST(value AS INT) AS result
        FROM VALUES
          (0, '100'),
          (1, '1.23'),
          (2, '-4.56'),
          (3, '2147483647.999'),
          (4, '-2147483648.999'),
          (5, '2178802287'),
          (6, '2147483648'),
          (7, '-2147483649'),
          (8, '2147483648.0'),
          (9, '123.a'),
          (10, CAST(NULL AS STRING))
        AS data(id, value)
        ORDER BY id
        """
      Then query result ordered
        | id | result      |
        | 0  | 100         |
        | 1  | 1           |
        | 2  | -4          |
        | 3  | 2147483647  |
        | 4  | -2147483648 |
        | 5  | NULL        |
        | 6  | NULL        |
        | 7  | NULL        |
        | 8  | NULL        |
        | 9  | NULL        |
        | 10 | NULL        |

    Scenario: overflowing strings do not abort a filter predicate
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT value
        FROM VALUES ('2178802287'), ('100'), ('2147483648') AS data(value)
        WHERE CAST(value AS INT) = 100
        """
      Then query result
        | value |
        | 100   |

  Rule: ANSI and TRY casts stay strict

    Scenario Outline: ANSI CAST rejects <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(<input> AS INT) AS result
        """
      Then query error <error>

      Examples:
        | case                    | input        | error      |
        | a decimal string        | '1.23'       | 1.23       |
        | an overflowing integer  | '2147483648' | 2147483648 |

    Scenario: TRY_CAST returns NULL for decimal and overflowing strings
      When query
        """
        SELECT id, TRY_CAST(value AS INT) AS result
        FROM VALUES
          (0, '100'),
          (1, '1.23'),
          (2, '2147483648')
        AS data(id, value)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 0  | 100    |
        | 1  | NULL   |
        | 2  | NULL   |
