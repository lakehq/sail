Feature: sequence output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to sequence yields the schema Spark declares
      When query
        """
        SELECT sequence(1, 5) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    @sail-bug
    Scenario: a non-null column input to sequence yields the schema Spark declares
      When query
        """
        SELECT sequence(CAST(id AS INT), 5) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    @sail-bug
    Scenario: a nullable column input to sequence stays nullable
      When query
        """
        SELECT sequence(c, 5) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: integer (containsNull = false)
        """

  Rule: Integral type coercion

    Scenario: sequence widens a literal start to a BIGINT column stop
      When query
        """
        SELECT
          n,
          typeof(sequence(1, n)) AS result_type,
          sequence(1, n) AS result
        FROM VALUES (CAST(1 AS BIGINT)), (3), (12) AS t(n)
        ORDER BY n
        """
      Then query result ordered
        | n  | result_type   | result                                  |
        | 1  | array<bigint> | [1]                                     |
        | 3  | array<bigint> | [1, 2, 3]                               |
        | 12 | array<bigint> | [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] |
