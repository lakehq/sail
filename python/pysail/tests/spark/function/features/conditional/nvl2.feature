Feature: nvl2 output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to nvl2 yields the schema Spark declares
      When query
        """
        SELECT nvl2(NULL, 2, 1) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: nvl2 preserves the declared nullability of its result branches
      When query
        """
        SELECT nvl2(x, x, 0) AS result
        FROM VALUES (1), (CAST(NULL AS INT)) AS t(x)
        ORDER BY result
        """
      Then query result
        | result |
        | 0      |
        | 1      |
      And query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Result type

    Scenario: nvl2 is typed by its result arguments when the tested argument is a widened CASE
      When query
        """
        SELECT
          id,
          nvl2(CASE WHEN id = 0 THEN 1 WHEN id = 1 THEN CAST(2 AS BIGINT) END, 1, 0) AS result,
          typeof(nvl2(CASE WHEN id = 0 THEN 1 WHEN id = 1 THEN CAST(2 AS BIGINT) END, 1, 0)) AS result_type
        FROM VALUES (0), (1), (2) AS t(id)
        """
      Then query result
        | id | result | result_type |
        | 0  | 1      | int         |
        | 1  | 1      | int         |
        | 2  | 0      | int         |
