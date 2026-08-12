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
