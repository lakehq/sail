Feature: shiftright output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to shiftright yields the schema Spark declares
      When query
        """
        SELECT shiftright(4, 1) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a non-null column input to shiftright yields the schema Spark declares
      When query
        """
        SELECT shiftright(CAST(id AS INT), 1) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to shiftright stays nullable
      When query
        """
        SELECT shiftright(c, 1) AS result FROM VALUES (4), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
