Feature: shiftleft output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to shiftleft yields the schema Spark declares
      When query
        """
        SELECT shiftleft(2, 1) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a non-null column input to shiftleft yields the schema Spark declares
      When query
        """
        SELECT shiftleft(CAST(id AS INT), 1) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to shiftleft stays nullable
      When query
        """
        SELECT shiftleft(c, 1) AS result FROM VALUES (2), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
