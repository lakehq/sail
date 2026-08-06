Feature: array_min output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to array_min yields the schema Spark declares
      When query
        """
        SELECT array_min(array(1, 20, null, 3)) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to array_min stays nullable
      When query
        """
        SELECT array_min(c) AS result FROM VALUES (array(1, 20, null, 3)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
