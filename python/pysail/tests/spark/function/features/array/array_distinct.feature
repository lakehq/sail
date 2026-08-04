Feature: array_distinct output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to array_distinct yields the schema Spark declares
      When query
        """
        SELECT array_distinct(array(1, 2, 3, null, 3)) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = true)
        """

    Scenario: a nullable column input to array_distinct stays nullable
      When query
        """
        SELECT array_distinct(c) AS result FROM VALUES (array(1, 2, 3, null, 3)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: integer (containsNull = true)
        """
