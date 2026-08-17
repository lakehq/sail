Feature: array_union output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to array_union yields the schema Spark declares
      When query
        """
        SELECT array_union(array(1, 2, 3), array(1, 3, 5)) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    Scenario: a nullable column input to array_union stays nullable
      When query
        """
        SELECT array_union(c, array(1, 3, 5)) AS result FROM VALUES (array(1, 2, 3)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: integer (containsNull = true)
        """
