@map_values
Feature: map_values output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to map_values yields the schema Spark declares
      When query
        """
        SELECT map_values(map(1, 'a', 2, 'b')) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = true)
        """

    Scenario: a nullable column input to map_values stays nullable
      When query
        """
        SELECT map_values(c) AS result FROM VALUES (map(1, 'a', 2, 'b')), (CAST(NULL AS MAP<INT,STRING>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = true)
        """
