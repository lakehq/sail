@map_keys
Feature: map_keys output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to map_keys yields the schema Spark declares
      When query
        """
        SELECT map_keys(map(1, 'a', 2, 'b')) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = true)
        """

    Scenario: a nullable column input to map_keys stays nullable
      When query
        """
        SELECT map_keys(c) AS result FROM VALUES (map(1, 'a', 2, 'b')), (CAST(NULL AS MAP<INT,STRING>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: integer (containsNull = true)
        """
