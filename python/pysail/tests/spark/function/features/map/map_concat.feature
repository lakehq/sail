@map_concat
Feature: map_concat output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to map_concat yields the schema Spark declares
      When query
        """
        SELECT map_concat(map(1, 'a', 2, 'b'), map(3, 'c')) AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: integer
         |    |-- value: string (valueContainsNull = false)
        """

    Scenario: a nullable column input to map_concat stays nullable
      When query
        """
        SELECT map_concat(c, map(3, 'c')) AS result FROM VALUES (map(1, 'a', 2, 'b')), (CAST(NULL AS MAP<INT,STRING>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: map (nullable = true)
         |    |-- key: integer
         |    |-- value: string (valueContainsNull = true)
        """
