@array_append
Feature: array_append output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to array_append yields the schema Spark declares
      When query
        """
        SELECT array_append(array('b', 'd', 'c', 'a'), 'd') AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = true)
        """

    Scenario: a nullable column input to array_append stays nullable
      When query
        """
        SELECT array_append(c, 'd') AS result FROM VALUES (array('b', 'd', 'c', 'a')), (CAST(NULL AS ARRAY<STRING>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = true)
        """
