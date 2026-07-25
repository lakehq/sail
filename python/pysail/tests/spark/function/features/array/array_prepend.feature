@array_prepend
Feature: array_prepend output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to array_prepend yields the schema Spark declares
      When query
        """
        SELECT array_prepend(array('b', 'd', 'c', 'a'), 'd') AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = true)
        """

    Scenario: a nullable column input to array_prepend stays nullable
      When query
        """
        SELECT array_prepend(c, 'd') AS result FROM VALUES (array('b', 'd', 'c', 'a')), (CAST(NULL AS ARRAY<STRING>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = true)
        """
