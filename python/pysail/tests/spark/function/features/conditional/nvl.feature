@nvl
Feature: nvl output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to nvl yields the schema Spark declares
      When query
        """
        SELECT nvl(NULL, array('2')) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = false)
        """
