@unhex
Feature: unhex output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to unhex yields the schema Spark declares
      When query
        """
        SELECT decode(unhex('537061726B2053514C'), 'UTF-8') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
