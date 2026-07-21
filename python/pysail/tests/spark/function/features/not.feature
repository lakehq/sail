@not
Feature: not output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to not yields the schema Spark declares
      When query
        """
        SELECT not true AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """
