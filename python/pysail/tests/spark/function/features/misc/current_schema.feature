@current_schema
Feature: current_schema output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to current_schema yields the schema Spark declares
      When query
        """
        SELECT current_schema() AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """
