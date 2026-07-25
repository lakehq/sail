@current_timezone
Feature: current_timezone output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to current_timezone yields the schema Spark declares
      When query
        """
        SELECT current_timezone() AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """
