@current_database
Feature: current_database output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to current_database yields the schema Spark declares
      When query
        """
        SELECT current_database() AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """
