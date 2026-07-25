@current_date
Feature: current_date output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to current_date yields the schema Spark declares
      When query
        """
        SELECT current_date() AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """
