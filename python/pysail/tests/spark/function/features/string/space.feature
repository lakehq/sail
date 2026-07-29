@space
Feature: space output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to space yields the schema Spark declares
      When query
        """
        SELECT concat(space(2), '1') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """
