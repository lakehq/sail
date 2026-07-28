@version
Feature: version output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to version yields the schema Spark declares
      When query
        """
        SELECT version() AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """
