Feature: now output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to now yields the schema Spark declares
      When query
        """
        SELECT now() AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """
