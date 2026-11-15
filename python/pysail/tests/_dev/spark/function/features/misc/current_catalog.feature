Feature: current_catalog output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to current_catalog yields the schema Spark declares
      When query
        """
        SELECT current_catalog() AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """
