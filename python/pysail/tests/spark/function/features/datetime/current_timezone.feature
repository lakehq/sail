Feature: current_timezone output schema

  @function(nullability)
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
