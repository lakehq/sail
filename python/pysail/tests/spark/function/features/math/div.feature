Feature: div output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to div yields the schema Spark declares
      When query
        """
        SELECT 3 div 2 AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """
