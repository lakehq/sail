Feature: uuid output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to uuid yields the schema Spark declares
      When query
        """
        SELECT uuid() AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """
