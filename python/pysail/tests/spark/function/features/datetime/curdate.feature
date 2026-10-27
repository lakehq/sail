Feature: curdate output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to curdate yields the schema Spark declares
      When query
        """
        SELECT curdate() AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """
