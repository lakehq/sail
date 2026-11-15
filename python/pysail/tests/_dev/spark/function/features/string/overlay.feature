Feature: overlay output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to overlay yields the schema Spark declares
      When query
        """
        SELECT overlay('Spark SQL' PLACING '_' FROM 6) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """
