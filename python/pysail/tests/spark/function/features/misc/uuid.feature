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

  Rule: An optional literal seed is accepted

    @sail-bug
    Scenario: uuid accepts an integer seed
      When query
        """
        SELECT length(uuid(1)) AS result
        """
      Then query result
        | result |
        | 36     |
