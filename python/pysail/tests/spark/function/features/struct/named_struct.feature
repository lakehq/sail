Feature: named_struct output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to named_struct yields the schema Spark declares
      When query
        """
        SELECT named_struct("a", 1, "b", 2, "c", 3) AS result
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = false)
         |    |-- a: integer (nullable = false)
         |    |-- b: integer (nullable = false)
         |    |-- c: integer (nullable = false)
        """
