Feature: greatest output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to greatest yields the schema Spark declares
      When query
        """
        SELECT greatest(10, 9, 2, 4, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to greatest yields the schema Spark declares
      When query
        """
        SELECT greatest(CAST(id AS INT), 9, 2, 4, 3) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    @sail-bug
    Scenario: a nullable column input to greatest stays nullable
      When query
        """
        SELECT greatest(c, 9, 2, 4, 3) AS result FROM VALUES (10), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """
