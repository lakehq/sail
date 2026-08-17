Feature: least output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to least yields the schema Spark declares
      When query
        """
        SELECT least(10, 9, 2, 4, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to least yields the schema Spark declares
      When query
        """
        SELECT least(CAST(id AS INT), 9, 2, 4, 3) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    @sail-bug
    Scenario: a nullable column input to least stays nullable
      When query
        """
        SELECT least(c, 9, 2, 4, 3) AS result FROM VALUES (10), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """
