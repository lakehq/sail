Feature: VALUES relation output schema

  Rule: Column nullability

    @sail-bug
    Scenario: non-null VALUES literals produce a non-nullable column
      When query
        """
        SELECT id FROM VALUES (1), (2) AS t(id)
        """
      Then query schema
        """
        root
         |-- id: integer (nullable = false)
        """
