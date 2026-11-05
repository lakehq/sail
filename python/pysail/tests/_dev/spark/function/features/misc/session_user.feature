Feature: session_user output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to session_user yields the schema Spark declares
      When query
        """
        SELECT session_user() AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """
