@session_user
Feature: session_user output schema

  @spark_null
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
