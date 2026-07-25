@user
Feature: user output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to user yields the schema Spark declares
      When query
        """
        SELECT user() AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """
