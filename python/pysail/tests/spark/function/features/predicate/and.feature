@and
Feature: and output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to and yields the schema Spark declares
      When query
        """
        SELECT true and true AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """
