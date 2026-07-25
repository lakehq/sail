@or
Feature: or output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to or yields the schema Spark declares
      When query
        """
        SELECT true or false AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """
