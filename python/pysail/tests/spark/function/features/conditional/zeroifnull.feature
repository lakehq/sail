@zeroifnull
Feature: zeroifnull output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to zeroifnull yields the schema Spark declares
      When query
        """
        SELECT zeroifnull(NULL) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """
