@predicate_in
Feature: in output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to in yields the schema Spark declares
      When query
        """
        SELECT 1 in(1, 2, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """
