@mod
Feature: mod output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to mod yields the schema Spark declares
      When query
        """
        SELECT 2 % 1.8 AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(2,1) (nullable = true)
        """
