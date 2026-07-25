@to_json
Feature: to_json output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to to_json yields the schema Spark declares
      When query
        """
        SELECT to_json(named_struct('a', 1, 'b', 2)) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
