Feature: extract output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to extract yields the schema Spark declares
      When query
        """
        SELECT extract(YEAR FROM TIMESTAMP '2019-08-12 01:00:00.123456') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """
