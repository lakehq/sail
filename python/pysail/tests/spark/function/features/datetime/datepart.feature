Feature: datepart output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to datepart yields the schema Spark declares
      When query
        """
        SELECT datepart('YEAR', TIMESTAMP '2019-08-12 01:00:00.123456') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """
