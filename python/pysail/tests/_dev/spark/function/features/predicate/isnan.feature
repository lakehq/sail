Feature: isnan output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to isnan yields the schema Spark declares
      When query
        """
        SELECT isnan(cast('NaN' as double)) AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to isnan yields the schema Spark declares
      When query
        """
        SELECT isnan(CAST(id AS DOUBLE)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    @sail-bug
    Scenario: a nullable column input to isnan stays nullable
      When query
        """
        SELECT isnan(c) AS result FROM VALUES (cast('NaN' as double)), (CAST(NULL AS DOUBLE)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """
