Feature: shiftrightunsigned output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to shiftrightunsigned yields the schema Spark declares
      When query
        """
        SELECT shiftrightunsigned(4, 1) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to shiftrightunsigned yields the schema Spark declares
      When query
        """
        SELECT shiftrightunsigned(CAST(id AS INT), 1) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to shiftrightunsigned stays nullable
      When query
        """
        SELECT shiftrightunsigned(c, 1) AS result FROM VALUES (4), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
