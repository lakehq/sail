Feature: atan2 output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to atan2 yields the schema Spark declares
      When query
        """
        SELECT atan2(0, 0) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to atan2 yields the schema Spark declares
      When query
        """
        SELECT atan2(CAST(id AS INT), 0) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = false)
        """

    Scenario: a nullable column input to atan2 stays nullable
      When query
        """
        SELECT atan2(c, 0) AS result FROM VALUES (0), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
