Feature: hypot output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to hypot yields the schema Spark declares
      When query
        """
        SELECT hypot(3, 4) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to hypot yields the schema Spark declares
      When query
        """
        SELECT hypot(CAST(id AS INT), 4) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = false)
        """

    Scenario: a nullable column input to hypot stays nullable
      When query
        """
        SELECT hypot(c, 4) AS result FROM VALUES (3), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
