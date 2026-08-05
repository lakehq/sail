Feature: pow output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to pow yields the schema Spark declares
      When query
        """
        SELECT pow(2, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to pow yields the schema Spark declares
      When query
        """
        SELECT pow(CAST(id AS INT), 3) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = false)
        """

    Scenario: a nullable column input to pow stays nullable
      When query
        """
        SELECT pow(c, 3) AS result FROM VALUES (2), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
