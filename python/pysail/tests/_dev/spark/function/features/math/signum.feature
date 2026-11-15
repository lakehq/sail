Feature: signum output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to signum yields the schema Spark declares
      When query
        """
        SELECT signum(40) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to signum yields the schema Spark declares
      When query
        """
        SELECT signum(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to signum stays nullable
      When query
        """
        SELECT signum(c) AS result FROM VALUES (40), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
