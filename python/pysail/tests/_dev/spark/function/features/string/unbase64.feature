Feature: unbase64 output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to unbase64 yields the schema Spark declares
      When query
        """
        SELECT unbase64('U3BhcmsgU1FM') AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to unbase64 yields the schema Spark declares
      When query
        """
        SELECT unbase64(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = false)
        """

    Scenario: a nullable column input to unbase64 stays nullable
      When query
        """
        SELECT unbase64(c) AS result FROM VALUES ('U3BhcmsgU1FM'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """
