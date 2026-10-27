Feature: ascii output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to ascii yields the schema Spark declares
      When query
        """
        SELECT ascii('222') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to ascii yields the schema Spark declares
      When query
        """
        SELECT ascii(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to ascii stays nullable
      When query
        """
        SELECT ascii(c) AS result FROM VALUES ('222'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
