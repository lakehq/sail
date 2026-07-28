@quote
Feature: quote output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to quote yields the schema Spark declares
      When query
        """
        SELECT quote('Don\'t') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null column input to quote yields the schema Spark declares
      When query
        """
        SELECT quote(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable column input to quote stays nullable
      When query
        """
        SELECT quote(c) AS result FROM VALUES ('Don\'t'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
