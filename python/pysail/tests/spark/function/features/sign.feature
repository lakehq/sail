@sign
Feature: sign output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to sign yields the schema Spark declares
      When query
        """
        SELECT sign(40) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to sign yields the schema Spark declares
      When query
        """
        SELECT sign(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to sign stays nullable
      When query
        """
        SELECT sign(c) AS result FROM VALUES (40), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
