@csc
Feature: csc output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to csc yields the schema Spark declares
      When query
        """
        SELECT csc(1) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to csc yields the schema Spark declares
      When query
        """
        SELECT csc(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to csc stays nullable
      When query
        """
        SELECT csc(c) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
