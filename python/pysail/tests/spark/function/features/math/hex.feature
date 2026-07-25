@hex
Feature: hex output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to hex yields the schema Spark declares
      When query
        """
        SELECT hex(17) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to hex yields the schema Spark declares
      When query
        """
        SELECT hex(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to hex stays nullable
      When query
        """
        SELECT hex(c) AS result FROM VALUES (17), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
