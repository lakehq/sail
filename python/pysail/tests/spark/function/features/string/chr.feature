@chr
Feature: chr output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to chr yields the schema Spark declares
      When query
        """
        SELECT chr(65) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to chr yields the schema Spark declares
      When query
        """
        SELECT chr(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to chr stays nullable
      When query
        """
        SELECT chr(c) AS result FROM VALUES (65), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
