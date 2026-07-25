@minute
Feature: minute output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to minute yields the schema Spark declares
      When query
        """
        SELECT minute('2009-07-30 12:58:59') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to minute yields the schema Spark declares
      When query
        """
        SELECT minute(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to minute stays nullable
      When query
        """
        SELECT minute(c) AS result FROM VALUES ('2009-07-30 12:58:59'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
