@dayofmonth
Feature: dayofmonth output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to dayofmonth yields the schema Spark declares
      When query
        """
        SELECT dayofmonth('2009-07-30') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to dayofmonth yields the schema Spark declares
      When query
        """
        SELECT dayofmonth(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to dayofmonth stays nullable
      When query
        """
        SELECT dayofmonth(c) AS result FROM VALUES ('2009-07-30'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
