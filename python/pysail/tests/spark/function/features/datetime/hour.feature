Feature: hour output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to hour yields the schema Spark declares
      When query
        """
        SELECT hour('2018-02-14 12:58:59') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to hour yields the schema Spark declares
      When query
        """
        SELECT hour(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to hour stays nullable
      When query
        """
        SELECT hour(c) AS result FROM VALUES ('2018-02-14 12:58:59'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
