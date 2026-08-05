Feature: quarter output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to quarter yields the schema Spark declares
      When query
        """
        SELECT quarter('2016-08-31') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to quarter yields the schema Spark declares
      When query
        """
        SELECT quarter(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to quarter stays nullable
      When query
        """
        SELECT quarter(c) AS result FROM VALUES ('2016-08-31'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
