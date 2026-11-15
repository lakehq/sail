Feature: make_date output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to make_date yields the schema Spark declares
      When query
        """
        SELECT make_date(2013, 7, 15) AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to make_date yields the schema Spark declares
      When query
        """
        SELECT make_date(CAST(id AS INT), 7, 15) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """

    Scenario: a nullable column input to make_date stays nullable
      When query
        """
        SELECT make_date(c, 7, 15) AS result FROM VALUES (2013), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """
