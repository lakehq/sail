Feature: date_from_unix_date output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to date_from_unix_date yields the schema Spark declares
      When query
        """
        SELECT date_from_unix_date(1) AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """

    Scenario: a non-null column input to date_from_unix_date yields the schema Spark declares
      When query
        """
        SELECT date_from_unix_date(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """

    Scenario: a nullable column input to date_from_unix_date stays nullable
      When query
        """
        SELECT date_from_unix_date(c) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """
