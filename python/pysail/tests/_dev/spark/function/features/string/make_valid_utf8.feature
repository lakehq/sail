Feature: make_valid_utf8 output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to make_valid_utf8 yields the schema Spark declares
      When query
        """
        SELECT make_valid_utf8('Spark') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null column input to make_valid_utf8 yields the schema Spark declares
      When query
        """
        SELECT make_valid_utf8(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable column input to make_valid_utf8 stays nullable
      When query
        """
        SELECT make_valid_utf8(c) AS result FROM VALUES ('Spark'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
