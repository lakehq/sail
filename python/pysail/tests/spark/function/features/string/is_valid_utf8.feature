Feature: is_valid_utf8 output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to is_valid_utf8 yields the schema Spark declares
      When query
        """
        SELECT is_valid_utf8('Spark') AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to is_valid_utf8 yields the schema Spark declares
      When query
        """
        SELECT is_valid_utf8(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """

    @sail-bug
    Scenario: a nullable column input to is_valid_utf8 stays nullable
      When query
        """
        SELECT is_valid_utf8(c) AS result FROM VALUES ('Spark'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """
