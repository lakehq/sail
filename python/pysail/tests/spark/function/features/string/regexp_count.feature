Feature: regexp_count output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to regexp_count yields the schema Spark declares
      When query
        """
        SELECT regexp_count('Steven Jones and Stephen Smith are the best players', 'Ste(v|ph)en') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to regexp_count yields the schema Spark declares
      When query
        """
        SELECT regexp_count(CAST(id AS STRING), 'Ste(v|ph)en') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to regexp_count stays nullable
      When query
        """
        SELECT regexp_count(c, 'Ste(v|ph)en') AS result FROM VALUES ('Steven Jones and Stephen Smith are the best players'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Return type

    Scenario: the count is returned as INT, matching Spark
      When query
        """
        SELECT regexp_count('abcabc', 'a') AS result
        """
      Then query result
        | result |
        | 2      |
