Feature: regexp_substr output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to regexp_substr yields the schema Spark declares
      When query
        """
        SELECT regexp_substr('Steven Jones and Stephen Smith are the best players', 'Ste(v|ph)en') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null column input to regexp_substr yields the schema Spark declares
      When query
        """
        SELECT regexp_substr(CAST(id AS STRING), 'Ste(v|ph)en') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable column input to regexp_substr stays nullable
      When query
        """
        SELECT regexp_substr(c, 'Ste(v|ph)en') AS result FROM VALUES ('Steven Jones and Stephen Smith are the best players'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
