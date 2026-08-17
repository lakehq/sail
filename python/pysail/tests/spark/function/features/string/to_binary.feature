Feature: to_binary output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to to_binary yields the schema Spark declares
      When query
        """
        SELECT to_binary('abc', 'utf-8') AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a non-null column input to to_binary yields the schema Spark declares
      When query
        """
        SELECT to_binary(CAST(id AS STRING), 'utf-8') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a nullable column input to to_binary stays nullable
      When query
        """
        SELECT to_binary(c, 'utf-8') AS result FROM VALUES ('abc'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """
