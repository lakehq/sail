Feature: split_part output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to split_part yields the schema Spark declares
      When query
        """
        SELECT split_part('11.12.13', '.', 3) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null column input to split_part yields the schema Spark declares
      When query
        """
        SELECT split_part(CAST(id AS STRING), '.', 3) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable column input to split_part stays nullable
      When query
        """
        SELECT split_part(c, '.', 3) AS result FROM VALUES ('11.12.13'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
