Feature: sha1 output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to sha1 yields the schema Spark declares
      When query
        """
        SELECT sha1('Spark') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a non-null column input to sha1 yields the schema Spark declares
      When query
        """
        SELECT sha1(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to sha1 stays nullable
      When query
        """
        SELECT sha1(c) AS result FROM VALUES ('Spark'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
