Feature: mask output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to mask yields the schema Spark declares
      When query
        """
        SELECT mask('abcd-EFGH-8765-4321') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null column input to mask yields the schema Spark declares
      When query
        """
        SELECT mask(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable column input to mask stays nullable
      When query
        """
        SELECT mask(c) AS result FROM VALUES ('abcd-EFGH-8765-4321'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
