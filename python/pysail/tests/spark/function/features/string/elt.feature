@elt
Feature: elt output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to elt yields the schema Spark declares
      When query
        """
        SELECT elt(1, 'scala', 'java') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null column input to elt yields the schema Spark declares
      When query
        """
        SELECT elt(CAST(id AS INT), 'scala', 'java') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable column input to elt stays nullable
      When query
        """
        SELECT elt(c, 'scala', 'java') AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
