@isnull
Feature: isnull output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to isnull yields the schema Spark declares
      When query
        """
        SELECT isnull(1) AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a non-null column input to isnull yields the schema Spark declares
      When query
        """
        SELECT isnull(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a nullable column input to isnull stays nullable
      When query
        """
        SELECT isnull(c) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """
