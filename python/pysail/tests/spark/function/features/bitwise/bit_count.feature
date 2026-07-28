@bit_count
Feature: bit_count output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to bit_count yields the schema Spark declares
      When query
        """
        SELECT bit_count(0) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a non-null column input to bit_count yields the schema Spark declares
      When query
        """
        SELECT bit_count(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to bit_count stays nullable
      When query
        """
        SELECT bit_count(c) AS result FROM VALUES (0), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
