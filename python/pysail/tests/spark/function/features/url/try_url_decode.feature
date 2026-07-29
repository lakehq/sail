@try_url_decode
Feature: try_url_decode output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to try_url_decode yields the schema Spark declares
      When query
        """
        SELECT try_url_decode('https%3A%2F%2Fspark.apache.org') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null column input to try_url_decode yields the schema Spark declares
      When query
        """
        SELECT try_url_decode(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable column input to try_url_decode stays nullable
      When query
        """
        SELECT try_url_decode(c) AS result FROM VALUES ('https%3A%2F%2Fspark.apache.org'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
