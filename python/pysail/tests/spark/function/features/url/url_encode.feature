@url_encode
Feature: url_encode output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to url_encode yields the schema Spark declares
      When query
        """
        SELECT url_encode('https://spark.apache.org') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null column input to url_encode yields the schema Spark declares
      When query
        """
        SELECT url_encode(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable column input to url_encode stays nullable
      When query
        """
        SELECT url_encode(c) AS result FROM VALUES ('https://spark.apache.org'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
