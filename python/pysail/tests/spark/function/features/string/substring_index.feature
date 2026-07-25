@substring_index
Feature: substring_index output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to substring_index yields the schema Spark declares
      When query
        """
        SELECT substring_index('www.apache.org', '.', 2) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to substring_index yields the schema Spark declares
      When query
        """
        SELECT substring_index(CAST(id AS STRING), '.', 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to substring_index stays nullable
      When query
        """
        SELECT substring_index(c, '.', 2) AS result FROM VALUES ('www.apache.org'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
