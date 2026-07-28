@lcase
Feature: lcase output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to lcase yields the schema Spark declares
      When query
        """
        SELECT lcase('SparkSql') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to lcase yields the schema Spark declares
      When query
        """
        SELECT lcase(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to lcase stays nullable
      When query
        """
        SELECT lcase(c) AS result FROM VALUES ('SparkSql'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
