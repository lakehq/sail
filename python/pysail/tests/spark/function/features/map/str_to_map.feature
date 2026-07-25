@str_to_map
Feature: str_to_map output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to str_to_map yields the schema Spark declares
      When query
        """
        SELECT str_to_map('a:1,b:2,c:3', ',', ':') AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: string
         |    |-- value: string (valueContainsNull = true)
        """

    @sail-bug
    Scenario: a non-null column input to str_to_map yields the schema Spark declares
      When query
        """
        SELECT str_to_map(CAST(id AS STRING), ',', ':') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: string
         |    |-- value: string (valueContainsNull = true)
        """

    Scenario: a nullable column input to str_to_map stays nullable
      When query
        """
        SELECT str_to_map(c, ',', ':') AS result FROM VALUES ('a:1,b:2,c:3'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: map (nullable = true)
         |    |-- key: string
         |    |-- value: string (valueContainsNull = true)
        """
