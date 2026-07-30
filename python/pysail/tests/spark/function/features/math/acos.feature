@acos
Feature: acos output schema

  @spark_null
  Rule: Output schema

    Scenario Outline: acos output schema matches Spark (nullable double)
      When query
        """
        <query>
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

      Examples:
        | query                                                                 |
        | SELECT acos(1) AS result                                              |
        | SELECT acos(CAST(id AS INT)) AS result FROM range(3)                  |
        | SELECT acos(c) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c) |
