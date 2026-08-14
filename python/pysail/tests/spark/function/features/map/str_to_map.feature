Feature: str_to_map output schema

  Rule: Duplicate key policy

    Scenario: duplicate keys raise an error under the default EXCEPTION policy
      When query
        """
        SELECT str_to_map('a:1,a:2', ',', ':') AS result
        """
      Then query error .*\[DUPLICATED_MAP_KEY\].*

    Scenario: LAST_WIN keeps the final value
      Given config spark.sql.mapKeyDedupPolicy = LAST_WIN
      When query
        """
        SELECT str_to_map('a:1,a:2', ',', ':') AS result
        """
      Then query result
        | result   |
        | {a -> 2} |

  @function(nullability)
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
