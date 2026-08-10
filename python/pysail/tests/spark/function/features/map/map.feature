Feature: map output schema

  Rule: Duplicate key policy

    Scenario: duplicate keys raise an error under the default EXCEPTION policy
      When query
        """
        SELECT map(1, 'a', 1, 'b') AS result
        """
      Then query error .*\[DUPLICATED_MAP_KEY\].*

    Scenario: LAST_WIN keeps the final value at the first key position
      Given config spark.sql.mapKeyDedupPolicy = LAST_WIN
      When query
        """
        SELECT map(1, 'a', 2, 'b', 1, 'c') AS result
        """
      Then query result
        | result           |
        | {1 -> c, 2 -> b} |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to map yields the schema Spark declares
      When query
        """
        SELECT map(1.0, '2', 3.0, '4') AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: decimal(2,1)
         |    |-- value: string (valueContainsNull = false)
        """

    @sail-bug
    Scenario: a nullable column input to map stays nullable
      When query
        """
        SELECT map(c, '2', 3.0, '4') AS result FROM VALUES (1.0), (CAST(NULL AS DECIMAL(2,1))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: decimal(2,1)
         |    |-- value: string (valueContainsNull = false)
        """
