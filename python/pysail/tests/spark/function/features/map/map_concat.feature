Feature: map_concat output schema

  Rule: Duplicate key policy

    Scenario: duplicate keys raise an error under the default EXCEPTION policy
      When query
        """
        SELECT map_concat(map(1, 'a'), map(1, 'b')) AS result
        """
      Then query error .*\[DUPLICATED_MAP_KEY\].*

    Scenario: LAST_WIN keeps the value from the final map at the first key position
      Given config spark.sql.mapKeyDedupPolicy = LAST_WIN
      When query
        """
        SELECT map_concat(map(1, 'a', 2, 'b'), map(2, 'c', 3, 'd')) AS result
        """
      Then query result
        | result                   |
        | {1 -> a, 2 -> c, 3 -> d} |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to map_concat yields the schema Spark declares
      When query
        """
        SELECT map_concat(map(1, 'a', 2, 'b'), map(3, 'c')) AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: integer
         |    |-- value: string (valueContainsNull = false)
        """

    Scenario: a nullable column input to map_concat stays nullable
      When query
        """
        SELECT map_concat(c, map(3, 'c')) AS result FROM VALUES (map(1, 'a', 2, 'b')), (CAST(NULL AS MAP<INT,STRING>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: map (nullable = true)
         |    |-- key: integer
         |    |-- value: string (valueContainsNull = true)
        """
