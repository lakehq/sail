Feature: spark_partition_id

  Scenario: HAVING aggregates reuse the materialized partition grouping key
    When query
      """
      SELECT spark_partition_id() AS p, count(*) AS n
      FROM range(0, 40, 1, 4)
      GROUP BY spark_partition_id()
      HAVING max(spark_partition_id()) >= 2
      """
    Then query result
      | p | n  |
      | 2 | 10 |
      | 3 | 10 |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to spark_partition_id yields the schema Spark declares
      When query
        """
        SELECT spark_partition_id() AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """
