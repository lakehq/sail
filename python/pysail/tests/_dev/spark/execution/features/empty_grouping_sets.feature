Feature: Empty grouping sets in distributed execution
  # DataFusion grouping.slt: duplicate empty grouping sets each emit a grand total.
  Scenario Outline: Duplicate empty groups survive aggregation on workers
    When query
      """
      SELECT sum(z) AS n FROM (
        SELECT <value> AS z FROM VALUES ('0'), ('0'), ('1'), (NULL) AS t(k)
        WHERE <predicate>
        GROUP BY GROUPING SETS ((), ())
      ) groups
      """
    Then query result collected
      | n        |
      | <result> |

    Examples:
      | predicate              | value                        | result |
      | k = '0'                | grouping_id() + 1            | 2      |
      | k = '2'                | grouping_id() + 1            | 2      |
      | k = CAST(NULL AS STRING)| grouping_id()                | 0      |
      | k = '0'                | count(*) + grouping_id() + 1  | 6      |
      | k = '2'                | count(*) + grouping_id() + 1  | 2      |
      | k = CAST(NULL AS STRING)| count(*) + grouping_id() + 1  | 2      |

  Scenario Outline: Duplicate empty groups preserve extrema over a worker scan
    Given variable location for temporary directory empty_group_extrema
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ location.sql }} USING parquet
      SELECT * FROM VALUES (-2L), (3L), (9L), (CAST(NULL AS BIGINT)) AS t(v)
      """
    When query template
      """
      SELECT min(v) AS lo, max(v) AS hi FROM parquet.`{{ location.string }}`
      WHERE <predicate>
      GROUP BY GROUPING SETS ((), ())
      """
    Then query result collected
      | lo   | hi   |
      | <lo> | <hi> |
      | <lo> | <hi> |

    Examples:
      | predicate     | lo   | hi   |
      | v IS NOT NULL | -2   | 9    |
      | v IS NULL     | NULL | NULL |
      | v > 100       | NULL | NULL |
