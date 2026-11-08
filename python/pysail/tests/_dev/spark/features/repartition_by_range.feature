@spark-4
Feature: REPARTITION_BY_RANGE hint

  Scenario: The range hint keeps rows with NULL keys
    When query
      """
      SELECT /*+ REPARTITION_BY_RANGE(2, v) */ v
      FROM VALUES (1), (CAST(NULL AS INT)), (3), (CAST(NULL AS INT)) AS t(v)
      """
    Then query result collected
      | v    |
      | 1    |
      | 3    |
      | NULL |
      | NULL |

  Scenario: The range hint name is case-insensitive
    When query
      """
      SELECT /*+ repartition_by_range(3, id) */ id FROM range(3)
      """
    Then query result collected
      | id |
      | 0  |
      | 1  |
      | 2  |

  Scenario: The range hint accepts a repeated column
    When query
      """
      SELECT /*+ REPARTITION_BY_RANGE(3, id, id) */ id FROM range(3)
      """
    Then query result collected
      | id |
      | 0  |
      | 1  |
      | 2  |

  Scenario: The range hint rejects zero partitions
    When query
      """
      SELECT /*+ REPARTITION_BY_RANGE(0, id) */ id FROM range(3)
      """
    Then query error (?i)(must be positive|at least one partition)

  Scenario: The range hint rejects an unknown column
    When query
      """
      SELECT /*+ REPARTITION_BY_RANGE(3, nope) */ id FROM range(3)
      """
    Then query error (?i)(unresolved_column|cannot resolve)

  Scenario: The range hint puts each key range in its own partition
    When query
      """
      SELECT pid, min(key) AS low, max(key) AS high, count(*) AS n
      FROM (
        SELECT key, spark_partition_id() AS pid
        FROM (SELECT /*+ REPARTITION_BY_RANGE(3, key) */ id % 12 AS key FROM range(0, 36, 1, 4))
      )
      GROUP BY pid
      ORDER BY pid
      """
    Then query result ordered
      | pid | low | high | n  |
      | 0   | 0   | 3    | 12 |
      | 1   | 4   | 7    | 12 |
      | 2   | 8   | 11   | 12 |

  Scenario: The range hint keeps each key in one partition when keys are fewer than partitions
    When query
      """
      SELECT pid, key, count(*) AS n
      FROM (
        SELECT key, spark_partition_id() AS pid
        FROM (SELECT /*+ REPARTITION_BY_RANGE(10, key) */ id % 3 AS key FROM range(0, 12, 1, 2))
      )
      GROUP BY pid, key
      ORDER BY pid
      """
    Then query result ordered
      | pid | key | n |
      | 0   | 0   | 4 |
      | 1   | 1   | 4 |
      | 2   | 2   | 4 |

  Scenario: The range hint puts all NULL keys in the first partition
    When query
      """
      SELECT pid, count(*) AS n
      FROM (
        SELECT key, spark_partition_id() AS pid
        FROM (SELECT /*+ REPARTITION_BY_RANGE(4, key) */ CAST(NULL AS INT) AS key FROM range(0, 12, 1, 2))
      )
      GROUP BY pid
      """
    Then query result collected
      | pid | n  |
      | 0   | 12 |

  Scenario: The range hint treats negative and positive zero as one key
    When query
      """
      SELECT pid, count(*) AS n
      FROM (
        SELECT v, spark_partition_id() AS pid
        FROM (
          SELECT /*+ REPARTITION_BY_RANGE(4, v) */ v
          FROM VALUES (-0.0D), (0.0D), (0.0D), (-0.0D), (1.0D), (1.0D), (2.0D), (2.0D) AS t(v)
        )
      )
      GROUP BY pid
      ORDER BY pid
      """
    Then query result ordered
      | pid | n |
      | 0   | 4 |
      | 1   | 2 |
      | 2   | 2 |

  Scenario: The range hint returns no rows for empty input
    When query
      """
      SELECT /*+ REPARTITION_BY_RANGE(3, id) */ id FROM range(0)
      """
    Then query result collected
      | id |
