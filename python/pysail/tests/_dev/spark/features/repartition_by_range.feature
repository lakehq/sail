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
