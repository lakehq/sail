Feature: Projected IN local producers and windows

  Scenario Outline: window extraction preserves the local producer evaluation boundary
    # Selecting a constant instead of n makes the window unused.
    When query
      """
      SELECT id, <observed> AS n,
        x IN (SELECT 1) AS present, x NOT IN (SELECT 1) AS absent
      FROM (<producer>) t
      ORDER BY id
      """
    Then query result ordered
      | id | n        | present | absent  |
      | 1  | <first>  | <value> | <value> |
      | 2  | <second> | <value> | <value> |

    Examples:
      | producer | observed | first | second | value |
      | SELECT id, NULLIF(1, 1) AS x, ROW_NUMBER() OVER (ORDER BY id) AS n FROM VALUES (1), (2) u(id) | n | 1 | 2 | false |
      | SELECT id, NULLIF(1, 1) AS x, ROW_NUMBER() OVER (ORDER BY id) AS n FROM VALUES (1), (2) u(id) | 0 | 0 | 0 | false |
      | SELECT id, x, ROW_NUMBER() OVER (ORDER BY id) AS n FROM (SELECT id, NULLIF(1, 1) AS x FROM VALUES (1), (2) u(id)) v | n | 1 | 2 | false |
      | SELECT id, x, ROW_NUMBER() OVER (ORDER BY id) AS n FROM (SELECT id, NULLIF(1, 1) AS x FROM VALUES (1), (2) u(id)) v | 0 | 0 | 0 | false |
      | SELECT id, n, NULLIF(1, 1) AS x FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS n FROM VALUES (1), (2) u(id)) v | n | 1 | 2 | NULL |
      | SELECT id, n, NULLIF(1, 1) AS x FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS n FROM VALUES (1), (2) u(id)) v | 0 | 0 | 0 | NULL |
      | SELECT id, NULLIF(1, 1) AS x, ROW_NUMBER() OVER (ORDER BY id) AS n FROM range(1, 3) | n | 1 | 2 | NULL |
      | SELECT id, NULLIF(1, 1) AS x, ROW_NUMBER() OVER (ORDER BY id) AS n FROM range(1, 3) | 0 | 0 | 0 | NULL |
      | SELECT id, x, ROW_NUMBER() OVER (ORDER BY id) AS n FROM (SELECT id, NULLIF(1, 1) AS x FROM range(1, 3)) v | n | 1 | 2 | NULL |
      | SELECT id, x, ROW_NUMBER() OVER (ORDER BY id) AS n FROM (SELECT id, NULLIF(1, 1) AS x FROM range(1, 3)) v | 0 | 0 | 0 | NULL |
      | SELECT id, n, NULLIF(1, 1) AS x FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS n FROM range(1, 3)) v | n | 1 | 2 | NULL |
      | SELECT id, n, NULLIF(1, 1) AS x FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS n FROM range(1, 3)) v | 0 | 0 | 0 | NULL |

  Scenario Outline: unevaluable regular siblings keep window producers from materializing locally
    When query
      """
      SELECT id, x IN (SELECT 1) AS present
      FROM (
        SELECT id, NULLIF(1, 1) AS x, ROW_NUMBER() OVER (ORDER BY id) AS n,
          <sibling> AS unused
        FROM VALUES (1), (2) u(id)
      ) t
      ORDER BY id
      """
    Then query result ordered
      | id | present |
      | 1  | NULL    |
      | 2  | NULL    |

    Examples:
      | sibling                           |
      | (SELECT MAX(id) FROM range(2))     |
      | id IN (SELECT 1)                  |

  Scenario: unevaluable window arguments keep window producers from materializing locally
    When query
      """
      SELECT id, x IN (SELECT 1) AS present
      FROM (
        SELECT id, NULLIF(1, 1) AS x,
          SUM((SELECT MAX(id) FROM range(2))) OVER (ORDER BY id) AS unused
        FROM VALUES (1), (2) u(id)
      ) t
      ORDER BY id
      """
    Then query result ordered
      | id | present |
      | 1  | NULL    |
      | 2  | NULL    |

  Scenario: null expressions depending on a window result remain foldable
    When query
      """
      SELECT id, x IN (SELECT 1) AS present
      FROM (
        SELECT id, ROW_NUMBER() OVER (ORDER BY id) + CAST(NULL AS INT) AS x
        FROM VALUES (1), (2) u(id)
      ) t
      ORDER BY id
      """
    Then query result ordered
      | id | present |
      | 1  | NULL    |
      | 2  | NULL    |

  Scenario: multiple windows share locally materialized regular producers
    When query
      """
      SELECT id, n1, n2, x IN (SELECT 1) AS present
      FROM (
        SELECT id, NULLIF(1, 1) AS x,
          ROW_NUMBER() OVER (ORDER BY id) AS n1,
          ROW_NUMBER() OVER (ORDER BY id DESC) AS n2
        FROM VALUES (1), (2) u(id)
      ) t
      ORDER BY id
      """
    Then query result ordered
      | id | n1 | n2 | present |
      | 1  | 1  | 2  | false   |
      | 2  | 2  | 1  | false   |

  Scenario: volatile regular siblings do not block local window producers
    When query
      """
      SELECT id, r >= 0 AS generated, x IN (SELECT 1) AS present
      FROM (
        SELECT id, NULLIF(1, 1) AS x, ROW_NUMBER() OVER (ORDER BY id) AS n,
          rand(0) AS r
        FROM VALUES (1), (2) u(id)
      ) t
      ORDER BY id
      """
    Then query result ordered
      | id | generated | present |
      | 1  | true      | false   |
      | 2  | true      | false   |

  Scenario Outline: partition scalar siblings preserve the local producer boundary
    When query
      """
      SELECT id, x IN (SELECT 1) AS present, x NOT IN (SELECT 1) AS absent
      FROM (
        SELECT id, NULLIF(1, 1) AS x, <function>() AS unused <window>
        FROM <source>
      ) t
      ORDER BY id
      """
    Then query result ordered
      | id | present | absent  |
      | 1  | <value> | <value> |
      | 2  | <value> | <value> |

    Examples:
      | function                    | window                                  | source                   | value |
      | spark_partition_id          |                                         | VALUES (1), (2) u(id)     | false |
      | monotonically_increasing_id |                                         | VALUES (1), (2) u(id)     | false |
      | spark_partition_id          | , ROW_NUMBER() OVER (ORDER BY id) AS n   | VALUES (1), (2) u(id)     | false |
      | monotonically_increasing_id | , ROW_NUMBER() OVER (ORDER BY id) AS n   | VALUES (1), (2) u(id)     | false |
      | spark_partition_id          |                                         | range(1, 3)              | NULL  |
      | monotonically_increasing_id |                                         | range(1, 3)              | NULL  |
      | spark_partition_id          | , ROW_NUMBER() OVER (ORDER BY id) AS n   | range(1, 3)              | NULL  |
      | monotonically_increasing_id | , ROW_NUMBER() OVER (ORDER BY id) AS n   | range(1, 3)              | NULL  |
