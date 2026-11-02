Feature: Set operation multiplicity and type coercion
  Background:
    Given config spark.sql.caseSensitive = false
    Given config spark.sql.ansi.enabled = true
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW t AS
      SELECT * FROM VALUES
        (1L, 'a', 10, 100),
        (2L, 'a', 20, 200),
        (3L, 'b', CAST(NULL AS INT), 300),
        (4L, 'b', 40, CAST(NULL AS INT)),
        (5L, CAST(NULL AS STRING), 50, 500),
        (6L, 'a', 10, 600),
        (7L, 'c', 70, 700)
      AS t(id, g, v, w)
      """
    Given final statement
      """
      DROP VIEW IF EXISTS t
      """
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW u AS SELECT *
      FROM VALUES (1L, 'a', 100), (2L, 'a', 200), (4L, 'b', 400), (8L, 'z', 800), (CAST(NULL AS BIGINT), 'n',
        900) AS u(id, g, z)
      """
    Given final statement
      """
      DROP VIEW IF EXISTS u
      """

  Scenario: INTERSECT ALL retains the minimum multiplicity of each row
    When query
      """
      SELECT g, v
      FROM t
      INTERSECT ALL SELECT g, v
      FROM t
      WHERE id <= 3
      """
    Then query result collected
      | g | v    |
      | a | 10   |
      | a | 20   |
      | b | NULL |
    Then query column types
      | column | type   |
      | g      | string |
      | v      | int    |

  Scenario: UNION ALL widens a small integer column to bigint
    When query
      """
      SELECT CAST(v AS SMALLINT) AS x
      FROM t
      UNION ALL SELECT id AS x
      FROM u
      """
    Then query result collected
      | x    |
      | 10   |
      | 10   |
      | 1    |
      | 20   |
      | 2    |
      | 40   |
      | 4    |
      | 50   |
      | 70   |
      | 8    |
      | NULL |
      | NULL |
    Then query column types
      | column | type   |
      | x      | bigint |

  Scenario: UNION ALL widens nested array elements to bigint
    When query
      """
      SELECT array(v) AS a
      FROM t
      UNION ALL SELECT array(CAST(z AS BIGINT)) AS a
      FROM u
      """
    Then query result collected
      | a      |
      | [100]  |
      | [10]   |
      | [10]   |
      | [200]  |
      | [20]   |
      | [400]  |
      | [40]   |
      | [50]   |
      | [70]   |
      | [800]  |
      | [900]  |
      | [None] |
    Then query column types
      | column | type          |
      | a      | array<bigint> |

  Scenario: INTERSECT ALL emits one row when only one right copy exists
    When query
      """
      SELECT *
      FROM VALUES (1), (1) l(x)
      INTERSECT ALL SELECT *
      FROM VALUES (1) r(x)
      """
    Then query result collected
      | x |
      | 1 |
    Then query column types
      | column | type |
      | x      | int  |

  Scenario: INTERSECT ALL counts null rows as matching values
    When query
      """
      SELECT *
      FROM VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT)) l(x)
      INTERSECT ALL SELECT CAST(NULL AS INT) AS x
      """
    Then query result collected
      | x    |
      | NULL |
    Then query column types
      | column | type |
      | x      | int  |

  Scenario: UNION ALL preserves bigint values outside the small integer range
    When query
      """
      SELECT CAST(1 AS SMALLINT) AS x
      UNION ALL SELECT 10000000000L AS x
      """
    Then query result collected
      | x           |
      | 10000000000 |
      | 1           |
    Then query column types
      | column | type   |
      | x      | bigint |

  Scenario: Arithmetic above UNION ALL uses the widened integer type
    When query
      """
      SELECT x + 1 AS x
      FROM (SELECT CAST(1 AS SMALLINT) AS x UNION ALL SELECT 10000000000L AS x)
      """
    Then query result collected
      | x           |
      | 10000000001 |
      | 2           |
    Then query column types
      | column | type   |
      | x      | bigint |

  Scenario: An empty left input still determines UNION ALL type coercion
    When query
      """
      SELECT CAST(1 AS SMALLINT) AS x
      WHERE false
      UNION ALL SELECT 10000000000L AS x
      """
    Then query result collected
      | x           |
      | 10000000000 |
    Then query column types
      | column | type   |
      | x      | bigint |

  Scenario Outline: Set operations widen their output before matching rows
    When query
      """
      SELECT CAST(1 AS SMALLINT) AS x <operation> SELECT CAST(<right> AS BIGINT) AS x
      """
    Then query result collected
      | x |
      | 1 |
    Then query column types
      | column | type   |
      | x      | bigint |

    Examples:
      | operation     | right |
      | INTERSECT     | 1     |
      | INTERSECT ALL | 1     |
      | EXCEPT        | 2     |
      | EXCEPT ALL    | 2     |

  Scenario Outline: Set operation multiplicity is computed after lossy coercion
    When query
      """
      SELECT x FROM VALUES (9007199254740992L), (9007199254740993L) t(x)
      <operation> SELECT CAST(9007199254740992L AS DOUBLE) AS x
      """
    Then query result collected
      | x                  |
      | 9007199254740992.0 |
    Then query column types
      | column | type   |
      | x      | double |

    Examples:
      | operation     |
      | INTERSECT ALL |
      | EXCEPT ALL    |
      | INTERSECT     |

  Scenario Outline: Set operation multiplicity follows session timestamp coercion
    Given config spark.sql.session.timeZone = <timezone>
    When query
      """
      SELECT CAST(x AS STRING) AS x FROM (
        SELECT x FROM VALUES
          (TIMESTAMP_NTZ '2020-03-08 02:30:00'),
          (TIMESTAMP_NTZ '2020-03-08 03:30:00') t(x)
        <operation> SELECT TIMESTAMP '2020-03-08 03:30:00' AS x
      )
      """
    Then query result collected
      | x                   |
      | <expected>          |

    Examples:
      | timezone            | operation     | expected            |
      | America/Los_Angeles | INTERSECT ALL | 2020-03-08 03:30:00 |
      | America/Los_Angeles | EXCEPT ALL    | 2020-03-08 03:30:00 |
      | America/Los_Angeles | INTERSECT     | 2020-03-08 03:30:00 |
      | Asia/Shanghai       | INTERSECT ALL | 2020-03-08 03:30:00 |
      | Asia/Shanghai       | EXCEPT ALL    | 2020-03-08 02:30:00 |
      | UTC                 | INTERSECT ALL | 2020-03-08 03:30:00 |
      | UTC                 | EXCEPT ALL    | 2020-03-08 02:30:00 |

  Scenario: EXCEPT DISTINCT removes values collapsed by session timestamp coercion
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT COUNT(*) AS n FROM (
        SELECT x FROM VALUES
          (TIMESTAMP_NTZ '2020-03-08 02:30:00'),
          (TIMESTAMP_NTZ '2020-03-08 03:30:00') t(x)
        EXCEPT SELECT TIMESTAMP '2020-03-08 03:30:00' AS x
      )
      """
    Then query result collected
      | n |
      | 0 |
