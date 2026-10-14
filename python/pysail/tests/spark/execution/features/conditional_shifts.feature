Feature: Numeric conditional shifts in distributed execution

  Scenario: Unsigned shifts preserve numeric conditional values across workers
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id,
        shiftrightunsigned(IF(id = 0, 8, CAST(-2.5 AS FLOAT)), 1) AS fractional,
        shiftrightunsigned(IF(id = 0, 8, CAST(3000000000.9 AS DECIMAL(12,1))), 1) AS decimal_value,
        shiftrightunsigned(CAST(-8 AS INT), IF(id = 0, 0, 32L)) AS masked,
        shiftrightunsigned(IF(id = 0, CAST(NULL AS INT), 8), IF(id = 0, 2147483648L, 1L)) AS nullable_value,
        shiftrightunsigned(CAST(-9223372036854775808 AS BIGINT), 1) AS minimum
      FROM range(0, 2, 1, 2) ORDER BY id
      """
    Then query result ordered
      | id | fractional | decimal_value | masked | nullable_value | minimum             |
      | 0  | 4          | 4             | -8     | NULL           | 4611686018427387904 |
      | 1  | 2147483647 | 1500000000    | -8     | 4              | 4611686018427387904 |

  Scenario Outline: BIGINT shift counts preserve INT results and NULLs across workers with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id,
        shiftleft(CAST(8 AS INT), id - 1) AS l,
        shiftright(CAST(-8 AS INT), id - 1) AS r,
        shiftrightunsigned(CAST(-8 AS INT), id - 1) AS u,
        shiftright(IF(id = 0, CAST(NULL AS INT), -8), id - 1) AS nullable_value,
        shiftright(-8, IF(id = 0, CAST(NULL AS BIGINT), id - 1)) AS nullable_count
      FROM range(0, 3, 1, 2) ORDER BY id
      """
    Then query result ordered
      | id | l  | r  | u          | nullable_value | nullable_count |
      | 0  | 0  | -1 | 1          | NULL           | NULL           |
      | 1  | 8  | -8 | -8         | -8             | -8             |
      | 2  | 16 | -4 | 2147483644 | -4             | -4             |
    And query schema
      """
      root
       |-- id: long (nullable = false)
       |-- l: integer (nullable = false)
       |-- r: integer (nullable = false)
       |-- u: integer (nullable = false)
       |-- nullable_value: integer (nullable = true)
       |-- nullable_count: integer (nullable = true)
      """

    Examples:
      | ansi  |
      | true  |
      | false |

  Scenario: Non-ANSI BIGINT shift counts retain low bits for arrays and folded literals across workers
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, shiftleft(1, n) AS l, shiftright(-8, n) AS r,
        shiftrightunsigned(-8, n) AS u, shiftrightunsigned(CAST(-8 AS BIGINT), n) AS long_u,
        shiftleft(1, 4294967297L) AS literal_count
      FROM (
        SELECT id, CASE WHEN id = 0 THEN 4294967297L
                        WHEN id = 1 THEN 4294967295L
                        WHEN id = 2 THEN 9223372036854775807L
                        ELSE CAST(NULL AS BIGINT) END AS n
        FROM range(0, 4, 1, 2)
      ) ORDER BY id
      """
    Then query result ordered
      | id | l           | r  | u          | long_u              | literal_count |
      | 0  | 2           | -4 | 2147483644 | 9223372036854775804 | 2             |
      | 1  | -2147483648 | -1 | 1          | 1                   | 2             |
      | 2  | -2147483648 | -1 | 1          | 1                   | 2             |
      | 3  | NULL        | NULL | NULL     | NULL                | 2             |
    And query schema
      """
      root
       |-- id: long (nullable = false)
       |-- l: integer (nullable = true)
       |-- r: integer (nullable = true)
       |-- u: integer (nullable = true)
       |-- long_u: long (nullable = true)
       |-- literal_count: integer (nullable = false)
      """
