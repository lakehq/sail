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
