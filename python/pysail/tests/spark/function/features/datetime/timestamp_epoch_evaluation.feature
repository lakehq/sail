Feature: Timestamp epoch conversions evaluate their input once

  Scenario Outline: <expression> does not combine different volatile timestamp values
    When query
      """
      SELECT count(*) AS impossible_values
      FROM range(1000)
      WHERE <expression> = 1
      """
    Then query result
      | impossible_values |
      | 0                 |

    Examples:
      | expression                                                                                    |
      | unix_seconds(timestamp_micros(CASE WHEN rand() < 0.5 THEN -1L ELSE 2000000L END))                |
      | unix_millis(timestamp_micros(CASE WHEN rand() < 0.5 THEN -1L ELSE 2000L END))                     |
      | CAST(timestamp_micros(CASE WHEN rand() < 0.5 THEN -1L ELSE 2000000L END) AS BIGINT)               |

  Scenario: Epoch conversion keeps integer extremes, negative fractions and NULLs
    When query
      """
      SELECT id, unix_seconds(ts) AS seconds, unix_millis(ts) AS millis,
             CAST(ts AS BIGINT) AS cast_seconds
      FROM (
        SELECT id, timestamp_micros(micros) AS ts
        FROM VALUES (0, CAST('-9223372036854775808' AS BIGINT)), (1, -1000001L),
                    (2, -1L), (3, 0L), (4, 1L), (5, 1000001L),
                    (6, 9223372036854775807L), (7, CAST(NULL AS BIGINT)) AS t(id, micros)
      )
      ORDER BY id
      """
    Then query result ordered
      | id | seconds        | millis            | cast_seconds   |
      | 0  | -9223372036855 | -9223372036854776 | -9223372036855 |
      | 1  | -2             | -1001             | -2             |
      | 2  | -1             | -1                | -1             |
      | 3  | 0              | 0                 | 0              |
      | 4  | 0              | 0                 | 0              |
      | 5  | 1              | 1000              | 1              |
      | 6  | 9223372036854  | 9223372036854775  | 9223372036854  |
      | 7  | NULL           | NULL              | NULL           |

  Scenario: Epoch conversion preserves input nullability
    When query
      """
      SELECT unix_seconds(timestamp_micros(id)) AS seconds,
             unix_millis(timestamp_micros(id)) AS millis,
             CAST(timestamp_micros(id) AS BIGINT) AS cast_seconds,
             unix_seconds(timestamp_micros(CASE WHEN id = 0 THEN id END)) AS nullable_seconds
      FROM range(2)
      """
    Then query schema
      """
      root
       |-- seconds: long (nullable = false)
       |-- millis: long (nullable = false)
       |-- cast_seconds: long (nullable = false)
       |-- nullable_seconds: long (nullable = true)
      """
