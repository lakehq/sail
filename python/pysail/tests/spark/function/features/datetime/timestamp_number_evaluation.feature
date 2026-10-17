Feature: Numeric timestamp conversions preserve their input

  Scenario Outline: Numeric timestamp checks use the same value as the conversion for <expression>
    When query
      """
      SELECT count(*) AS impossible_values
      FROM range(1000)
      WHERE coalesce(unix_micros(<expression>), 1000000L) NOT IN (<allowed>)
      """
    Then query result
      | impossible_values |
      | 0                 |

    Examples:
      | expression                                                                                                            | allowed                     |
      | CAST(CASE WHEN rand() < 0.5 THEN 1D ELSE CAST(NULL AS DOUBLE) END AS TIMESTAMP)                                          | 1000000                     |
      | timestamp_seconds(CAST(CASE WHEN rand() < 0.5 THEN 1 ELSE 2 END AS DECIMAL(8, 7)))                                        | 1000000, 2000000             |
      | CAST(CAST(CASE WHEN rand() < 0.5 THEN 1.0000001BD ELSE 2.0000009BD END AS DECIMAL(38, 7)) AS TIMESTAMP)                   | 1000000, 2000000             |
      | timestamp_seconds(CASE WHEN rand() < 0.5 THEN 1D ELSE CAST('Infinity' AS DOUBLE) END)                                     | 1000000                     |
      | TRY_CAST(CASE WHEN rand() < 0.5 THEN 1D ELSE CAST('Infinity' AS DOUBLE) END AS TIMESTAMP)                                 | 1000000                     |
      | CAST(CASE WHEN rand() < 0.5 THEN 1L ELSE 9223372036854775807L END AS TIMESTAMP)                                            | 1000000, 9223372036854775807 |

  Scenario: Numeric timestamp conversion preserves a non-nullable volatile input
    When query
      """
      SELECT CAST(monotonically_increasing_id() AS TIMESTAMP) AS value
      FROM range(1)
      """
    Then query schema
      """
      root
       |-- value: timestamp (nullable = false)
      """
    Then query result
      | value               |
      | 1970-01-01 00:00:00 |

  Scenario: Numeric timestamp conversion can bind a volatile input inside a lambda
    When query
      """
      SELECT transform(array(1D, 2D), value ->
               unix_micros(CAST(value + floor(rand()) AS TIMESTAMP))) AS values
      """
    Then query result
      | values             |
      | [1000000, 2000000] |

  Scenario Outline: Decimal timestamp casts truncate fractional microseconds at precision <precision> scale <scale> with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id,
             unix_micros(CAST(CAST(v AS DECIMAL(<precision>, <scale>)) AS TIMESTAMP)) AS strict_cast,
             unix_micros(TRY_CAST(CAST(v AS DECIMAL(<precision>, <scale>)) AS TIMESTAMP)) AS safe_cast,
             unix_micros(to_timestamp(CAST(v AS DECIMAL(<precision>, <scale>)))) AS timestamp_function,
             unix_micros(CAST(CAST(CASE WHEN id = 6 THEN NULL ELSE v END AS DECIMAL(<precision>, <scale>)) AS TIMESTAMP)) AS nullable_cast
      FROM VALUES (0, '1.0000009'), (1, '-1.0000009'),
                  (2, '1.0000001'), (3, '-1.0000001'),
                  (4, '0.0000009'), (5, '-0.0000009'), (6, '2') AS t(id, v)
      ORDER BY id
      """
    Then query result ordered
      | id | strict_cast | safe_cast | timestamp_function | nullable_cast |
      | 0  | 1000000     | 1000000   | 1000000            | 1000000       |
      | 1  | -1000000    | -1000000  | -1000000           | -1000000      |
      | 2  | 1000000     | 1000000   | 1000000            | 1000000       |
      | 3  | -1000000    | -1000000  | -1000000           | -1000000      |
      | 4  | 0           | 0         | 0                  | 0             |
      | 5  | 0           | 0         | 0                  | 0             |
      | 6  | 2000000     | 2000000   | 2000000            | NULL          |

    Examples:
      | precision | scale | ansi  |
      | 38        | 7     | true  |
      | 38        | 10    | true  |
      | 38        | 7     | false |
      | 38        | 10    | false |
      | 18        | 7     | true  |
      | 8         | 7     | true  |
