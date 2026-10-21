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
      FROM range(0, 1, 1, 1)
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

  # TODO: Support outer references in non-aggregate scalar projections.
  @sail-bug
  Scenario: Numeric TRY_CAST evaluates a volatile scalar subquery once
    Given config spark.sql.session.timeZone = UTC
    And config spark.sql.ansi.enabled = true
    And config spark.sql.timestampType = TIMESTAMP_LTZ
    # Overflow checking and conversion must use the same value: zero converts to zero,
    # and 1e20 overflows to NULL. A saturated timestamp is impossible for this TRY_CAST.
    When query
      """
      SELECT DISTINCT unix_micros(TRY_CAST(
        (SELECT IF(rand(1) < 0.5, 0D + t.id * 0D, 1e20D)) AS TIMESTAMP
      )) AS result FROM range(1000) t ORDER BY result
      """
    Then query result collected ordered
      | result |
      | NULL   |
      | 0      |

  # TODO: Support outer references in non-aggregate scalar projections.
  @sail-bug
  Scenario: timestamp_seconds evaluates a volatile scalar subquery once
    Given config spark.sql.session.timeZone = UTC
    # The finite-value check and conversion must share the subquery's value.
    When query
      """
      SELECT DISTINCT unix_micros(timestamp_seconds(
        (SELECT IF(rand(1) < 0.5, 1D + t.id * 0D, CAST('Infinity' AS DOUBLE)))
      )) AS result FROM range(1000) t ORDER BY result
      """
    Then query result collected ordered
      | result  |
      | NULL    |
      | 1000000 |

  Scenario Outline: Integer timestamp casts saturate column values with ANSI <ansi>
    Given config spark.sql.session.timeZone = UTC
    And config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id, unix_micros(CAST(v AS TIMESTAMP)) AS strict_cast,
             unix_micros(TRY_CAST(v AS TIMESTAMP)) AS safe_cast,
             unix_micros(to_timestamp(v)) AS timestamp_function
      FROM VALUES (0, -9223372036854775808L), (1, -9223372036855L),
                  (2, -9223372036854L), (3, 9223372036854L),
                  (4, 9223372036855L), (5, 9223372036854775807L),
                  (6, CAST(NULL AS BIGINT)) AS t(id, v)
      ORDER BY id
      """
    Then query result ordered
      | id | strict_cast          | safe_cast            | timestamp_function   |
      | 0  | -9223372036854775808 | -9223372036854775808 | -9223372036854775808 |
      | 1  | -9223372036854775808 | -9223372036854775808 | -9223372036854775808 |
      | 2  | -9223372036854000000 | -9223372036854000000 | -9223372036854000000 |
      | 3  | 9223372036854000000  | 9223372036854000000  | 9223372036854000000  |
      | 4  | 9223372036854775807  | 9223372036854775807  | 9223372036854775807  |
      | 5  | 9223372036854775807  | 9223372036854775807  | 9223372036854775807  |
      | 6  | NULL                 | NULL                 | NULL                 |
    When query
      """
      SELECT CAST(id AS TIMESTAMP) AS strict_cast, TRY_CAST(id AS TIMESTAMP) AS safe_cast,
             to_timestamp(id) AS timestamp_function FROM range(1)
      """
    Then query schema
      """
      root
       |-- strict_cast: timestamp (nullable = false)
       |-- safe_cast: timestamp (nullable = false)
       |-- timestamp_function: timestamp (nullable = false)
      """
    Then query result
      | strict_cast         | safe_cast           | timestamp_function  |
      | 1970-01-01 00:00:00 | 1970-01-01 00:00:00 | 1970-01-01 00:00:00 |

    Examples:
      | ansi  |
      | true  |
      | false |

  Scenario Outline: Integer timestamp checks reuse a computed column for <producer>
    # Native range checks may read a bound column more than once. Projection
    # simplification must not duplicate its volatile or scalar-subquery producer.
    When query
      """
      SELECT count(*) AS impossible_values FROM (
        SELECT <producer> AS v FROM range(1000)
      ) t
      WHERE unix_micros(CAST(v AS TIMESTAMP)) NOT IN (1000000L, 9223372036854775807L)
      """
    Then query result
      | impossible_values |
      | 0                 |

    Examples:
      | producer                                                                         |
      | CASE WHEN rand(1) < 0.5 THEN 1L ELSE 9223372036854775807L END                       |
      | (SELECT CASE WHEN rand(1) < 0.5 THEN 1L ELSE 9223372036854775807L END)              |
