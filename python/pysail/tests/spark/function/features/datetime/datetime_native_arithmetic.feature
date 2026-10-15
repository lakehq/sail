Feature: Native timestamp expressions preserve Spark boundary behavior

  Background:
    Given config spark.sql.session.timeZone = UTC

  Scenario: Numeric to_timestamp_ntz parses a year rather than epoch units
    When query
      """
      SELECT CAST(to_timestamp_ntz(v) AS STRING) AS result
      FROM VALUES (2024), (CAST(NULL AS INT)) AS t(v)
      """
    Then query result
      | result              |
      | 2024-01-01 00:00:00 |
      | NULL                |

  Scenario: Numeric to_timestamp_ntz returns NULL for an invalid year in legacy mode
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT to_timestamp_ntz(1) AS result
      """
    Then query result
      | result |
      | NULL   |

  Scenario: Numeric to_timestamp_ntz rejects an invalid year in ANSI mode
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT to_timestamp_ntz(1)
      """
    Then query error (?i)cast|timestamp|invalid|parse

  Scenario Outline: Timestamp addition coerces fractional <unit> quantities to integers
    When query
      """
      SELECT id, unix_micros(timestampadd(<unit>, v, TIMESTAMP '1970-01-01 00:00:00')) AS micros
      FROM VALUES (0, -1.5D), (1, 1.5D), (2, 0.5D), (3, CAST(NULL AS DOUBLE)) AS t(id, v)
      ORDER BY id
      """
    Then query result
      | id | micros     |
      | 0  | <negative> |
      | 1  | <positive> |
      | 2  | 0          |
      | 3  | NULL       |

    Examples:
      | unit        | negative | positive |
      | SECOND      | -1000000 | 1000000  |
      | MILLISECOND | -1000    | 1000     |
      | MICROSECOND | -1       | 1        |

  Scenario Outline: Integer timestamp <cast> saturates without losing ordinary microseconds
    When query
      """
      SELECT id, unix_micros(<cast>(v AS TIMESTAMP_LTZ)) AS micros
      FROM VALUES (0, CAST('-9223372036854775808' AS BIGINT)),
                  (1, -9223372036854L), (2, 9223372036854L),
                  (3, 9223372036854775807L), (4, CAST(NULL AS BIGINT)) AS t(id, v)
      ORDER BY id
      """
    Then query result
      | id | micros               |
      | 0  | -9223372036854775808 |
      | 1  | -9223372036854000000 |
      | 2  | 9223372036854000000  |
      | 3  | 9223372036854775807  |
      | 4  | NULL                 |

    Examples:
      | cast     |
      | CAST     |
      | TRY_CAST |

  Scenario: Decimal timestamp casts truncate and wrap exactly
    When query
      """
      SELECT id, unix_micros(CAST(v AS TIMESTAMP_LTZ)) AS micros
      FROM VALUES (0, CAST(9223372036854.775808 AS DECIMAL(30,7))),
                  (1, CAST(-9223372036854.775809 AS DECIMAL(30,7))),
                  (2, CAST(-0.0000009 AS DECIMAL(30,7))),
                  (3, CAST(NULL AS DECIMAL(30,7))) AS t(id, v)
      ORDER BY id
      """
    Then query result
      | id | micros               |
      | 0  | -9223372036854775808 |
      | 1  | 9223372036854775807  |
      | 2  | 0                    |
      | 3  | NULL                 |

  Scenario: Decimal timestamp_seconds preserves exact microseconds and NULLs
    When query
      """
      SELECT id, unix_micros(timestamp_seconds(v)) AS micros
      FROM VALUES (0, CAST(9007199254.7409930 AS DECIMAL(30,7))),
                  (1, CAST(-0.0000010 AS DECIMAL(30,7))),
                  (2, CAST(NULL AS DECIMAL(30,7))) AS t(id, v)
      ORDER BY id
      """
    Then query result
      | id | micros           |
      | 0  | 9007199254740993 |
      | 1  | -1               |
      | 2  | NULL             |

  Scenario: Decimal timestamp_seconds rejects fractional microseconds
    When query
      """
      SELECT timestamp_seconds(v)
      FROM VALUES (CAST(NULL AS DECIMAL(20,7))), (CAST(0.0000001 AS DECIMAL(20,7))) AS t(v)
      """
    Then query error (?i)rounding|precision|overflow

  Scenario: Legacy floating timestamp casts preserve saturation and nonfinite NULLs
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, unix_micros(CAST(v AS TIMESTAMP_LTZ)) AS micros
      FROM VALUES (0, -0.5D), (1, 1.25D), (2, 1.0E300D), (3, -1.0E300D),
                  (4, CAST('NaN' AS DOUBLE)), (5, CAST('Infinity' AS DOUBLE)),
                  (6, CAST(NULL AS DOUBLE)) AS t(id, v)
      ORDER BY id
      """
    Then query result
      | id | micros               |
      | 0  | -500000              |
      | 1  | 1250000              |
      | 2  | 9223372036854775807  |
      | 3  | -9223372036854775808 |
      | 4  | NULL                 |
      | 5  | NULL                 |
      | 6  | NULL                 |

  Scenario: Constant floating timestamp casts preserve saturation and nonfinite NULLs
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT unix_micros(CAST(1.0E300D AS TIMESTAMP_LTZ)) AS saturated,
             CAST(CAST('NaN' AS DOUBLE) AS TIMESTAMP_LTZ) AS nan,
             timestamp_seconds(CAST('Infinity' AS DOUBLE)) AS infinity,
             unix_micros(TRY_CAST(9223372036854.775808D AS TIMESTAMP_LTZ)) AS boundary
      """
    Then query result
      | saturated           | nan  | infinity | boundary            |
      | 9223372036854775807 | NULL | NULL     | 9223372036854775807 |

  Scenario: Try floating timestamp casts preserve finite limits and reject overflow
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id, unix_micros(TRY_CAST(v AS TIMESTAMP_LTZ)) AS micros
      FROM VALUES (0, 9223372036854.775808D), (1, -9223372036854.775808D),
                  (2, 1.0E300D), (3, CAST('NaN' AS DOUBLE)),
                  (4, CAST('Infinity' AS DOUBLE)), (5, CAST(NULL AS DOUBLE)) AS t(id, v)
      ORDER BY id
      """
    Then query result
      | id | micros               |
      | 0  | 9223372036854775807  |
      | 1  | -9223372036854775808 |
      | 2  | NULL                 |
      | 3  | NULL                 |
      | 4  | NULL                 |
      | 5  | NULL                 |

  Scenario: ANSI constant floating timestamp casts accept the rounded upper boundary
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT unix_micros(CAST(9223372036854.775808D AS TIMESTAMP_LTZ)) AS micros
      """
    Then query result
      | micros              |
      | 9223372036854775807 |

  Scenario Outline: ANSI floating timestamp casts reject <value>
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT CAST(v AS TIMESTAMP_LTZ)
      FROM VALUES (CAST(NULL AS DOUBLE)), (<value>) AS t(v)
      """
    Then query error (?i)cast|overflow|malformed

    Examples:
      | value                        |
      | CAST('NaN' AS DOUBLE)        |
      | CAST('Infinity' AS DOUBLE)   |
      | 1.0E300D                     |

  Scenario Outline: Elapsed timestamp addition preserves <zone> overlap instants
    Given config spark.sql.session.timeZone = <zone>
    When query
      """
      SELECT id, unix_micros(timestampadd(HOUR, 1L, ts)) - unix_micros(ts) AS delta,
             unix_micros(ts + INTERVAL 1 SECOND) - unix_micros(ts) AS interval_delta
      FROM VALUES (0, TIMESTAMP '2024-11-03 08:30:00Z'),
                  (1, TIMESTAMP '2024-11-03 09:30:00Z'),
                  (2, CAST(NULL AS TIMESTAMP)) AS t(id, ts)
      ORDER BY id
      """
    Then query result
      | id | delta      | interval_delta |
      | 0  | 3600000000 | 1000000        |
      | 1  | 3600000000 | 1000000        |
      | 2  | NULL       | NULL           |

    Examples:
      | zone                |
      | UTC                 |
      | America/Los_Angeles |

  Scenario Outline: Calendar addition clamps month ends for <type>
    When query
      """
      SELECT CAST(timestampadd(MONTH, 1, CAST('2024-01-31 12:34:56.123456' AS <type>)) AS STRING) AS added,
             CAST(CAST('2024-01-31 12:34:56.123456' AS <type>) + INTERVAL 1 MONTH AS STRING) AS interval_added
      """
    Then query result
      | added                      | interval_added             |
      | 2024-02-29 12:34:56.123456 | 2024-02-29 12:34:56.123456 |

    Examples:
      | type          |
      | TIMESTAMP_LTZ |
      | TIMESTAMP_NTZ |

  Scenario: Calendar addition retains the full UTC timestamp range
    When query
      """
      SELECT unix_micros(timestampadd(MONTH, 1, ts)) AS added,
             unix_micros(ts + INTERVAL 1 MONTH) AS interval_added,
             unix_micros(timestampadd(DAY, 1, ts)) AS day_added,
             unix_micros(timestampadd(WEEK, 1, ts)) AS week_added
      FROM (SELECT timestamp_micros(9000000000000000000L) AS ts)
      """
    Then query result
      | added               | interval_added      | day_added           | week_added          |
      | 9000002678400000000 | 9000002678400000000 | 9000000086400000000 | 9000000604800000000 |

  Scenario: Large UTC day quantities can cancel the starting timestamp
    When query
      """
      SELECT unix_micros(timestampadd(DAY, 200000000, timestamp_micros(-9000000000000000000L))) AS result
      """
    Then query result
      | result              |
      | 8280000000000000000 |

  @sail-bug
  Scenario: Named-zone calendar days preserve timestamps beyond Chrono's range
    # Accepted extreme-range regression: year 287168 exceeds Chrono's calendar range.
    # The former elapsed-day arithmetic handled this value but was wrong across DST.
    # Retain DST correctness for ordinary dates and defer broader timezone range support.
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT unix_micros(timestamp_micros(9000000000000000000L) + INTERVAL 1 DAY) AS result
      """
    Then query result
      | result              |
      | 9000000086400000000 |
