Feature: Timestamp windows use the session time zone

  Background:
    Given variable location for temporary directory timestamp_window
    Given final statement
      """
      DROP TABLE IF EXISTS timestamp_window
      """
    Given statement template
      """
      CREATE TABLE timestamp_window USING parquet LOCATION {{ location.sql }} AS
      SELECT id, TIMESTAMP_NTZ '2020-06-14 12:00:00' + id * INTERVAL 30 MINUTES AS ts
      FROM range(24)
      """

  Scenario Outline: A mixed timestamp window includes the whole local day in <zone>
    Given config spark.sql.session.timeZone = <zone>
    And config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT count(*) AS n, CAST(min(ts) AS STRING) AS first, CAST(max(ts) AS STRING) AS last
      FROM timestamp_window
      WHERE ts >= TIMESTAMP '2020-06-14 00:00:00' AND ts < TIMESTAMP '2020-06-15 00:00:00'
      """
    Then query result
      | n  | first               | last                |
      | 24 | 2020-06-14 12:00:00 | 2020-06-14 23:30:00 |

    Examples:
      | zone                | ansi  |
      | Asia/Shanghai       | true  |
      | Asia/Shanghai       | false |
      | UTC                 | true  |
      | America/Los_Angeles | true  |
      | +05:30              | true  |
      | -07:00              | false |

  Scenario: Window boundaries, reversed operands, and BETWEEN agree
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        count(CASE WHEN ts >= TIMESTAMP '2020-06-14 12:00:00'
                    AND ts < TIMESTAMP '2020-06-14 20:00:00' THEN 1 END) AS forward,
        count(CASE WHEN TIMESTAMP '2020-06-14 12:00:00' <= ts
                    AND TIMESTAMP '2020-06-14 20:00:00' > ts THEN 1 END) AS reversed,
        count(CASE WHEN ts BETWEEN TIMESTAMP '2020-06-14 12:00:00'
                              AND TIMESTAMP '2020-06-14 20:00:00' THEN 1 END) AS inclusive,
        count(CASE WHEN ts = TIMESTAMP '2020-06-14 20:00:00' THEN 1 END) AS equal,
        count(CASE WHEN ts <=> TIMESTAMP '2020-06-14 20:00:00' THEN 1 END) AS null_safe,
        count(CASE WHEN ts != TIMESTAMP '2020-06-14 20:00:00' THEN 1 END) AS unequal
      FROM timestamp_window
      """
    Then query result
      | forward | reversed | inclusive | equal | null_safe | unequal |
      | 16      | 16       | 17        | 1     | 1         | 23      |

  Scenario: Explicit offsets in literals do not replace the session time zone
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT count(*) AS n FROM timestamp_window
      WHERE ts >= TIMESTAMP '2020-06-14 04:00:00Z'
        AND ts < TIMESTAMP '2020-06-14 12:00:00Z'
      """
    Then query result
      | n  |
      | 16 |

  Scenario: Comparisons select the correct boundary row
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT id,
        ts < TIMESTAMP '2020-06-14 20:00:00' AS before,
        ts = TIMESTAMP '2020-06-14 20:00:00' AS equal,
        ts > TIMESTAMP '2020-06-14 20:00:00' AS after,
        ts IS DISTINCT FROM TIMESTAMP '2020-06-14 20:00:00' AS different
      FROM timestamp_window WHERE id IN (0, 16, 23) ORDER BY id
      """
    Then query result
      | id | before | equal | after | different |
      | 0  | true   | false | false | true      |
      | 16 | false  | true  | false | false     |
      | 23 | false  | false | true  | true      |

  Scenario: Timestamp joins interpret wall times in the session time zone
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT t.id FROM timestamp_window t
      JOIN (SELECT TIMESTAMP '2020-06-14 12:00:00Z' AS ts) b ON t.ts = b.ts
      """
    Then query result
      | id |
      | 16 |

  Scenario: Unselected conditional branches do not evaluate timestamp bounds
    Given config spark.sql.session.timeZone = Asia/Shanghai
    And config spark.sql.ansi.enabled = true
    When query
      """
      SELECT
        CASE WHEN false THEN ts < to_timestamp('not-a-timestamp') ELSE false END AS case_result,
        IF(false, ts < to_timestamp('not-a-timestamp'), false) AS if_result,
        CASE WHEN id < 0 THEN ts < to_timestamp('not-a-timestamp') ELSE false END AS dynamic_result
      FROM timestamp_window WHERE id = 0
      """
    Then query result
      | case_result | if_result | dynamic_result |
      | false       | false     | false          |
    When query
      """
      SELECT ts < to_timestamp('not-a-timestamp') FROM timestamp_window WHERE id = 0
      """
    Then query error not-a-timestamp

  Scenario: Daylight saving gaps and overlaps follow Spark's local-time resolution
    Given config spark.sql.session.timeZone = America/New_York
    And variable dst_location for temporary directory timestamp_dst_window
    And final statement
      """
      DROP TABLE IF EXISTS timestamp_dst_window
      """
    And statement template
      """
      CREATE TABLE timestamp_dst_window USING parquet LOCATION {{ dst_location.sql }} AS
      SELECT *, TIMESTAMP '2020-03-08 03:15:00' AS boundary FROM VALUES
        (0, TIMESTAMP_NTZ '2020-03-08 01:59:59.999999'),
        (1, TIMESTAMP_NTZ '2020-03-08 02:00:00'),
        (2, TIMESTAMP_NTZ '2020-03-08 02:30:00'),
        (3, TIMESTAMP_NTZ '2020-03-08 03:00:00'),
        (4, TIMESTAMP_NTZ '2020-03-08 03:30:00'),
        (5, TIMESTAMP_NTZ '2020-11-01 01:00:00'),
        (6, TIMESTAMP_NTZ '2020-11-01 01:30:00'),
        (7, TIMESTAMP_NTZ '2020-11-01 01:59:59.999999'),
        (8, TIMESTAMP_NTZ '2020-11-01 02:00:00'),
        (9, CAST(NULL AS TIMESTAMP_NTZ)) AS t(id, ts)
      """
    # Spark rewrites constant bounds to NTZ and includes 02:30, even though it converts to 03:30.
    # Sail compares converted values consistently, so 02:30 is not before the 03:15 bound.
    When query
      """
      SELECT id FROM timestamp_dst_window
      WHERE ts < TIMESTAMP '2020-03-08 03:15:00' ORDER BY id
      """
    Then query result for Spark
      | id |
      | 0  |
      | 1  |
      | 2  |
      | 3  |
    Then query result for Sail
      | id |
      | 0  |
      | 1  |
      | 3  |
    When query
      """
      SELECT id FROM timestamp_dst_window
      WHERE ts < to_timestamp('2020-03-08 03:15:00') ORDER BY id
      """
    Then query result for Spark
      | id |
      | 0  |
      | 1  |
      | 2  |
      | 3  |
    Then query result for Sail
      | id |
      | 0  |
      | 1  |
      | 3  |
    When query
      """
      SELECT id FROM timestamp_dst_window WHERE ts < boundary ORDER BY id
      """
    Then query result
      | id |
      | 0  |
      | 1  |
      | 3  |
    When query
      """
      SELECT TIMESTAMP_NTZ '2020-03-08 02:30:00' < TIMESTAMP '2020-03-08 03:15:00' AS before
      """
    Then query result
      | before |
      | false  |
    When query
      """
      SELECT id FROM timestamp_dst_window
      WHERE ts >= TIMESTAMP '2020-11-01 05:15:00Z'
        AND ts < TIMESTAMP '2020-11-01 06:00:00Z' ORDER BY id
      """
    Then query result
      | id |
      | 6  |
      | 7  |
    When query
      """
      SELECT ts < TIMESTAMP '2020-03-08 03:15:00' AS before,
             ts <=> TIMESTAMP '2020-03-08 03:15:00' AS null_safe,
             ts IS DISTINCT FROM TIMESTAMP '2020-03-08 03:15:00' AS different
      FROM timestamp_dst_window WHERE id = 9
      """
    Then query result
      | before | null_safe | different |
      | NULL   | false     | true      |

  Scenario Outline: The configured timestamp literal type uses the same local window
    Given config spark.sql.session.timeZone = Asia/Shanghai
    And config spark.sql.timestampType = <type>
    When query
      """
      SELECT count(*) AS n FROM timestamp_window
      WHERE ts >= TIMESTAMP '2020-06-14 12:00:00'
        AND ts < TIMESTAMP '2020-06-14 20:00:00'
      """
    Then query result
      | n  |
      | 16 |

    Examples:
      | type          |
      | TIMESTAMP_NTZ |
      | TIMESTAMP_LTZ |

  Scenario Outline: Explicit <cast> preserves window bounds in the session timezone
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT id FROM timestamp_window
      WHERE <cast>(ts AS TIMESTAMP_LTZ) >= TIMESTAMP '2020-06-14 23:00:00'
        AND <cast>(ts AS TIMESTAMP_LTZ) < TIMESTAMP '2020-06-15 00:00:00'
      ORDER BY id
      """
    Then query result
      | id |
      | 22 |
      | 23 |

    Examples:
      | cast     |
      | CAST     |
      | TRY_CAST |

  Scenario: Timestamp casts use the session timezone
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT unix_micros(CAST(ts AS TIMESTAMP_LTZ)) AS micros
      FROM timestamp_window WHERE id = 0
      """
    Then query result
      | micros           |
      | 1592161200000000 |
