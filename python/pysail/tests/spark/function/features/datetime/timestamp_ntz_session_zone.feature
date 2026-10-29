Feature: TIMESTAMP_NTZ and TIMESTAMP values in the session time zone
  Spark casts TIMESTAMP_NTZ to TIMESTAMP in the session time zone, which it accepts in the spellings of
  `java.time.ZoneId`, in grouping sets, window keys, `timestampdiff`, `nvl2` and `nullif`.

  Scenario: ROLLUP keyed by a TIMESTAMP_NTZ comparison with a TIMESTAMP constant
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT count(*) AS n
      FROM VALUES (0, TIMESTAMP_NTZ '2020-06-13 12:00:00'), (1, TIMESTAMP_NTZ '2020-06-15 12:00:00') AS t(id, ntz)
      GROUP BY ROLLUP (ntz > TIMESTAMP '2020-06-14 00:00:00')
      ORDER BY n
      """
    Then query result ordered
      | n |
      | 1 |
      | 1 |
      | 2 |

  Scenario: GROUPING SETS keyed by a TIMESTAMP_NTZ and TIMESTAMP comparison
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT count(*) AS n
      FROM VALUES
        (0, TIMESTAMP_NTZ '2020-06-13 12:00:00', TIMESTAMP '2020-06-14 12:00:00'),
        (1, TIMESTAMP_NTZ '2020-06-15 12:00:00', TIMESTAMP '2020-06-14 12:00:00') AS t(id, ntz, ltz)
      GROUP BY GROUPING SETS ((id, ntz < ltz), ())
      ORDER BY n
      """
    Then query result ordered
      | n |
      | 1 |
      | 1 |
      | 2 |

  Scenario: Window partitioned and ordered by a TIMESTAMP_NTZ and TIMESTAMP comparison
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        count(*) OVER (PARTITION BY ntz < ltz) AS n,
        rank() OVER (PARTITION BY id % 2 ORDER BY ntz < ltz) AS r,
        count(*) OVER (PARTITION BY date_trunc('DAY', ntz) < ltz) AS d
      FROM VALUES
        (0, TIMESTAMP_NTZ '2020-06-13 12:00:00', TIMESTAMP '2020-06-14 12:00:00'),
        (1, TIMESTAMP_NTZ '2020-06-15 12:00:00', TIMESTAMP '2020-06-14 12:00:00') AS t(id, ntz, ltz)
      ORDER BY id
      """
    Then query result ordered
      | id | n | r | d |
      | 0  | 1 | 1 | 1 |
      | 1  | 1 | 1 | 1 |

  Scenario: timestampdiff of a TIMESTAMP_NTZ value in a DST gap and a TIMESTAMP value
    Given config spark.sql.session.timeZone = Europe/London
    When query
      """
      SELECT
        timestampdiff(SECOND, TIMESTAMP_NTZ '2024-03-31 01:30:00', TIMESTAMP '2024-03-31 03:00:00') AS s,
        timestampdiff(MINUTE, TIMESTAMP_NTZ '2024-03-31 01:30:00', TIMESTAMP '2024-03-31 03:00:00') AS m,
        timestampdiff(HOUR, TIMESTAMP_NTZ '2024-03-31 01:30:00', TIMESTAMP '2024-03-31 04:00:00') AS h
      """
    Then query result
      | s    | m  | h |
      | 1800 | 30 | 1 |

  Scenario: TIMESTAMP values in the session time zone Z
    Given config spark.sql.session.timeZone = Z
    When query
      """
      SELECT
        current_timezone() AS zone,
        CAST(timestamp_micros(1592049600000000) AS STRING) AS ltz,
        CAST(CAST(timestamp_micros(1592049600000000) AS DATE) AS STRING) AS date,
        CAST(coalesce(TIMESTAMP_NTZ '2020-06-13 12:00:00', timestamp_micros(0)) AS STRING) AS c,
        TIMESTAMP_NTZ '2020-06-13 11:00:00' < timestamp_micros(1592049600000000) AS lt
      """
    Then query result
      | zone | ltz                 | date       | c                   | lt   |
      | Z    | 2020-06-13 12:00:00 | 2020-06-13 | 2020-06-13 12:00:00 | true |

  Scenario: TIMESTAMP values in the session time zone GMT+8
    Given config spark.sql.session.timeZone = GMT+8
    When query
      """
      SELECT
        current_timezone() AS zone,
        CAST(timestamp_micros(1592049600000000) AS STRING) AS ltz,
        CAST(coalesce(TIMESTAMP_NTZ '2020-06-13 12:00:00', timestamp_micros(0)) AS STRING) AS c,
        TIMESTAMP_NTZ '2020-06-13 19:00:00' < timestamp_micros(1592049600000000) AS lt,
        months_between(timestamp_micros(1592049600000000), TIMESTAMP_NTZ '2020-01-01 00:00:00') AS mb
      """
    Then query result
      | zone  | ltz                 | c                   | lt   | mb         |
      | GMT+8 | 2020-06-13 20:00:00 | 2020-06-13 12:00:00 | true | 5.41397849 |

  Scenario: TIMESTAMP values in the session time zone PST
    Given config spark.sql.session.timeZone = PST
    When query
      """
      SELECT
        current_timezone() AS zone,
        CAST(timestamp_micros(1592049600000000) AS STRING) AS ltz,
        CAST(coalesce(TIMESTAMP_NTZ '2020-06-13 12:00:00', timestamp_micros(0)) AS STRING) AS c,
        TIMESTAMP_NTZ '2020-06-13 04:00:00' < timestamp_micros(1592049600000000) AS lt
      """
    Then query result
      | zone | ltz                 | c                   | lt   |
      | PST  | 2020-06-13 05:00:00 | 2020-06-13 12:00:00 | true |

  Scenario: TIMESTAMP values in the session time zone +5:30
    Given config spark.sql.session.timeZone = +5:30
    When query
      """
      SELECT
        current_timezone() AS zone,
        CAST(timestamp_micros(1592049600000000) AS STRING) AS ltz,
        CAST(coalesce(TIMESTAMP_NTZ '2020-06-13 12:00:00', timestamp_micros(0)) AS STRING) AS c
      """
    Then query result
      | zone  | ltz                 | c                   |
      | +5:30 | 2020-06-13 17:30:00 | 2020-06-13 12:00:00 |

  Scenario: nvl2 casts a DATE branch to TIMESTAMP at the start of the day in the session time zone
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        CAST(nvl2(TIMESTAMP '2020-03-29 01:30:00', DATE '2020-06-14', timestamp_micros(0)) AS STRING) AS s,
        unix_micros(nvl2(TIMESTAMP '2020-03-29 01:30:00', DATE '2020-06-14', timestamp_micros(0))) AS u
      """
    Then query result
      | s                   | u                |
      | 2020-06-14 00:00:00 | 1592118000000000 |

  Scenario: nullif of date_trunc over TIMESTAMP_NTZ returns TIMESTAMP
    Given config spark.sql.session.timeZone = Europe/London
    When query
      """
      SELECT
        CAST(date_trunc('HOUR', TIMESTAMP_NTZ '2024-01-01 10:30:00') AS STRING) AS s,
        typeof(nullif(date_trunc('HOUR', TIMESTAMP_NTZ '2024-01-01 10:30:00'), TIMESTAMP '2024-01-01 10:00:00')) AS n
      """
    Then query result
      | s                   | n         |
      | 2024-01-01 10:00:00 | timestamp |

  # Sail labels values in a session time zone that is an offset with seconds with UTC. These values
  # do not depend on the offset.
  Scenario: TIMESTAMP values in the session time zone +05:30:45
    Given config spark.sql.session.timeZone = +05:30:45
    When query
      """
      SELECT
        current_timezone() AS zone,
        CAST(coalesce(TIMESTAMP_NTZ '2024-06-01 12:00:00', timestamp_micros(0)) AS STRING) AS c,
        hour(coalesce(TIMESTAMP_NTZ '2024-06-01 12:00:00', timestamp_micros(0))) AS h,
        CAST(trunc(timestamp_micros(1717243200000000), 'MM') AS STRING) AS m
      """
    Then query result
      | zone      | c                   | h  | m          |
      | +05:30:45 | 2024-06-01 12:00:00 | 12 | 2024-06-01 |

  Scenario: INSERT casts TIMESTAMP to a TIMESTAMP_NTZ column in the session time zone
    Given variable location for temporary directory timestamp_ntz_session_zone_insert
    Given final statement
      """
      DROP TABLE IF EXISTS timestamp_ntz_session_zone_insert
      """
    Given config spark.sql.session.timeZone = Asia/Shanghai
    Given statement template
      """
      CREATE TABLE timestamp_ntz_session_zone_insert (id INT, ts TIMESTAMP_NTZ) USING parquet LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO timestamp_ntz_session_zone_insert
      SELECT 0, coalesce(TIMESTAMP_NTZ '2020-06-13 12:00:00', TIMESTAMP '2020-06-13 20:00:00')
      UNION ALL
      SELECT 1, TIMESTAMP '2020-06-13 20:00:00'
      """
    When query
      """
      SELECT id, CAST(ts AS STRING) AS ts FROM timestamp_ntz_session_zone_insert ORDER BY id
      """
    Then query result ordered
      | id | ts                  |
      | 0  | 2020-06-13 12:00:00 |
      | 1  | 2020-06-13 20:00:00 |

  Scenario: date_trunc of TIMESTAMP values with one out of the nanosecond range
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT id, unix_micros(date_trunc('DAY', ts)) AS d
      FROM VALUES (0, TIMESTAMP '2020-06-14 05:30:00'), (1, TIMESTAMP '1500-01-01 00:00:00') AS t(id, ts)
      ORDER BY id
      """
    Then query result ordered
      | id | d                  |
      | 0  | 1592118000000000   |
      | 1  | -14831741222000000 |

  Scenario: date_trunc of TIMESTAMP values with one after the nanosecond range
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT id, CAST(date_trunc('DAY', ts) AS STRING) AS d, CAST(date_trunc('HOUR', ts) AS STRING) AS h
      FROM VALUES (0, TIMESTAMP '2020-06-14 05:30:00'), (1, timestamp_micros(253402300799999999)) AS t(id, ts)
      ORDER BY id
      """
    Then query result ordered
      | id | d                   | h                   |
      | 0  | 2020-06-14 00:00:00 | 2020-06-14 05:00:00 |
      | 1  | 9999-12-31 00:00:00 | 9999-12-31 23:00:00 |

  Scenario: nvl2 casts a DATE branch whose midnight is repeated
    Given config spark.sql.session.timeZone = America/Havana
    When query
      """
      SELECT CAST(nvl2(1, DATE '2020-11-01', timestamp_micros(0)) AS STRING) AS s
      """
    Then query result
      | s                   |
      | 2020-11-01 00:00:00 |
