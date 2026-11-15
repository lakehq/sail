Feature: TIMESTAMP interval arithmetic uses the session time zone
  Spark adds the months and days of an interval to a TIMESTAMP as a wall-clock time in the session time zone, keeping the
  UTC offset in a DST overlap when it is still valid and moving a time in a DST gap forward, and adds the rest of the
  interval to the instant. A day-time interval adds its whole days as days.

  Background:
    Given variable location for temporary directory timestamp_interval_session_zone
    Given final statement
      """
      DROP TABLE IF EXISTS timestamp_interval_session_zone
      """
    Given config spark.sql.session.timeZone = UTC
    Given statement template
      """
      CREATE TABLE timestamp_interval_session_zone (id INT, ltz TIMESTAMP) USING parquet LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO timestamp_interval_session_zone VALUES
        (0, TIMESTAMP '2020-10-31 08:30:00'),
        (1, TIMESTAMP '2020-11-01 08:30:00'),
        (2, TIMESTAMP '2020-11-01 09:30:00'),
        (3, TIMESTAMP '2020-03-07 10:30:00'),
        (4, TIMESTAMP '2020-02-08 10:30:00'),
        (5, TIMESTAMP '2020-01-31 20:00:00'),
        (6, TIMESTAMP '2020-02-29 20:00:00'),
        (7, TIMESTAMP '2020-01-15 09:30:00'),
        (8, TIMESTAMP '2018-11-03 03:30:00'),
        (9, NULL)
      """

  Scenario: TIMESTAMP day-time interval arithmetic uses the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        CAST(ltz + INTERVAL 1 DAY AS STRING) AS plus_day,
        unix_seconds(ltz + INTERVAL 1 DAY) - unix_seconds(ltz) AS plus_day_seconds,
        unix_seconds(ltz - INTERVAL 1 DAY) - unix_seconds(ltz) AS minus_day_seconds,
        unix_seconds(ltz + INTERVAL 24 HOURS) - unix_seconds(ltz) AS plus_24_hours_seconds,
        unix_seconds(ltz + INTERVAL 2 HOURS) - unix_seconds(ltz) AS plus_2_hours_seconds,
        unix_seconds(ltz + INTERVAL 291 DAYS) - unix_seconds(ltz) AS plus_291_days_seconds,
        unix_seconds(INTERVAL 1 DAY + ltz) - unix_seconds(ltz) AS interval_plus_seconds,
        unix_seconds(timestampadd(DAY, 1, ltz)) - unix_seconds(ltz) AS timestampadd_day_seconds,
        unix_seconds(timestampadd(HOUR, 24, ltz)) - unix_seconds(ltz) AS timestampadd_24_hours_seconds
      FROM timestamp_interval_session_zone
      ORDER BY id
      """
    Then query result ordered
      | id | plus_day            | plus_day_seconds | minus_day_seconds | plus_24_hours_seconds | plus_2_hours_seconds | plus_291_days_seconds | interval_plus_seconds | timestampadd_day_seconds | timestampadd_24_hours_seconds |
      | 0  | 2020-11-01 01:30:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 1  | 2020-11-02 01:30:00 | 90000            | -86400            | 90000                 | 7200                 | 25142400              | 90000                 | 90000                    | 86400                         |
      | 2  | 2020-11-02 01:30:00 | 86400            | -90000            | 86400                 | 7200                 | 25138800              | 86400                 | 86400                    | 86400                         |
      | 3  | 2020-03-08 03:30:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 4  | 2020-02-09 02:30:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 5  | 2020-02-01 12:00:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 6  | 2020-03-01 12:00:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 7  | 2020-01-16 01:30:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 8  | 2018-11-03 20:30:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 9  | NULL                | NULL             | NULL              | NULL                  | NULL                 | NULL                  | NULL                  | NULL                     | NULL                          |

  Scenario: TIMESTAMP day-time interval arithmetic uses the session time zone (America/Sao_Paulo)
    Given config spark.sql.session.timeZone = America/Sao_Paulo
    When query
      """
      SELECT
        id,
        CAST(ltz + INTERVAL 1 DAY AS STRING) AS plus_day,
        unix_seconds(ltz + INTERVAL 1 DAY) - unix_seconds(ltz) AS plus_day_seconds,
        unix_seconds(ltz - INTERVAL 1 DAY) - unix_seconds(ltz) AS minus_day_seconds,
        unix_seconds(ltz + INTERVAL 24 HOURS) - unix_seconds(ltz) AS plus_24_hours_seconds,
        unix_seconds(ltz + INTERVAL 2 HOURS) - unix_seconds(ltz) AS plus_2_hours_seconds,
        unix_seconds(ltz + INTERVAL 291 DAYS) - unix_seconds(ltz) AS plus_291_days_seconds,
        unix_seconds(INTERVAL 1 DAY + ltz) - unix_seconds(ltz) AS interval_plus_seconds,
        unix_seconds(timestampadd(DAY, 1, ltz)) - unix_seconds(ltz) AS timestampadd_day_seconds,
        unix_seconds(timestampadd(HOUR, 24, ltz)) - unix_seconds(ltz) AS timestampadd_24_hours_seconds
      FROM timestamp_interval_session_zone
      ORDER BY id
      """
    Then query result ordered
      | id | plus_day            | plus_day_seconds | minus_day_seconds | plus_24_hours_seconds | plus_2_hours_seconds | plus_291_days_seconds | interval_plus_seconds | timestampadd_day_seconds | timestampadd_24_hours_seconds |
      | 0  | 2020-11-01 05:30:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 1  | 2020-11-02 05:30:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 2  | 2020-11-02 06:30:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 3  | 2020-03-08 07:30:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 4  | 2020-02-09 07:30:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 5  | 2020-02-01 17:00:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 6  | 2020-03-01 17:00:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 7  | 2020-01-16 06:30:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 8  | 2018-11-04 01:30:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 9  | NULL                | NULL             | NULL              | NULL                  | NULL                 | NULL                  | NULL                  | NULL                     | NULL                          |

  Scenario: TIMESTAMP day-time interval arithmetic uses the session time zone (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        id,
        CAST(ltz + INTERVAL 1 DAY AS STRING) AS plus_day,
        unix_seconds(ltz + INTERVAL 1 DAY) - unix_seconds(ltz) AS plus_day_seconds,
        unix_seconds(ltz - INTERVAL 1 DAY) - unix_seconds(ltz) AS minus_day_seconds,
        unix_seconds(ltz + INTERVAL 24 HOURS) - unix_seconds(ltz) AS plus_24_hours_seconds,
        unix_seconds(ltz + INTERVAL 2 HOURS) - unix_seconds(ltz) AS plus_2_hours_seconds,
        unix_seconds(ltz + INTERVAL 291 DAYS) - unix_seconds(ltz) AS plus_291_days_seconds,
        unix_seconds(INTERVAL 1 DAY + ltz) - unix_seconds(ltz) AS interval_plus_seconds,
        unix_seconds(timestampadd(DAY, 1, ltz)) - unix_seconds(ltz) AS timestampadd_day_seconds,
        unix_seconds(timestampadd(HOUR, 24, ltz)) - unix_seconds(ltz) AS timestampadd_24_hours_seconds
      FROM timestamp_interval_session_zone
      ORDER BY id
      """
    Then query result ordered
      | id | plus_day            | plus_day_seconds | minus_day_seconds | plus_24_hours_seconds | plus_2_hours_seconds | plus_291_days_seconds | interval_plus_seconds | timestampadd_day_seconds | timestampadd_24_hours_seconds |
      | 0  | 2020-11-01 08:30:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 1  | 2020-11-02 08:30:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 2  | 2020-11-02 09:30:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 3  | 2020-03-08 10:30:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 4  | 2020-02-09 10:30:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 5  | 2020-02-01 20:00:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 6  | 2020-03-01 20:00:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 7  | 2020-01-16 09:30:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 8  | 2018-11-04 03:30:00 | 86400            | -86400            | 86400                 | 7200                 | 25142400              | 86400                 | 86400                    | 86400                         |
      | 9  | NULL                | NULL             | NULL              | NULL                  | NULL                 | NULL                  | NULL                  | NULL                     | NULL                          |

  Scenario: TIMESTAMP month interval arithmetic uses the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        CAST(ltz + INTERVAL 1 MONTH AS STRING) AS plus_month,
        unix_seconds(ltz + INTERVAL 1 MONTH) - unix_seconds(ltz) AS plus_month_seconds,
        unix_seconds(ltz - INTERVAL 1 MONTH) - unix_seconds(ltz) AS minus_month_seconds,
        unix_seconds(ltz + INTERVAL '1-1' YEAR TO MONTH) - unix_seconds(ltz) AS plus_year_month_seconds,
        unix_seconds(ltz + make_interval(0, 1, 0, 1, 2)) - unix_seconds(ltz) AS plus_calendar_seconds,
        unix_seconds(timestampadd(MONTH, 1, ltz)) - unix_seconds(ltz) AS timestampadd_month_seconds
      FROM timestamp_interval_session_zone
      ORDER BY id
      """
    Then query result ordered
      | id | plus_month          | plus_month_seconds | minus_month_seconds | plus_year_month_seconds | plus_calendar_seconds | timestampadd_month_seconds |
      | 0  | 2020-11-30 01:30:00 | 2595600            | -2678400            | 34131600                | 2689200               | 2595600                    |
      | 1  | 2020-12-01 01:30:00 | 2595600            | -2678400            | 34131600                | 2689200               | 2595600                    |
      | 2  | 2020-12-01 01:30:00 | 2592000            | -2682000            | 34128000                | 2685600               | 2592000                    |
      | 3  | 2020-04-07 02:30:00 | 2674800            | -2505600            | 34210800                | 2768400               | 2674800                    |
      | 4  | 2020-03-08 03:30:00 | 2505600            | -2678400            | 34041600                | 2599200               | 2505600                    |
      | 5  | 2020-02-29 12:00:00 | 2505600            | -2678400            | 34041600                | 2599200               | 2505600                    |
      | 6  | 2020-03-29 12:00:00 | 2502000            | -2678400            | 34038000                | 2595600               | 2502000                    |
      | 7  | 2020-02-15 01:30:00 | 2678400            | -2678400            | 34300800                | 2772000               | 2678400                    |
      | 8  | 2018-12-02 20:30:00 | 2595600            | -2678400            | 34131600                | 2689200               | 2595600                    |
      | 9  | NULL                | NULL               | NULL                | NULL                    | NULL                  | NULL                       |

  Scenario: TIMESTAMP month interval arithmetic uses the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        id,
        CAST(ltz + INTERVAL 1 MONTH AS STRING) AS plus_month,
        unix_seconds(ltz + INTERVAL 1 MONTH) - unix_seconds(ltz) AS plus_month_seconds,
        unix_seconds(ltz - INTERVAL 1 MONTH) - unix_seconds(ltz) AS minus_month_seconds,
        unix_seconds(ltz + INTERVAL '1-1' YEAR TO MONTH) - unix_seconds(ltz) AS plus_year_month_seconds,
        unix_seconds(ltz + make_interval(0, 1, 0, 1, 2)) - unix_seconds(ltz) AS plus_calendar_seconds,
        unix_seconds(timestampadd(MONTH, 1, ltz)) - unix_seconds(ltz) AS timestampadd_month_seconds
      FROM timestamp_interval_session_zone
      ORDER BY id
      """
    Then query result ordered
      | id | plus_month          | plus_month_seconds | minus_month_seconds | plus_year_month_seconds | plus_calendar_seconds | timestampadd_month_seconds |
      | 0  | 2020-11-30 16:30:00 | 2592000            | -2678400            | 34128000                | 2685600               | 2592000                    |
      | 1  | 2020-12-01 16:30:00 | 2592000            | -2678400            | 34128000                | 2685600               | 2592000                    |
      | 2  | 2020-12-01 17:30:00 | 2592000            | -2678400            | 34128000                | 2685600               | 2592000                    |
      | 3  | 2020-04-07 18:30:00 | 2678400            | -2505600            | 34214400                | 2772000               | 2678400                    |
      | 4  | 2020-03-08 18:30:00 | 2505600            | -2678400            | 34041600                | 2599200               | 2505600                    |
      | 5  | 2020-03-01 04:00:00 | 2505600            | -2678400            | 34041600                | 2599200               | 2505600                    |
      | 6  | 2020-04-01 04:00:00 | 2678400            | -2505600            | 34214400                | 2772000               | 2678400                    |
      | 7  | 2020-02-15 17:30:00 | 2678400            | -2678400            | 34300800                | 2772000               | 2678400                    |
      | 8  | 2018-12-03 11:30:00 | 2592000            | -2678400            | 34128000                | 2685600               | 2592000                    |
      | 9  | NULL                | NULL               | NULL                | NULL                    | NULL                  | NULL                       |

  Scenario: TIMESTAMP month interval arithmetic uses the session time zone (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        id,
        CAST(ltz + INTERVAL 1 MONTH AS STRING) AS plus_month,
        unix_seconds(ltz + INTERVAL 1 MONTH) - unix_seconds(ltz) AS plus_month_seconds,
        unix_seconds(ltz - INTERVAL 1 MONTH) - unix_seconds(ltz) AS minus_month_seconds,
        unix_seconds(ltz + INTERVAL '1-1' YEAR TO MONTH) - unix_seconds(ltz) AS plus_year_month_seconds,
        unix_seconds(ltz + make_interval(0, 1, 0, 1, 2)) - unix_seconds(ltz) AS plus_calendar_seconds,
        unix_seconds(timestampadd(MONTH, 1, ltz)) - unix_seconds(ltz) AS timestampadd_month_seconds
      FROM timestamp_interval_session_zone
      ORDER BY id
      """
    Then query result ordered
      | id | plus_month          | plus_month_seconds | minus_month_seconds | plus_year_month_seconds | plus_calendar_seconds | timestampadd_month_seconds |
      | 0  | 2020-11-30 14:00:00 | 2592000            | -2678400            | 34128000                | 2685600               | 2592000                    |
      | 1  | 2020-12-01 14:00:00 | 2592000            | -2678400            | 34128000                | 2685600               | 2592000                    |
      | 2  | 2020-12-01 15:00:00 | 2592000            | -2678400            | 34128000                | 2685600               | 2592000                    |
      | 3  | 2020-04-07 16:00:00 | 2678400            | -2505600            | 34214400                | 2772000               | 2678400                    |
      | 4  | 2020-03-08 16:00:00 | 2505600            | -2678400            | 34041600                | 2599200               | 2505600                    |
      | 5  | 2020-03-01 01:30:00 | 2505600            | -2678400            | 34041600                | 2599200               | 2505600                    |
      | 6  | 2020-04-01 01:30:00 | 2678400            | -2505600            | 34214400                | 2772000               | 2678400                    |
      | 7  | 2020-02-15 15:00:00 | 2678400            | -2678400            | 34300800                | 2772000               | 2678400                    |
      | 8  | 2018-12-03 09:00:00 | 2592000            | -2678400            | 34128000                | 2685600               | 2592000                    |
      | 9  | NULL                | NULL               | NULL                | NULL                    | NULL                  | NULL                       |

  Scenario: TIMESTAMP month interval arithmetic uses the session time zone (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        id,
        CAST(ltz + INTERVAL 1 MONTH AS STRING) AS plus_month,
        unix_seconds(ltz + INTERVAL 1 MONTH) - unix_seconds(ltz) AS plus_month_seconds,
        unix_seconds(ltz - INTERVAL 1 MONTH) - unix_seconds(ltz) AS minus_month_seconds,
        unix_seconds(ltz + INTERVAL '1-1' YEAR TO MONTH) - unix_seconds(ltz) AS plus_year_month_seconds,
        unix_seconds(ltz + make_interval(0, 1, 0, 1, 2)) - unix_seconds(ltz) AS plus_calendar_seconds,
        unix_seconds(timestampadd(MONTH, 1, ltz)) - unix_seconds(ltz) AS timestampadd_month_seconds
      FROM timestamp_interval_session_zone
      ORDER BY id
      """
    Then query result ordered
      | id | plus_month          | plus_month_seconds | minus_month_seconds | plus_year_month_seconds | plus_calendar_seconds | timestampadd_month_seconds |
      | 0  | 2020-11-30 08:30:00 | 2592000            | -2678400            | 34128000                | 2685600               | 2592000                    |
      | 1  | 2020-12-01 08:30:00 | 2592000            | -2678400            | 34128000                | 2685600               | 2592000                    |
      | 2  | 2020-12-01 09:30:00 | 2592000            | -2678400            | 34128000                | 2685600               | 2592000                    |
      | 3  | 2020-04-07 10:30:00 | 2678400            | -2505600            | 34214400                | 2772000               | 2678400                    |
      | 4  | 2020-03-08 10:30:00 | 2505600            | -2678400            | 34041600                | 2599200               | 2505600                    |
      | 5  | 2020-02-29 20:00:00 | 2505600            | -2678400            | 34041600                | 2599200               | 2505600                    |
      | 6  | 2020-03-29 20:00:00 | 2505600            | -2678400            | 34041600                | 2599200               | 2505600                    |
      | 7  | 2020-02-15 09:30:00 | 2678400            | -2678400            | 34300800                | 2772000               | 2678400                    |
      | 8  | 2018-12-03 03:30:00 | 2592000            | -2678400            | 34128000                | 2685600               | 2592000                    |
      | 9  | NULL                | NULL               | NULL                | NULL                    | NULL                  | NULL                       |

  Scenario: TIMESTAMP literal interval arithmetic uses the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        CAST(TIMESTAMP '2020-02-08 02:30:00' + INTERVAL 1 MONTH AS STRING) AS month_into_gap,
        CAST(TIMESTAMP '2020-03-07 02:30:00' + INTERVAL 1 DAY AS STRING) AS day_into_gap,
        unix_seconds(TIMESTAMP '2020-10-31 01:30:00' + INTERVAL 1 DAY) - unix_seconds(TIMESTAMP '2020-10-31 01:30:00') AS into_overlap_from_daylight,
        unix_seconds(TIMESTAMP '2020-01-15 01:30:00' + INTERVAL 291 DAYS) - unix_seconds(TIMESTAMP '2020-01-15 01:30:00') AS into_overlap_from_standard,
        unix_seconds(TIMESTAMP '2020-11-01 01:30:00' + INTERVAL 1 DAY) - unix_seconds(TIMESTAMP '2020-11-01 01:30:00') AS from_overlap_earlier,
        unix_seconds(TIMESTAMP '2020-11-01 01:30:00-08:00' - INTERVAL 1 DAY) - unix_seconds(TIMESTAMP '2020-11-01 01:30:00-08:00') AS from_overlap_later
      """
    Then query result ordered
      | month_into_gap      | day_into_gap        | into_overlap_from_daylight | into_overlap_from_standard | from_overlap_earlier | from_overlap_later |
      | 2020-03-08 03:30:00 | 2020-03-08 03:30:00 | 86400                      | 25142400                   | 90000                | -90000             |

  Scenario: TIMESTAMPDIFF over TIMESTAMP interval arithmetic counts the session wall-clock time
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        timestampdiff(DAY, ts, ts + INTERVAL 1 DAY) AS days,
        timestampdiff(HOUR, ts, ts + INTERVAL 1 DAY) AS hours,
        CAST(ts + INTERVAL '1 02:00:00' DAY TO SECOND AS STRING) AS plus_day_to_second,
        CAST(ts - INTERVAL '1 02:00:00' DAY TO SECOND AS STRING) AS minus_day_to_second,
        CAST(ts + make_dt_interval(1, 2, 0, 0) AS STRING) AS plus_make_dt_interval
      FROM VALUES
        (0, TIMESTAMP '2020-11-01 01:30:00'),
        (1, TIMESTAMP '2020-03-07 12:00:00'),
        (2, TIMESTAMP '2020-03-09 01:00:00')
        AS t(id, ts)
      ORDER BY id
      """
    Then query result ordered
      | id | days | hours | plus_day_to_second  | minus_day_to_second | plus_make_dt_interval |
      | 0  | 1    | 24    | 2020-11-02 03:30:00 | 2020-10-30 23:30:00 | 2020-11-02 03:30:00   |
      | 1  | 1    | 24    | 2020-03-08 14:00:00 | 2020-03-06 10:00:00 | 2020-03-08 14:00:00   |
      | 2  | 1    | 24    | 2020-03-10 03:00:00 | 2020-03-07 23:00:00 | 2020-03-10 03:00:00   |

  Scenario: TIMESTAMPADD adds calendar units in the session time zone and keeps TIMESTAMP_NTZ
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        CAST(timestampadd(WEEK, 1, ts) AS STRING) AS plus_week,
        CAST(timestampadd(QUARTER, 1, ts) AS STRING) AS plus_quarter,
        CAST(timestampadd(YEAR, -1, ts) AS STRING) AS minus_year,
        CAST(timestampadd(DAY, -1, ts) AS STRING) AS minus_day,
        CAST(timestampadd(HOUR, 25, ts) AS STRING) AS plus_25_hours,
        typeof(timestampadd(DAY, 1, ntz)) AS ntz_type,
        CAST(timestampadd(DAY, 1, ntz) AS STRING) AS ntz_plus_day
      FROM VALUES
        (0, TIMESTAMP '2020-03-07 02:30:00', TIMESTAMP_NTZ '2020-03-07 02:30:00'),
        (1, TIMESTAMP '2020-10-31 01:30:00', TIMESTAMP_NTZ '2020-10-31 01:30:00')
        AS t(id, ts, ntz)
      ORDER BY id
      """
    Then query result ordered
      | id | plus_week           | plus_quarter        | minus_year          | minus_day           | plus_25_hours       | ntz_type      | ntz_plus_day        |
      | 0  | 2020-03-14 02:30:00 | 2020-06-07 02:30:00 | 2019-03-07 02:30:00 | 2020-03-06 02:30:00 | 2020-03-08 04:30:00 | timestamp_ntz | 2020-03-08 02:30:00 |
      | 1  | 2020-11-07 01:30:00 | 2021-01-31 01:30:00 | 2019-10-31 01:30:00 | 2020-10-30 01:30:00 | 2020-11-01 01:30:00 | timestamp_ntz | 2020-11-01 01:30:00 |

  Scenario: TIMESTAMP subtraction counts the session wall-clock time
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT id, CAST(b - a AS STRING) AS difference
      FROM VALUES
        (0, TIMESTAMP '2020-03-07 12:00:00', TIMESTAMP '2020-03-08 12:00:00'),
        (1, TIMESTAMP '2024-11-03 01:30:00', TIMESTAMP '2024-11-03 02:30:00'),
        (2, TIMESTAMP '2020-03-07 12:00:00', TIMESTAMP '2020-03-07 12:00:00' + INTERVAL 1 DAY),
        (3, TIMESTAMP '2020-06-01 00:00:00', TIMESTAMP '2020-05-01 00:00:00')
        AS t(id, a, b)
      ORDER BY id
      """
    Then query result ordered
      | id | difference                            |
      | 0  | INTERVAL '1 00:00:00' DAY TO SECOND   |
      | 1  | INTERVAL '0 01:00:00' DAY TO SECOND   |
      | 2  | INTERVAL '1 00:00:00' DAY TO SECOND   |
      | 3  | INTERVAL '-31 00:00:00' DAY TO SECOND |

  Scenario: TIMESTAMP interval arithmetic evaluates a nondeterministic value once
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT count(DISTINCT CAST(t AS STRING)) AS values
      FROM (
        SELECT
          (CASE WHEN rand(0) < 0.5 THEN TIMESTAMP '2020-01-01 00:00:00' ELSE TIMESTAMP '2020-06-01 00:00:00' END)
            + INTERVAL 1 DAY AS t
        FROM range(100)
      )
      """
    Then query result
      | values |
      | 2      |
