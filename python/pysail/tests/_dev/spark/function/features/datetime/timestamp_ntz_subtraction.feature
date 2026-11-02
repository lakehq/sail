Feature: TIMESTAMP and TIMESTAMP_NTZ subtraction uses the session wall clock
  Spark casts the TIMESTAMP side of a subtraction with a TIMESTAMP_NTZ value to TIMESTAMP_NTZ in the session time
  zone, so the result is the difference between the wall-clock times, even across a DST change.

  Background:
    Given variable location for temporary directory timestamp_ntz_subtraction
    Given final statement
      """
      DROP TABLE IF EXISTS timestamp_ntz_subtraction
      """
    Given config spark.sql.session.timeZone = UTC
    Given statement template
      """
      CREATE TABLE timestamp_ntz_subtraction (id INT, ntz TIMESTAMP_NTZ, s STRING) USING parquet LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO timestamp_ntz_subtraction VALUES
        (0, TIMESTAMP_NTZ '2020-06-14 12:00:00', '2020-06-14 10:00:00'),
        (1, TIMESTAMP_NTZ '2020-03-08 02:30:00', '2020-03-08 02:30:00'),
        (2, TIMESTAMP_NTZ '2020-11-01 01:30:00', '2020-11-01 01:30:00'),
        (3, TIMESTAMP_NTZ '2020-11-01 01:30:00', '2020-11-01 01:30:00-08:00'),
        (4, TIMESTAMP_NTZ '2020-03-08 04:00:00', '2020-03-08 01:00:00'),
        (5, TIMESTAMP_NTZ '2020-11-01 03:00:00', '2020-11-01 00:00:00'),
        (6, NULL, '2020-06-14 10:00:00')
      """

  Scenario: TIMESTAMP and TIMESTAMP_NTZ subtraction uses the session wall clock (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        CAST(s AS TIMESTAMP) - ntz AS timestamp_minus_ntz,
        ntz - CAST(s AS TIMESTAMP) AS ntz_minus_timestamp,
        TIMESTAMP '2020-03-08 02:30:00' - ntz AS literal_minus_ntz
      FROM timestamp_ntz_subtraction
      ORDER BY id
      """
    Then query result ordered
      | id | timestamp_minus_ntz                  | ntz_minus_timestamp                  | literal_minus_ntz                      |
      | 0  | INTERVAL '-0 02:00:00' DAY TO SECOND | INTERVAL '0 02:00:00' DAY TO SECOND  | INTERVAL '-98 08:30:00' DAY TO SECOND  |
      | 1  | INTERVAL '0 01:00:00' DAY TO SECOND  | INTERVAL '-0 01:00:00' DAY TO SECOND | INTERVAL '0 01:00:00' DAY TO SECOND    |
      | 2  | INTERVAL '0 00:00:00' DAY TO SECOND  | INTERVAL '0 00:00:00' DAY TO SECOND  | INTERVAL '-237 22:00:00' DAY TO SECOND |
      | 3  | INTERVAL '0 00:00:00' DAY TO SECOND  | INTERVAL '0 00:00:00' DAY TO SECOND  | INTERVAL '-237 22:00:00' DAY TO SECOND |
      | 4  | INTERVAL '-0 03:00:00' DAY TO SECOND | INTERVAL '0 03:00:00' DAY TO SECOND  | INTERVAL '-0 00:30:00' DAY TO SECOND   |
      | 5  | INTERVAL '-0 03:00:00' DAY TO SECOND | INTERVAL '0 03:00:00' DAY TO SECOND  | INTERVAL '-237 23:30:00' DAY TO SECOND |
      | 6  | NULL                                 | NULL                                 | NULL                                   |

  Scenario: TIMESTAMP and TIMESTAMP_NTZ subtraction uses the session wall clock (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        id,
        CAST(s AS TIMESTAMP) - ntz AS timestamp_minus_ntz,
        ntz - CAST(s AS TIMESTAMP) AS ntz_minus_timestamp,
        TIMESTAMP '2020-03-08 02:30:00' - ntz AS literal_minus_ntz
      FROM timestamp_ntz_subtraction
      ORDER BY id
      """
    Then query result ordered
      | id | timestamp_minus_ntz                  | ntz_minus_timestamp                  | literal_minus_ntz                      |
      | 0  | INTERVAL '-0 02:00:00' DAY TO SECOND | INTERVAL '0 02:00:00' DAY TO SECOND  | INTERVAL '-98 09:30:00' DAY TO SECOND  |
      | 1  | INTERVAL '0 00:00:00' DAY TO SECOND  | INTERVAL '0 00:00:00' DAY TO SECOND  | INTERVAL '0 00:00:00' DAY TO SECOND    |
      | 2  | INTERVAL '0 00:00:00' DAY TO SECOND  | INTERVAL '0 00:00:00' DAY TO SECOND  | INTERVAL '-237 23:00:00' DAY TO SECOND |
      | 3  | INTERVAL '0 16:00:00' DAY TO SECOND  | INTERVAL '-0 16:00:00' DAY TO SECOND | INTERVAL '-237 23:00:00' DAY TO SECOND |
      | 4  | INTERVAL '-0 03:00:00' DAY TO SECOND | INTERVAL '0 03:00:00' DAY TO SECOND  | INTERVAL '-0 01:30:00' DAY TO SECOND   |
      | 5  | INTERVAL '-0 03:00:00' DAY TO SECOND | INTERVAL '0 03:00:00' DAY TO SECOND  | INTERVAL '-238 00:30:00' DAY TO SECOND |
      | 6  | NULL                                 | NULL                                 | NULL                                   |

  Scenario: TIMESTAMP and TIMESTAMP_NTZ subtraction uses the session wall clock (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        id,
        CAST(s AS TIMESTAMP) - ntz AS timestamp_minus_ntz,
        ntz - CAST(s AS TIMESTAMP) AS ntz_minus_timestamp,
        TIMESTAMP '2020-03-08 02:30:00' - ntz AS literal_minus_ntz
      FROM timestamp_ntz_subtraction
      ORDER BY id
      """
    Then query result ordered
      | id | timestamp_minus_ntz                  | ntz_minus_timestamp                  | literal_minus_ntz                      |
      | 0  | INTERVAL '-0 02:00:00' DAY TO SECOND | INTERVAL '0 02:00:00' DAY TO SECOND  | INTERVAL '-98 09:30:00' DAY TO SECOND  |
      | 1  | INTERVAL '0 00:00:00' DAY TO SECOND  | INTERVAL '0 00:00:00' DAY TO SECOND  | INTERVAL '0 00:00:00' DAY TO SECOND    |
      | 2  | INTERVAL '0 00:00:00' DAY TO SECOND  | INTERVAL '0 00:00:00' DAY TO SECOND  | INTERVAL '-237 23:00:00' DAY TO SECOND |
      | 3  | INTERVAL '0 13:30:00' DAY TO SECOND  | INTERVAL '-0 13:30:00' DAY TO SECOND | INTERVAL '-237 23:00:00' DAY TO SECOND |
      | 4  | INTERVAL '-0 03:00:00' DAY TO SECOND | INTERVAL '0 03:00:00' DAY TO SECOND  | INTERVAL '-0 01:30:00' DAY TO SECOND   |
      | 5  | INTERVAL '-0 03:00:00' DAY TO SECOND | INTERVAL '0 03:00:00' DAY TO SECOND  | INTERVAL '-238 00:30:00' DAY TO SECOND |
      | 6  | NULL                                 | NULL                                 | NULL                                   |

  Scenario: TIMESTAMP and TIMESTAMP_NTZ subtraction uses the session wall clock (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        id,
        CAST(s AS TIMESTAMP) - ntz AS timestamp_minus_ntz,
        ntz - CAST(s AS TIMESTAMP) AS ntz_minus_timestamp,
        TIMESTAMP '2020-03-08 02:30:00' - ntz AS literal_minus_ntz
      FROM timestamp_ntz_subtraction
      ORDER BY id
      """
    Then query result ordered
      | id | timestamp_minus_ntz                  | ntz_minus_timestamp                  | literal_minus_ntz                      |
      | 0  | INTERVAL '-0 02:00:00' DAY TO SECOND | INTERVAL '0 02:00:00' DAY TO SECOND  | INTERVAL '-98 09:30:00' DAY TO SECOND  |
      | 1  | INTERVAL '0 00:00:00' DAY TO SECOND  | INTERVAL '0 00:00:00' DAY TO SECOND  | INTERVAL '0 00:00:00' DAY TO SECOND    |
      | 2  | INTERVAL '0 00:00:00' DAY TO SECOND  | INTERVAL '0 00:00:00' DAY TO SECOND  | INTERVAL '-237 23:00:00' DAY TO SECOND |
      | 3  | INTERVAL '0 08:00:00' DAY TO SECOND  | INTERVAL '-0 08:00:00' DAY TO SECOND | INTERVAL '-237 23:00:00' DAY TO SECOND |
      | 4  | INTERVAL '-0 03:00:00' DAY TO SECOND | INTERVAL '0 03:00:00' DAY TO SECOND  | INTERVAL '-0 01:30:00' DAY TO SECOND   |
      | 5  | INTERVAL '-0 03:00:00' DAY TO SECOND | INTERVAL '0 03:00:00' DAY TO SECOND  | INTERVAL '-238 00:30:00' DAY TO SECOND |
      | 6  | NULL                                 | NULL                                 | NULL                                   |

  Scenario: TIMESTAMP and TIMESTAMP_NTZ literal subtraction uses the session wall clock (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        TIMESTAMP '2020-03-08 01:00:00' - TIMESTAMP_NTZ '2020-03-08 04:00:00' AS spring_forward,
        TIMESTAMP_NTZ '2020-11-01 03:00:00' - TIMESTAMP '2020-11-01 00:00:00' AS fall_back,
        TIMESTAMP '2020-11-01 01:30:00-08:00' - TIMESTAMP_NTZ '2020-11-01 01:30:00' AS later_offset,
        TIMESTAMP '2020-03-08 02:30:00' - TIMESTAMP_NTZ '2020-03-08 02:30:00' AS gap
      """
    Then query result ordered
      | spring_forward                       | fall_back                           | later_offset                        | gap                                 |
      | INTERVAL '-0 03:00:00' DAY TO SECOND | INTERVAL '0 03:00:00' DAY TO SECOND | INTERVAL '0 00:00:00' DAY TO SECOND | INTERVAL '0 01:00:00' DAY TO SECOND |

  Scenario: TIMESTAMP and TIMESTAMP_NTZ literal subtraction uses the session wall clock (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        TIMESTAMP '2020-03-08 01:00:00' - TIMESTAMP_NTZ '2020-03-08 04:00:00' AS spring_forward,
        TIMESTAMP_NTZ '2020-11-01 03:00:00' - TIMESTAMP '2020-11-01 00:00:00' AS fall_back,
        TIMESTAMP '2020-11-01 01:30:00-08:00' - TIMESTAMP_NTZ '2020-11-01 01:30:00' AS later_offset,
        TIMESTAMP '2020-03-08 02:30:00' - TIMESTAMP_NTZ '2020-03-08 02:30:00' AS gap
      """
    Then query result ordered
      | spring_forward                       | fall_back                           | later_offset                        | gap                                 |
      | INTERVAL '-0 03:00:00' DAY TO SECOND | INTERVAL '0 03:00:00' DAY TO SECOND | INTERVAL '0 16:00:00' DAY TO SECOND | INTERVAL '0 00:00:00' DAY TO SECOND |
