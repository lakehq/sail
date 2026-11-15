Feature: Parquet TIMESTAMP values display and convert to DATE in the session time zone
  Spark shows, formats and casts TIMESTAMP values read from Parquet, which are adjusted to UTC, in the session time zone.

  Background:
    Given variable location for temporary directory timestamp_session_display
    Given final statement
      """
      DROP TABLE IF EXISTS timestamp_session_display
      """
    Given config spark.sql.session.timeZone = UTC
    Given statement template
      """
      CREATE TABLE timestamp_session_display (id INT, ltz TIMESTAMP) USING parquet LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO timestamp_session_display VALUES
        (0, TIMESTAMP '2020-06-14 06:59:59'),
        (1, TIMESTAMP '2020-06-14 07:00:00'),
        (2, TIMESTAMP '2020-06-13 15:59:59'),
        (3, TIMESTAMP '2020-06-13 16:00:00'),
        (4, TIMESTAMP '2020-03-08 09:59:59'),
        (5, TIMESTAMP '2020-03-08 10:00:00'),
        (6, TIMESTAMP '2020-11-01 08:30:00'),
        (7, TIMESTAMP '2020-11-01 09:30:00'),
        (8, TIMESTAMP '2020-06-13 18:29:59.999999'),
        (9, TIMESTAMP '1900-01-01 08:00:00'),
        (10, NULL)
      """

  Scenario: Parquet TIMESTAMP values display and convert to DATE in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        ltz,
        CAST(ltz AS STRING) AS string,
        CAST(ltz AS DATE) AS date,
        to_date(ltz) AS to_date
      FROM timestamp_session_display
      ORDER BY id
      """
    Then query result ordered
      | id | ltz                        | string                     | date       | to_date    |
      | 0  | 2020-06-13 23:59:59        | 2020-06-13 23:59:59        | 2020-06-13 | 2020-06-13 |
      | 1  | 2020-06-14 00:00:00        | 2020-06-14 00:00:00        | 2020-06-14 | 2020-06-14 |
      | 2  | 2020-06-13 08:59:59        | 2020-06-13 08:59:59        | 2020-06-13 | 2020-06-13 |
      | 3  | 2020-06-13 09:00:00        | 2020-06-13 09:00:00        | 2020-06-13 | 2020-06-13 |
      | 4  | 2020-03-08 01:59:59        | 2020-03-08 01:59:59        | 2020-03-08 | 2020-03-08 |
      | 5  | 2020-03-08 03:00:00        | 2020-03-08 03:00:00        | 2020-03-08 | 2020-03-08 |
      | 6  | 2020-11-01 01:30:00        | 2020-11-01 01:30:00        | 2020-11-01 | 2020-11-01 |
      | 7  | 2020-11-01 01:30:00        | 2020-11-01 01:30:00        | 2020-11-01 | 2020-11-01 |
      | 8  | 2020-06-13 11:29:59.999999 | 2020-06-13 11:29:59.999999 | 2020-06-13 | 2020-06-13 |
      | 9  | 1900-01-01 00:00:00        | 1900-01-01 00:00:00        | 1900-01-01 | 1900-01-01 |
      | 10 | NULL                       | NULL                       | NULL       | NULL       |

  Scenario: Parquet TIMESTAMP values display and convert to DATE in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        id,
        ltz,
        CAST(ltz AS STRING) AS string,
        CAST(ltz AS DATE) AS date,
        to_date(ltz) AS to_date
      FROM timestamp_session_display
      ORDER BY id
      """
    Then query result ordered
      | id | ltz                        | string                     | date       | to_date    |
      | 0  | 2020-06-14 14:59:59        | 2020-06-14 14:59:59        | 2020-06-14 | 2020-06-14 |
      | 1  | 2020-06-14 15:00:00        | 2020-06-14 15:00:00        | 2020-06-14 | 2020-06-14 |
      | 2  | 2020-06-13 23:59:59        | 2020-06-13 23:59:59        | 2020-06-13 | 2020-06-13 |
      | 3  | 2020-06-14 00:00:00        | 2020-06-14 00:00:00        | 2020-06-14 | 2020-06-14 |
      | 4  | 2020-03-08 17:59:59        | 2020-03-08 17:59:59        | 2020-03-08 | 2020-03-08 |
      | 5  | 2020-03-08 18:00:00        | 2020-03-08 18:00:00        | 2020-03-08 | 2020-03-08 |
      | 6  | 2020-11-01 16:30:00        | 2020-11-01 16:30:00        | 2020-11-01 | 2020-11-01 |
      | 7  | 2020-11-01 17:30:00        | 2020-11-01 17:30:00        | 2020-11-01 | 2020-11-01 |
      | 8  | 2020-06-14 02:29:59.999999 | 2020-06-14 02:29:59.999999 | 2020-06-14 | 2020-06-14 |
      | 9  | 1900-01-01 16:05:43        | 1900-01-01 16:05:43        | 1900-01-01 | 1900-01-01 |
      | 10 | NULL                       | NULL                       | NULL       | NULL       |

  Scenario: Parquet TIMESTAMP values display and convert to DATE in the session time zone (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        id,
        ltz,
        CAST(ltz AS STRING) AS string,
        CAST(ltz AS DATE) AS date,
        to_date(ltz) AS to_date
      FROM timestamp_session_display
      ORDER BY id
      """
    Then query result ordered
      | id | ltz                        | string                     | date       | to_date    |
      | 0  | 2020-06-14 12:29:59        | 2020-06-14 12:29:59        | 2020-06-14 | 2020-06-14 |
      | 1  | 2020-06-14 12:30:00        | 2020-06-14 12:30:00        | 2020-06-14 | 2020-06-14 |
      | 2  | 2020-06-13 21:29:59        | 2020-06-13 21:29:59        | 2020-06-13 | 2020-06-13 |
      | 3  | 2020-06-13 21:30:00        | 2020-06-13 21:30:00        | 2020-06-13 | 2020-06-13 |
      | 4  | 2020-03-08 15:29:59        | 2020-03-08 15:29:59        | 2020-03-08 | 2020-03-08 |
      | 5  | 2020-03-08 15:30:00        | 2020-03-08 15:30:00        | 2020-03-08 | 2020-03-08 |
      | 6  | 2020-11-01 14:00:00        | 2020-11-01 14:00:00        | 2020-11-01 | 2020-11-01 |
      | 7  | 2020-11-01 15:00:00        | 2020-11-01 15:00:00        | 2020-11-01 | 2020-11-01 |
      | 8  | 2020-06-13 23:59:59.999999 | 2020-06-13 23:59:59.999999 | 2020-06-13 | 2020-06-13 |
      | 9  | 1900-01-01 13:30:00        | 1900-01-01 13:30:00        | 1900-01-01 | 1900-01-01 |
      | 10 | NULL                       | NULL                       | NULL       | NULL       |

  Scenario: Parquet TIMESTAMP values display and convert to DATE in the session time zone (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        id,
        ltz,
        CAST(ltz AS STRING) AS string,
        CAST(ltz AS DATE) AS date,
        to_date(ltz) AS to_date
      FROM timestamp_session_display
      ORDER BY id
      """
    Then query result ordered
      | id | ltz                        | string                     | date       | to_date    |
      | 0  | 2020-06-14 06:59:59        | 2020-06-14 06:59:59        | 2020-06-14 | 2020-06-14 |
      | 1  | 2020-06-14 07:00:00        | 2020-06-14 07:00:00        | 2020-06-14 | 2020-06-14 |
      | 2  | 2020-06-13 15:59:59        | 2020-06-13 15:59:59        | 2020-06-13 | 2020-06-13 |
      | 3  | 2020-06-13 16:00:00        | 2020-06-13 16:00:00        | 2020-06-13 | 2020-06-13 |
      | 4  | 2020-03-08 09:59:59        | 2020-03-08 09:59:59        | 2020-03-08 | 2020-03-08 |
      | 5  | 2020-03-08 10:00:00        | 2020-03-08 10:00:00        | 2020-03-08 | 2020-03-08 |
      | 6  | 2020-11-01 08:30:00        | 2020-11-01 08:30:00        | 2020-11-01 | 2020-11-01 |
      | 7  | 2020-11-01 09:30:00        | 2020-11-01 09:30:00        | 2020-11-01 | 2020-11-01 |
      | 8  | 2020-06-13 18:29:59.999999 | 2020-06-13 18:29:59.999999 | 2020-06-13 | 2020-06-13 |
      | 9  | 1900-01-01 08:00:00        | 1900-01-01 08:00:00        | 1900-01-01 | 1900-01-01 |
      | 10 | NULL                       | NULL                       | NULL       | NULL       |
