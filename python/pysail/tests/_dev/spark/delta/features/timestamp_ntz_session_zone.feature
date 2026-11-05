Feature: Delta Lake MERGE and UPDATE cast TIMESTAMP to TIMESTAMP_NTZ in the session time zone

  Background:
    Given variable location for temporary directory delta_timestamp_ntz_session_zone
    Given final statement
      """
      DROP TABLE IF EXISTS delta_timestamp_ntz_session_zone
      """
    Given config spark.sql.session.timeZone = Asia/Shanghai
    Given statement template
      """
      CREATE TABLE delta_timestamp_ntz_session_zone (id INT, v TIMESTAMP_NTZ)
      USING DELTA LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO delta_timestamp_ntz_session_zone VALUES (1, TIMESTAMP_NTZ '2000-01-01 00:00:00')
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW delta_timestamp_ntz_session_zone_source AS
      SELECT 1 AS id, coalesce(TIMESTAMP_NTZ '2020-06-13 12:00:00', TIMESTAMP '2020-06-13 20:00:00') AS v
      UNION ALL
      SELECT 2 AS id, TIMESTAMP '2020-06-13 20:00:00' AS v
      """

  Scenario: MERGE assignments cast TIMESTAMP to TIMESTAMP_NTZ in the session time zone
    Given statement
      """
      MERGE INTO delta_timestamp_ntz_session_zone AS t
      USING delta_timestamp_ntz_session_zone_source AS s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET v = s.v
      WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)
      """
    When query
      """
      SELECT id, CAST(v AS STRING) AS v FROM delta_timestamp_ntz_session_zone ORDER BY id
      """
    Then query result ordered
      | id | v                   |
      | 1  | 2020-06-13 12:00:00 |
      | 2  | 2020-06-13 20:00:00 |

  Scenario: MERGE star actions cast TIMESTAMP to TIMESTAMP_NTZ in the session time zone
    Given statement
      """
      MERGE INTO delta_timestamp_ntz_session_zone AS t
      USING delta_timestamp_ntz_session_zone_source AS s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
      """
    When query
      """
      SELECT id, CAST(v AS STRING) AS v FROM delta_timestamp_ntz_session_zone ORDER BY id
      """
    Then query result ordered
      | id | v                   |
      | 1  | 2020-06-13 12:00:00 |
      | 2  | 2020-06-13 20:00:00 |

  Scenario: UPDATE casts TIMESTAMP to TIMESTAMP_NTZ in the session time zone
    Given statement
      """
      UPDATE delta_timestamp_ntz_session_zone
      SET v = coalesce(TIMESTAMP_NTZ '2020-06-13 12:00:00', TIMESTAMP '2020-06-13 20:00:00')
      WHERE id = 1
      """
    When query
      """
      SELECT id, CAST(v AS STRING) AS v FROM delta_timestamp_ntz_session_zone ORDER BY id
      """
    Then query result ordered
      | id | v                   |
      | 1  | 2020-06-13 12:00:00 |
