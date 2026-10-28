Feature: Iceberg writes TIMESTAMP values in any session time zone
  Sail labels TIMESTAMP values with the session time zone, while Iceberg stores them as UTC instants.

  Rule: TIMESTAMP values keep their instants
    Background:
      Given variable location for temporary directory iceberg_timestamp_session_time_zone
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_timestamp_session_time_zone
        """

    Scenario: CTAS writes a TIMESTAMP value in a non-UTC session
      Given config spark.sql.session.timeZone = Asia/Shanghai
      Given statement template
        """
        CREATE TABLE iceberg_timestamp_session_time_zone
        USING iceberg
        LOCATION {{ location.uri }}
        AS SELECT TIMESTAMP '2020-06-14 12:00:00' AS ts
        """
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(ts AS STRING) AS ts FROM iceberg_timestamp_session_time_zone
        """
      Then query result
        | ts                  |
        | 2020-06-14 04:00:00 |

    Scenario: INSERT writes a TIMESTAMP value in a non-UTC session
      Given statement template
        """
        CREATE TABLE iceberg_timestamp_session_time_zone (ts TIMESTAMP)
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Given config spark.sql.session.timeZone = America/Los_Angeles
      Given statement
        """
        INSERT INTO iceberg_timestamp_session_time_zone VALUES (TIMESTAMP '2020-06-14 12:00:00')
        """
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(ts AS STRING) AS ts FROM iceberg_timestamp_session_time_zone
        """
      Then query result
        | ts                  |
        | 2020-06-14 19:00:00 |

    Scenario: CTAS writes nested TIMESTAMP values in a non-UTC session
      Given config spark.sql.session.timeZone = Asia/Shanghai
      Given statement template
        """
        CREATE TABLE iceberg_timestamp_session_time_zone
        USING iceberg
        LOCATION {{ location.uri }}
        AS SELECT
          named_struct('ts', TIMESTAMP '2020-06-14 12:00:00') AS s,
          array(TIMESTAMP '2020-06-14 12:00:00') AS a,
          map('k', TIMESTAMP '2020-06-14 12:00:00') AS m
        """
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(s.ts AS STRING) AS s, CAST(a[0] AS STRING) AS a, CAST(m['k'] AS STRING) AS m
        FROM iceberg_timestamp_session_time_zone
        """
      Then query result
        | s                   | a                   | m                   |
        | 2020-06-14 04:00:00 | 2020-06-14 04:00:00 | 2020-06-14 04:00:00 |
