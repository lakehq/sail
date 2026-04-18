Feature: Delta Lake timestamp-partitioned tables

  Rule: Writes and reads timestamp-partitioned tables correctly

    Background:
      Given variable location for temporary directory delta_ts_partition
      Given config spark.sql.session.timeZone = UTC
      Given final statement
        """
        DROP TABLE IF EXISTS delta_ts_part_test
        """
      Given statement template
        """
        CREATE TABLE delta_ts_part_test (
          id INT,
          ts TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (ts)
        LOCATION {{ location.sql }}
        """

    Scenario: Timestamp-partitioned table has percent-encoded partition directories
      Given statement
        """
        INSERT INTO delta_ts_part_test VALUES
          (1, TIMESTAMP '2024-01-15 10:30:00'),
          (2, TIMESTAMP '2024-06-20 15:45:00.123456')
        """
      Then file tree in location matches
        """
        📂 ts=2024-01-15%2010%3A30%3A00.000000
          📄 part-<id>.<codec>.parquet
        📂 ts=2024-06-20%2015%3A45%3A00.123456
          📄 part-<id>.<codec>.parquet
        """

    Scenario: Read back rows from a timestamp-partitioned table it wrote
      Given statement
        """
        INSERT INTO delta_ts_part_test VALUES
          (1, TIMESTAMP '2024-01-15 10:30:00'),
          (2, TIMESTAMP '2024-06-20 15:45:00.123456')
        """
      When query
        """
        SELECT id, CAST(ts AS STRING) AS ts_str
        FROM delta_ts_part_test
        ORDER BY id
        """
      Then query result
        | id | ts_str                     |
        | 1  | 2024-01-15 10:30:00        |
        | 2  | 2024-06-20 15:45:00.123456 |
