Feature: Delta Lake LogScan Parallelism

  Rule: EXPLAIN shows distributed delta log scan plan before execution
    Background:
      Given variable location for temporary directory logscan_parallel
      Given final statement
        """
        DROP TABLE IF EXISTS delta_logscan_parallel
        """
      Given final statement
        """
        DROP VIEW IF EXISTS src_logscan_parallel
        """
      Given statement template
        """
        CREATE TABLE delta_logscan_parallel (
          id INT,
          value STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """

      # Create multiple commits so the planner needs to read multiple commit JSON files.
      Given statement
        """
        INSERT INTO delta_logscan_parallel SELECT * FROM VALUES (1, 'v1')
        """
      Given statement
        """
        INSERT INTO delta_logscan_parallel SELECT * FROM VALUES (2, 'v2')
        """
      Given statement
        """
        INSERT INTO delta_logscan_parallel SELECT * FROM VALUES (3, 'v3')
        """

      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_logscan_parallel AS
        SELECT * FROM VALUES
          (2, 'v2_new'),
          (4, 'v4')
        AS src(id, value)
        """

    Scenario: MERGE EXPLAIN includes log scan union and parallel file groups, then MERGE succeeds
      When query
        """
        EXPLAIN
        MERGE INTO delta_logscan_parallel AS t
        USING src_logscan_parallel AS s
        ON t.id = s.id
        WHEN MATCHED THEN
          UPDATE SET value = s.value
        WHEN NOT MATCHED THEN
          INSERT *
        """
      Then query plan matches snapshot

      Given statement
        """
        MERGE INTO delta_logscan_parallel AS t
        USING src_logscan_parallel AS s
        ON t.id = s.id
        WHEN MATCHED THEN
          UPDATE SET value = s.value
        WHEN NOT MATCHED THEN
          INSERT *
        """
      When query
        """
        SELECT id, value FROM delta_logscan_parallel ORDER BY id
        """
      Then query result ordered
        | id | value  |
        | 1  | v1     |
        | 2  | v2_new |
        | 3  | v3     |
        | 4  | v4     |


