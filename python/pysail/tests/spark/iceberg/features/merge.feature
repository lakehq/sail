Feature: Iceberg MERGE

  Rule: Merge-on-read execution plans and metadata

    Scenario: EXPLAIN shows merge-on-read data and position-delete writers
      Given variable location for temporary directory iceberg_merge_plan
      Given final statement
        """
        DROP TABLE IF EXISTS merge_plan_table
        """
      Given statement template
        """
        CREATE TABLE merge_plan_table (
          id INT,
          value STRING,
          flag STRING
        )
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES (
          'format-version' = '2',
          'write.merge.mode' = 'merge-on-read'
        )
        """
      Given statement
        """
        INSERT INTO merge_plan_table
        SELECT * FROM VALUES
          (1, 'old', 'keep'),
          (2, 'old', 'update'),
          (3, 'old', 'delete'),
          (5, 'old', 'expire'),
          (6, 'old', 'purge')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW merge_plan_source AS
        SELECT * FROM VALUES
          (2, 'new', 'insert'),
          (3, 'ignored', 'delete'),
          (4, 'ins', 'insert')
        AS src(id, value, flag)
        """
      When query
        """
        EXPLAIN MERGE INTO merge_plan_table AS t
        USING merge_plan_source AS s
        ON t.id = s.id
        WHEN MATCHED AND t.flag = 'update' THEN
          UPDATE SET value = s.value
        WHEN MATCHED AND t.flag = 'delete' THEN
          DELETE
        WHEN NOT MATCHED THEN
          INSERT (id, value, flag) VALUES (s.id, s.value, s.flag)
        WHEN NOT MATCHED BY SOURCE AND t.flag = 'expire' THEN
          UPDATE SET value = 'expired'
        WHEN NOT MATCHED BY SOURCE AND t.flag = 'purge' THEN
          DELETE
        """
      Then query plan matches snapshot

    Scenario: MERGE writes overwrite metadata with data and position-delete manifests
      Given variable location for temporary directory iceberg_merge_metadata
      Given final statement
        """
        DROP TABLE IF EXISTS merge_metadata_table
        """
      Given statement template
        """
        CREATE TABLE merge_metadata_table (
          id INT,
          value STRING,
          flag STRING
        )
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES (
          'format-version' = '2',
          'write.merge.mode' = 'merge-on-read'
        )
        """
      Given statement
        """
        INSERT INTO merge_metadata_table
        SELECT * FROM VALUES
          (1, 'old', 'keep'),
          (2, 'old', 'update'),
          (3, 'old', 'delete'),
          (5, 'old', 'expire'),
          (6, 'old', 'purge')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW merge_metadata_source AS
        SELECT * FROM VALUES
          (2, 'new', 'insert'),
          (3, 'ignored', 'delete'),
          (4, 'ins', 'insert')
        AS src(id, value, flag)
        """
      Given statement
        """
        MERGE INTO merge_metadata_table AS t
        USING merge_metadata_source AS s
        ON t.id = s.id
        WHEN MATCHED AND t.flag = 'update' THEN
          UPDATE SET value = s.value
        WHEN MATCHED AND t.flag = 'delete' THEN
          DELETE
        WHEN NOT MATCHED THEN
          INSERT (id, value, flag) VALUES (s.id, s.value, s.flag)
        WHEN NOT MATCHED BY SOURCE AND t.flag = 'expire' THEN
          UPDATE SET value = 'expired'
        WHEN NOT MATCHED BY SOURCE AND t.flag = 'purge' THEN
          DELETE
        """
      Then iceberg metadata matches snapshot
      Then iceberg current manifest list matches snapshot
      Then iceberg current snapshot summary matches snapshot
      Then iceberg schema history matches snapshot
      Then iceberg snapshot count is 2
      When query
        """
        SELECT id, value, flag FROM merge_metadata_table ORDER BY id
        """
      Then query result ordered
        | id | value   | flag   |
        | 1  | old     | keep   |
        | 2  | new     | update |
        | 4  | ins     | insert |
        | 5  | expired | expire |
