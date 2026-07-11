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

    Scenario: EXPLAIN keeps merge metadata scans constant across target files
      Given variable location for temporary directory iceberg_merge_many_files_plan
      Given final statement
        """
        DROP TABLE IF EXISTS merge_many_files_plan_table
        """
      Given statement template
        """
        CREATE TABLE merge_many_files_plan_table (
          id INT,
          value STRING
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
        INSERT INTO merge_many_files_plan_table VALUES (1, 'one')
        """
      Given statement
        """
        INSERT INTO merge_many_files_plan_table VALUES (2, 'two')
        """
      Given statement
        """
        INSERT INTO merge_many_files_plan_table VALUES (3, 'three')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW merge_many_files_plan_source AS
        SELECT * FROM VALUES
          (1, 'updated-one'),
          (2, 'updated-two'),
          (3, 'updated-three')
        AS source(id, value)
        """
      When query
        """
        EXPLAIN MERGE INTO merge_many_files_plan_table AS t
        USING merge_many_files_plan_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value
        """
      Then query plan matches snapshot
      Given statement
        """
        MERGE INTO merge_many_files_plan_table AS t
        USING merge_many_files_plan_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value
        """
      When query
        """
        SELECT id, value FROM merge_many_files_plan_table ORDER BY id
        """
      Then query result ordered
        | id | value         |
        | 1  | updated-one   |
        | 2  | updated-two   |
        | 3  | updated-three |

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

  Rule: WHEN clauses use first-match semantics

    Scenario: Overlapping matched and target-only clauses apply only their first action
      Given variable location for temporary directory iceberg_merge_first_match
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_merge_first_match
        """
      Given statement template
        """
        CREATE TABLE iceberg_merge_first_match (
          id INT,
          left_value STRING,
          right_value STRING,
          kind STRING
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
        INSERT INTO iceberg_merge_first_match VALUES
          (1, 'old-left', 'old-right', 'delete-update'),
          (2, 'old-left', 'old-right', 'partial-update'),
          (3, 'old-left', 'old-right', 'source-update-delete'),
          (4, 'old-left', 'old-right', 'source-delete-update')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW iceberg_merge_first_match_source AS
        SELECT * FROM VALUES
          (1, 'new-left', 'new-right', 'delete-update'),
          (2, 'new-left', 'new-right', 'partial-update')
        AS src(id, left_value, right_value, kind)
        """
      Given statement
        """
        MERGE INTO iceberg_merge_first_match AS t
        USING iceberg_merge_first_match_source AS s
        ON t.id = s.id
        WHEN MATCHED AND s.kind = 'delete-update' THEN DELETE
        WHEN MATCHED AND s.kind IN ('delete-update', 'partial-update') THEN
          UPDATE SET left_value = s.left_value
        WHEN MATCHED AND s.kind = 'partial-update' THEN
          UPDATE SET right_value = s.right_value
        WHEN MATCHED THEN DELETE
        WHEN NOT MATCHED BY SOURCE AND t.kind = 'source-delete-update' THEN DELETE
        WHEN NOT MATCHED BY SOURCE AND t.kind IN ('source-update-delete', 'source-delete-update') THEN
          UPDATE SET left_value = 'source-left'
        WHEN NOT MATCHED BY SOURCE AND t.kind = 'source-update-delete' THEN
          UPDATE SET right_value = 'source-right'
        WHEN NOT MATCHED BY SOURCE THEN DELETE
        """
      When query
        """
        SELECT id, left_value, right_value, kind
        FROM iceberg_merge_first_match
        ORDER BY id
        """
      Then query result ordered
        | id | left_value  | right_value | kind                 |
        | 2  | new-left    | old-right   | partial-update       |
        | 3  | source-left | old-right   | source-update-delete |

  Rule: Internal MERGE columns cannot shadow table data

    Scenario: A target column using an internal operation name is rejected clearly
      Given variable location for temporary directory iceberg_merge_internal_column
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_merge_internal_column
        """
      Given statement template
        """
        CREATE TABLE iceberg_merge_internal_column (
          `__sail_operation_type` INT,
          value STRING
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
        INSERT INTO iceberg_merge_internal_column VALUES (1, 'old')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW iceberg_merge_internal_column_source AS
        SELECT 1 AS id, 'new' AS value
        """
      When query
        """
        MERGE INTO iceberg_merge_internal_column AS t
        USING iceberg_merge_internal_column_source AS s
        ON t.`__sail_operation_type` = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value
        """
      Then query error reserved internal MERGE column
