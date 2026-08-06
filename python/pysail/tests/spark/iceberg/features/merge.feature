Feature: Iceberg MERGE

  Rule: MERGE conditions must be deterministic

    Scenario: A non-deterministic matched condition is rejected before writing
      Given variable location for temporary directory iceberg_merge_nondeterministic
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_merge_nondeterministic
        """
      Given statement template
        """
        CREATE TABLE iceberg_merge_nondeterministic (id INT, value STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES (
          'format-version' = '2',
          'write.merge.mode' = 'merge-on-read'
        )
        """
      Given statement
        """
        INSERT INTO iceberg_merge_nondeterministic VALUES (1, 'old')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW iceberg_merge_nondeterministic_source AS
        SELECT 1 AS id, 'new' AS value
        """
      When query
        """
        MERGE INTO iceberg_merge_nondeterministic AS t
        USING iceberg_merge_nondeterministic_source AS s
        ON t.id = s.id
        WHEN MATCHED AND rand() < 0.5 THEN UPDATE SET value = s.value
        """
      Then query error Non-deterministic expressions are not allowed in MERGE conditions
      When query
        """
        SELECT id, value FROM iceberg_merge_nondeterministic
        """
      Then query result
        | id | value |
        | 1  | old   |

  Rule: MERGE assignments follow target schema semantics

    Scenario: A legal target column does not collide with generated source aliases
      Given variable location for temporary directory iceberg_merge_source_alias
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_merge_source_alias
        """
      Given statement template
        """
        CREATE TABLE iceberg_merge_source_alias (`__sail_src_id` INT, value STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES (
          'format-version' = '2',
          'write.merge.mode' = 'merge-on-read'
        )
        """
      Given statement
        """
        INSERT INTO iceberg_merge_source_alias VALUES (1, 'old')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW iceberg_merge_source_alias_source AS
        SELECT 1 AS id, 'new' AS value
        """
      Given statement
        """
        MERGE INTO iceberg_merge_source_alias AS t
        USING iceberg_merge_source_alias_source AS s
        ON t.`__sail_src_id` = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value
        """
      When query
        """
        SELECT `__sail_src_id`, value FROM iceberg_merge_source_alias
        """
      Then query result
        | __sail_src_id | value |
        | 1             | new   |

    Scenario: Star actions reject source columns missing from the target schema
      Given variable location for temporary directory iceberg_merge_star_missing
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_merge_star_missing
        """
      Given statement template
        """
        CREATE TABLE iceberg_merge_star_missing (id INT, value STRING, keep STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES (
          'format-version' = '2',
          'write.merge.mode' = 'merge-on-read'
        )
        """
      Given statement
        """
        INSERT INTO iceberg_merge_star_missing VALUES (1, 'old', 'preserved')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW iceberg_merge_star_missing_source AS
        SELECT 1 AS id, 'new' AS value
        """
      When query
        """
        MERGE INTO iceberg_merge_star_missing AS t
        USING iceberg_merge_star_missing_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
      Then query error Cannot resolve source column `keep` for MERGE \* action without schema evolution

    Scenario: Assignments cast to target types and reject overflow
      Given config spark.sql.ansi.enabled = true
      Given variable location for temporary directory iceberg_merge_assignment_cast
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_merge_assignment_cast
        """
      Given statement template
        """
        CREATE TABLE iceberg_merge_assignment_cast (id INT, value INT)
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES (
          'format-version' = '2',
          'write.merge.mode' = 'merge-on-read'
        )
        """
      Given statement
        """
        INSERT INTO iceberg_merge_assignment_cast VALUES (1, 10)
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW iceberg_merge_assignment_cast_source AS
        SELECT 1 AS id, CAST(20 AS BIGINT) AS value
        """
      Given statement
        """
        MERGE INTO iceberg_merge_assignment_cast AS t
        USING iceberg_merge_assignment_cast_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value
        """
      When query
        """
        SELECT id, value FROM iceberg_merge_assignment_cast
        """
      Then query result
        | id | value |
        | 1  | 20    |
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW iceberg_merge_assignment_cast_source AS
        SELECT 1 AS id, CAST(2147483648 AS BIGINT) AS value
        """
      When query
        """
        MERGE INTO iceberg_merge_assignment_cast AS t
        USING iceberg_merge_assignment_cast_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value
        """
      Then query error (?i).*(cast|overflow).*
      When query
        """
        SELECT id, value FROM iceberg_merge_assignment_cast
        """
      Then query result
        | id | value |
        | 1  | 20    |

    Scenario: Duplicate target assignments are rejected
      Given variable location for temporary directory iceberg_merge_duplicate_assignment
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_merge_duplicate_assignment
        """
      Given statement template
        """
        CREATE TABLE iceberg_merge_duplicate_assignment (id INT, value STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES (
          'format-version' = '2',
          'write.merge.mode' = 'merge-on-read'
        )
        """
      Given statement
        """
        INSERT INTO iceberg_merge_duplicate_assignment VALUES (1, 'old')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW iceberg_merge_duplicate_assignment_source AS
        SELECT 1 AS id
        """
      When query
        """
        MERGE INTO iceberg_merge_duplicate_assignment AS t
        USING iceberg_merge_duplicate_assignment_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = 'first', value = 'second'
        """
      Then query error Multiple assignments for MERGE target column

    Scenario: Star matching honors case-sensitive resolution
      Given config spark.sql.caseSensitive = true
      Given variable location for temporary directory iceberg_merge_case_sensitive
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_merge_case_sensitive
        """
      Given statement template
        """
        CREATE TABLE iceberg_merge_case_sensitive (`A` INT)
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES (
          'format-version' = '2',
          'write.merge.mode' = 'merge-on-read'
        )
        """
      Given statement
        """
        INSERT INTO iceberg_merge_case_sensitive VALUES (1)
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW iceberg_merge_case_sensitive_source AS
        SELECT 1 AS a
        """
      When query
        """
        MERGE INTO iceberg_merge_case_sensitive AS t
        USING iceberg_merge_case_sensitive_source AS s
        ON t.`A` = s.a
        WHEN MATCHED THEN UPDATE SET *
        """
      Then query error Cannot resolve source column `A` for MERGE \* action without schema evolution

    Scenario: Unqualified MERGE columns honor case-sensitive resolution
      Given config spark.sql.caseSensitive = true
      Given variable location for temporary directory iceberg_merge_unqualified_case_sensitive
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_merge_unqualified_case_sensitive
        """
      Given statement template
        """
        CREATE TABLE iceberg_merge_unqualified_case_sensitive (`A` INT)
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES (
          'format-version' = '2',
          'write.merge.mode' = 'merge-on-read'
        )
        """
      Given statement
        """
        INSERT INTO iceberg_merge_unqualified_case_sensitive VALUES (1)
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW iceberg_merge_unqualified_case_sensitive_source AS
        SELECT 2 AS a
        """
      Given statement
        """
        MERGE INTO iceberg_merge_unqualified_case_sensitive AS t
        USING iceberg_merge_unqualified_case_sensitive_source AS s
        ON `A` = a
        WHEN MATCHED THEN UPDATE SET `A` = s.a
        WHEN NOT MATCHED THEN INSERT (`A`) VALUES (s.a)
        """
      When query
        """
        SELECT `A` FROM iceberg_merge_unqualified_case_sensitive ORDER BY `A`
        """
      Then query result ordered
        | A |
        | 1 |
        | 2 |

  Rule: Merge-on-read execution plans and metadata

    Scenario: EXPLAIN hash partitions merge-on-read row intents for parallel writers
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

    Scenario: EXPLAIN hashes partitioned merge intents by Iceberg transforms
      Given variable location for temporary directory iceberg_merge_partitioned_plan
      Given final statement
        """
        DROP TABLE IF EXISTS merge_partitioned_plan_table
        """
      Given statement template
        """
        CREATE TABLE merge_partitioned_plan_table (
          id INT,
          event_time TIMESTAMP,
          value STRING
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
        LOCATION {{ location.uri }}
        TBLPROPERTIES (
          'format-version' = '2',
          'write.merge.mode' = 'merge-on-read'
        )
        """
      Given statement
        """
        INSERT INTO merge_partitioned_plan_table VALUES
          (1, TIMESTAMP '2024-01-01 10:00:00', 'old')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW merge_partitioned_plan_source AS
        SELECT * FROM VALUES
          (1, TIMESTAMP '2024-01-01 10:00:00', 'updated'),
          (2, TIMESTAMP '2024-01-02 10:00:00', 'inserted')
        AS source(id, event_time, value)
        """
      When query
        """
        EXPLAIN MERGE INTO merge_partitioned_plan_table AS t
        USING merge_partitioned_plan_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value
        WHEN NOT MATCHED THEN
          INSERT (id, event_time, value) VALUES (s.id, s.event_time, s.value)
        """
      Then query plan matches snapshot
      Given statement
        """
        MERGE INTO merge_partitioned_plan_table AS t
        USING merge_partitioned_plan_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value
        WHEN NOT MATCHED THEN
          INSERT (id, event_time, value) VALUES (s.id, s.event_time, s.value)
        """
      Then iceberg current manifest list matches snapshot
      Then iceberg current snapshot summary matches snapshot
      Then iceberg snapshot count is 2
      When query
        """
        SELECT id, value FROM merge_partitioned_plan_table ORDER BY id
        """
      Then query result ordered
        | id | value    |
        | 1  | updated  |
        | 2  | inserted |

    Scenario: EXPLAIN keeps merge metadata scans constant across target files
      Given variable location for temporary directory iceberg_merge_many_files_plan
      Given final statement
        """
        DROP TABLE IF EXISTS merge_many_files_plan_table
        """
      # An unpartitioned MERGE hashes random-UUID file paths, making exact file-count
      # snapshots unstable. Partition by id to use deterministic writer distribution keys.
      Given statement template
        """
        CREATE TABLE merge_many_files_plan_table (
          id INT,
          value STRING
        )
        USING iceberg
        PARTITIONED BY (id)
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
      Then iceberg current manifest list matches snapshot
      Then iceberg current snapshot summary matches snapshot
      Then iceberg snapshot count is 4
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
        PARTITIONED BY (flag)
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

  Rule: MERGE assignments honor the configured store assignment policy

    Scenario Outline: Incompatible string assignments are rejected before execution
      Given config spark.sql.storeAssignmentPolicy = <policy>
      Given variable location for temporary directory iceberg_merge_store_assignment_policy
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_merge_store_assignment_policy
        """
      Given statement template
        """
        CREATE TABLE iceberg_merge_store_assignment_policy (id INT, value INT)
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES (
          'format-version' = '2',
          'write.merge.mode' = 'merge-on-read'
        )
        """
      Given statement
        """
        INSERT INTO iceberg_merge_store_assignment_policy VALUES (1, 10)
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW iceberg_merge_store_assignment_policy_source AS
        SELECT 1 AS id, '20' AS value
        """
      When query
        """
        MERGE INTO iceberg_merge_store_assignment_policy AS t
        USING iceberg_merge_store_assignment_policy_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value
        """
      Then query error (?i)(legacy store assignment policy|cannot safely cast|cannot write incompatible data)

      Examples:
        | policy |
        | ANSI   |
        | STRICT |
        | LEGACY |

  Rule: Each MERGE clause exposes only the rows available to that clause

    Background:
      Given variable location for temporary directory iceberg_merge_clause_visibility
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_merge_clause_visibility
        """
      Given statement template
        """
        CREATE TABLE iceberg_merge_clause_visibility (id INT, value STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES (
          'format-version' = '2',
          'write.merge.mode' = 'merge-on-read'
        )
        """
      Given statement
        """
        INSERT INTO iceberg_merge_clause_visibility VALUES (1, 'target')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW iceberg_merge_clause_visibility_source AS
        SELECT 2 AS id, 'source' AS value
        """

    Scenario: NOT MATCHED clauses cannot reference target columns
      When query
        """
        MERGE INTO iceberg_merge_clause_visibility AS t
        USING iceberg_merge_clause_visibility_source AS s
        ON t.id = s.id
        WHEN NOT MATCHED AND t.value = 'target' THEN
          INSERT (id, value) VALUES (s.id, t.value)
        """
      Then query error (?i)t.*value.*(missing|cannot resolve)

    Scenario: NOT MATCHED BY SOURCE clauses cannot reference source columns
      When query
        """
        MERGE INTO iceberg_merge_clause_visibility AS t
        USING iceberg_merge_clause_visibility_source AS s
        ON t.id = s.id
        WHEN NOT MATCHED BY SOURCE AND s.value = 'source' THEN
          UPDATE SET value = s.value
        """
      Then query error (?i)s.*value.*(missing|cannot resolve)

  Rule: Only the final clause in each MERGE family may omit its condition

    Background:
      Given variable location for temporary directory iceberg_merge_clause_order
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_merge_clause_order
        """
      Given statement template
        """
        CREATE TABLE iceberg_merge_clause_order (id INT, value STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES (
          'format-version' = '2',
          'write.merge.mode' = 'merge-on-read'
        )
        """
      Given statement
        """
        INSERT INTO iceberg_merge_clause_order VALUES (1, 'target')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW iceberg_merge_clause_order_source AS
        SELECT * FROM VALUES (1, 'matched'), (2, 'inserted') AS src(id, value)
        """

    Scenario: A non-final unconditional MATCHED clause is rejected
      When query
        """
        MERGE INTO iceberg_merge_clause_order AS t
        USING iceberg_merge_clause_order_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value
        WHEN MATCHED AND s.value = 'matched' THEN DELETE
        """
      Then query error NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION

    Scenario: A non-final unconditional NOT MATCHED clause is rejected
      When query
        """
        MERGE INTO iceberg_merge_clause_order AS t
        USING iceberg_merge_clause_order_source AS s
        ON t.id = s.id
        WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)
        WHEN NOT MATCHED AND s.id = 2 THEN INSERT (id, value) VALUES (s.id, 'later')
        """
      Then query error NON_LAST_NOT_MATCHED_BY_TARGET_CLAUSE_OMIT_CONDITION

    Scenario: A non-final unconditional NOT MATCHED BY SOURCE clause is rejected
      When query
        """
        MERGE INTO iceberg_merge_clause_order AS t
        USING iceberg_merge_clause_order_source AS s
        ON t.id = s.id
        WHEN NOT MATCHED BY SOURCE THEN UPDATE SET value = 'missing'
        WHEN NOT MATCHED BY SOURCE AND t.id = 1 THEN DELETE
        """
      Then query error NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION
