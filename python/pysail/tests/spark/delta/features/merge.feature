Feature: Delta Lake Merge

  Rule: MERGE conditions must be deterministic

    Scenario: A non-deterministic matched condition is rejected before writing
      Given variable location for temporary directory delta_merge_nondeterministic
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_nondeterministic
        """
      Given statement template
        """
        CREATE TABLE delta_merge_nondeterministic (id INT, value STRING)
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_nondeterministic VALUES (1, 'old')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW delta_merge_nondeterministic_source AS
        SELECT 1 AS id, 'new' AS value
        """
      When query
        """
        MERGE INTO delta_merge_nondeterministic AS t
        USING delta_merge_nondeterministic_source AS s
        ON t.id = s.id
        WHEN MATCHED AND rand() < 0.5 THEN UPDATE SET value = s.value
        """
      Then query error Non-deterministic expressions are not allowed in MERGE conditions
      When query
        """
        SELECT id, value FROM delta_merge_nondeterministic
        """
      Then query result
        | id | value |
        | 1  | old   |

  Rule: Internal MERGE columns cannot shadow table data

    Scenario: A target column using an internal operation name is rejected clearly
      Given variable location for temporary directory delta_merge_internal_column
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_internal_column
        """
      Given statement template
        """
        CREATE TABLE delta_merge_internal_column (
          `__sail_operation_type` INT,
          value STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_internal_column VALUES (1, 'old')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW delta_merge_internal_column_source AS
        SELECT 1 AS id, 'new' AS value
        """
      When query
        """
        MERGE INTO delta_merge_internal_column AS t
        USING delta_merge_internal_column_source AS s
        ON t.`__sail_operation_type` = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value
        """
      Then query error reserved internal MERGE column

    Scenario: A legal target column does not collide with generated source aliases
      Given variable location for temporary directory delta_merge_source_alias
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_source_alias
        """
      Given statement template
        """
        CREATE TABLE delta_merge_source_alias (`__sail_src_id` INT, value STRING)
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_source_alias VALUES (1, 'old')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW delta_merge_source_alias_source AS
        SELECT 1 AS id, 'new' AS value
        """
      Given statement
        """
        MERGE INTO delta_merge_source_alias AS t
        USING delta_merge_source_alias_source AS s
        ON t.`__sail_src_id` = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value
        """
      When query
        """
        SELECT `__sail_src_id`, value FROM delta_merge_source_alias
        """
      Then query result
        | __sail_src_id | value |
        | 1             | new   |

    Scenario: Schema evolution rejects case-insensitive internal column collisions
      Given variable location for temporary directory delta_merge_evolved_internal_column
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_evolved_internal_column
        """
      Given statement template
        """
        CREATE TABLE delta_merge_evolved_internal_column (id INT)
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_evolved_internal_column VALUES (1)
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW delta_merge_evolved_internal_source AS
        SELECT 1 AS id, 10 AS `__SAIL_OPERATION_TYPE`
        """
      When query
        """
        MERGE WITH SCHEMA EVOLUTION INTO delta_merge_evolved_internal_column AS t
        USING delta_merge_evolved_internal_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        """
      Then query error MERGE schema evolution cannot add reserved internal column
      When query
        """
        SELECT id FROM delta_merge_evolved_internal_column
        """
      Then query result
        | id |
        | 1  |

  Rule: MERGE assignments follow target schema semantics

    Scenario: Star actions reject source columns missing from the target schema
      Given variable location for temporary directory delta_merge_star_missing
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_star_missing
        """
      Given statement template
        """
        CREATE TABLE delta_merge_star_missing (id INT, value STRING, keep STRING)
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_star_missing VALUES (1, 'old', 'preserved')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW delta_merge_star_missing_source AS
        SELECT 1 AS id, 'new' AS value
        """
      When query
        """
        MERGE INTO delta_merge_star_missing AS t
        USING delta_merge_star_missing_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
      Then query error Cannot resolve source column `keep` for MERGE \* action without schema evolution

    Scenario: WITH SCHEMA EVOLUTION appends source-only columns for star actions
      Given variable location for temporary directory delta_merge_schema_evolution
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_schema_evolution
        """
      Given statement template
        """
        CREATE TABLE delta_merge_schema_evolution (id INT, value STRING)
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_schema_evolution VALUES (1, 'old'), (2, 'keep')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW delta_merge_schema_evolution_source AS
        SELECT * FROM VALUES
          (1, 'new', 10),
          (3, 'insert', 30)
        AS source(id, value, extra)
        """
      Given statement
        """
        MERGE WITH SCHEMA EVOLUTION INTO delta_merge_schema_evolution AS target
        USING delta_merge_schema_evolution_source AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
      When query
        """
        SELECT id, value, extra FROM delta_merge_schema_evolution ORDER BY id
        """
      Then query result ordered
        | id | value  | extra |
        | 1  | new    | 10    |
        | 2  | keep   | NULL  |
        | 3  | insert | 30    |

    Scenario: WITH SCHEMA EVOLUTION appends source-only nested struct fields
      Given variable location for temporary directory delta_merge_nested_schema_evolution
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_nested_schema_evolution
        """
      Given statement template
        """
        CREATE TABLE delta_merge_nested_schema_evolution (
          id INT,
          payload STRUCT<a: INT>
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_nested_schema_evolution VALUES
          (1, named_struct('a', 10)),
          (2, named_struct('a', 20))
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW delta_merge_nested_schema_source AS
        SELECT * FROM VALUES
          (1, named_struct('a', 11, 'b', 'updated')),
          (3, named_struct('a', 30, 'b', 'inserted'))
        AS source(id, payload)
        """
      Given statement
        """
        MERGE WITH SCHEMA EVOLUTION INTO delta_merge_nested_schema_evolution AS t
        USING delta_merge_nested_schema_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
      When query
        """
        SELECT id, payload.a, payload.b
        FROM delta_merge_nested_schema_evolution
        ORDER BY id
        """
      Then query result ordered
        | id | a  | b       |
        | 1  | 11 | updated |
        | 2  | 20 | NULL    |
        | 3  | 30 | inserted |

    Scenario: Assignments cast to target types and reject overflow
      Given config spark.sql.ansi.enabled = true
      Given variable location for temporary directory delta_merge_assignment_cast
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_assignment_cast
        """
      Given statement template
        """
        CREATE TABLE delta_merge_assignment_cast (id INT, value INT)
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_assignment_cast VALUES (1, 10)
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW delta_merge_assignment_cast_source AS
        SELECT 1 AS id, CAST(20 AS BIGINT) AS value
        """
      Given statement
        """
        MERGE INTO delta_merge_assignment_cast AS t
        USING delta_merge_assignment_cast_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value
        """
      When query
        """
        SELECT id, value FROM delta_merge_assignment_cast
        """
      Then query result
        | id | value |
        | 1  | 20    |
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW delta_merge_assignment_cast_source AS
        SELECT 1 AS id, CAST(2147483648 AS BIGINT) AS value
        """
      When query
        """
        MERGE INTO delta_merge_assignment_cast AS t
        USING delta_merge_assignment_cast_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value
        """
      Then query error (?i).*(cast|overflow).*
      When query
        """
        SELECT id, value FROM delta_merge_assignment_cast
        """
      Then query result
        | id | value |
        | 1  | 20    |

    Scenario: Duplicate target assignments are rejected
      Given variable location for temporary directory delta_merge_duplicate_assignment
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_duplicate_assignment
        """
      Given statement template
        """
        CREATE TABLE delta_merge_duplicate_assignment (id INT, value STRING)
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_duplicate_assignment VALUES (1, 'old')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW delta_merge_duplicate_assignment_source AS
        SELECT 1 AS id
        """
      When query
        """
        MERGE INTO delta_merge_duplicate_assignment AS t
        USING delta_merge_duplicate_assignment_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = 'first', value = 'second'
        """
      Then query error Multiple assignments for MERGE target column

    Scenario: Star matching honors case-sensitive resolution
      Given config spark.sql.caseSensitive = true
      Given variable location for temporary directory delta_merge_case_sensitive
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_case_sensitive
        """
      Given statement template
        """
        CREATE TABLE delta_merge_case_sensitive (`A` INT)
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_case_sensitive VALUES (1)
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW delta_merge_case_sensitive_source AS
        SELECT 1 AS a
        """
      When query
        """
        MERGE INTO delta_merge_case_sensitive AS t
        USING delta_merge_case_sensitive_source AS s
        ON t.`A` = s.a
        WHEN MATCHED THEN UPDATE SET *
        """
      Then query error Cannot resolve source column `A` for MERGE \* action without schema evolution

    Scenario: Explicit inserts use target column defaults
      Given variable location for temporary directory delta_merge_assignment_default
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_assignment_default
        """
      Given statement template
        """
        CREATE TABLE delta_merge_assignment_default (
          id INT,
          status STRING DEFAULT 'new'
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW delta_merge_assignment_default_source AS
        SELECT 1 AS id
        """
      Given statement
        """
        MERGE INTO delta_merge_assignment_default AS t
        USING delta_merge_assignment_default_source AS s
        ON t.id = s.id
        WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)
        """
      When query
        """
        SELECT id, status FROM delta_merge_assignment_default
        """
      Then query result
        | id | status |
        | 1  | new    |

    Scenario: Explicit DEFAULT assignments use the target default or NULL
      Given variable location for temporary directory delta_merge_explicit_assignment_default
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_explicit_assignment_default
        """
      Given statement template
        """
        CREATE TABLE delta_merge_explicit_assignment_default (
          id INT,
          status STRING DEFAULT 'new',
          note STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_explicit_assignment_default VALUES (1, 'old', 'old')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW delta_merge_explicit_assignment_default_source AS
        SELECT 1 AS id
        """
      Given statement
        """
        MERGE INTO delta_merge_explicit_assignment_default AS t
        USING delta_merge_explicit_assignment_default_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET status = DEFAULT, note = DEFAULT
        """
      When query
        """
        SELECT id, status, note FROM delta_merge_explicit_assignment_default
        """
      Then query result
        | id | status | note |
        | 1  | new    | NULL |

    Scenario: Explicit DEFAULT requires a default for a non-nullable target
      Given variable location for temporary directory delta_merge_required_assignment_default
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_required_assignment_default
        """
      Given statement template
        """
        CREATE TABLE delta_merge_required_assignment_default (
          id INT,
          status STRING NOT NULL
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_required_assignment_default VALUES (1, 'old')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW delta_merge_required_assignment_default_source AS
        SELECT 1 AS id
        """
      When query
        """
        MERGE INTO delta_merge_required_assignment_default AS t
        USING delta_merge_required_assignment_default_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET status = DEFAULT
        """
      Then query error (?i)(NO_DEFAULT_COLUMN_VALUE_AVAILABLE|not nullable.*no default value)

    Scenario: Explicit DEFAULT cannot be wrapped in a MERGE expression
      Given variable location for temporary directory delta_merge_complex_assignment_default
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_complex_assignment_default
        """
      Given statement template
        """
        CREATE TABLE delta_merge_complex_assignment_default (
          id INT,
          status STRING DEFAULT 'new'
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_complex_assignment_default VALUES (1, 'old')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW delta_merge_complex_assignment_default_source AS
        SELECT 1 AS id
        """
      When query
        """
        MERGE INTO delta_merge_complex_assignment_default AS t
        USING delta_merge_complex_assignment_default_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET status = CAST(DEFAULT AS STRING)
        """
      Then query error (?i)(DEFAULT.*standalone|DEFAULT.*complex expression)

  Rule: MERGE assignments honor the configured store assignment policy

    Scenario Outline: Incompatible string assignments are rejected before execution
      Given config spark.sql.storeAssignmentPolicy = <policy>
      Given variable location for temporary directory delta_merge_store_assignment_policy
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_store_assignment_policy
        """
      Given statement template
        """
        CREATE TABLE delta_merge_store_assignment_policy (id INT, value INT)
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_store_assignment_policy VALUES (1, 10)
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW delta_merge_store_assignment_policy_source AS
        SELECT 1 AS id, '20' AS value
        """
      When query
        """
        MERGE INTO delta_merge_store_assignment_policy AS t
        USING delta_merge_store_assignment_policy_source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value
        """
      Then query error (?i)(legacy store assignment policy|cannot safely cast|cannot write incompatible data)

      Examples:
        | policy |
        | ANSI   |
        | STRICT |
        | LEGACY |

  Rule: Matched updates, deletes, and default inserts
    Background:
      Given variable location for temporary directory merge_basic
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_basic
        """
      Given statement template
        """
        CREATE TABLE delta_merge_basic (
          id INT,
          value STRING,
          flag STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_basic
        SELECT * FROM VALUES
          (1, 'old', 'keep'),
          (2, 'old', 'update'),
          (3, 'old', 'delete')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_merge_basic AS
        SELECT * FROM VALUES
          (2, 'new', 'insert'),
          (3, 'any', 'ignored'),
          (4, 'ins', 'insert')
        AS src(id, value, flag)
        """

    Scenario: Update and delete matched rows while inserting unmatched rows
      Given statement
        """
        MERGE INTO delta_merge_basic AS t
        USING src_merge_basic AS s
        ON t.id = s.id
        WHEN MATCHED AND t.flag = 'update' THEN
          UPDATE SET value = s.value
        WHEN MATCHED AND t.flag = 'delete' THEN
          DELETE
        WHEN NOT MATCHED THEN
          INSERT *
        """
      Then delta log latest commit info matches snapshot
      Then delta log latest commit info contains
        | path                                               | value                  |
        | operation                                          | "MERGE"                |
        | operationParameters.mergePredicate                 | "t . id = s . id " |
        | operationParameters.matchedPredicates[0].actionType | "update"               |
        | operationParameters.matchedPredicates[1].actionType | "delete"               |
        | operationParameters.notMatchedPredicates[0].actionType | "insert"            |
      When query
        """
        SELECT id, value, flag FROM delta_merge_basic ORDER BY id
        """
      Then query result ordered
        | id | value | flag   |
        | 1  | old   | keep   |
        | 2  | new   | update |
        | 4  | ins   | insert |

  Rule: WHEN clauses use first-match semantics

    Scenario: Overlapping matched and target-only clauses apply only their first action
      Given variable location for temporary directory delta_merge_first_match
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_first_match
        """
      Given statement template
        """
        CREATE TABLE delta_merge_first_match (
          id INT,
          left_value STRING,
          right_value STRING,
          kind STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_first_match VALUES
          (1, 'old-left', 'old-right', 'delete-update'),
          (2, 'old-left', 'old-right', 'partial-update'),
          (3, 'old-left', 'old-right', 'source-update-delete'),
          (4, 'old-left', 'old-right', 'source-delete-update')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW delta_merge_first_match_source AS
        SELECT * FROM VALUES
          (1, 'new-left', 'new-right', 'delete-update'),
          (2, 'new-left', 'new-right', 'partial-update')
        AS src(id, left_value, right_value, kind)
        """
      Given statement
        """
        MERGE INTO delta_merge_first_match AS t
        USING delta_merge_first_match_source AS s
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
        FROM delta_merge_first_match
        ORDER BY id
        """
      Then query result ordered
        | id | left_value  | right_value | kind                 |
        | 2  | new-left    | old-right   | partial-update       |
        | 3  | source-left | old-right   | source-update-delete |

  Rule: Cardinality violation is rejected when multiple source rows match one target row
    Background:
      Given variable location for temporary directory merge_cardinality
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_cardinality
        """
      Given statement template
        """
        CREATE TABLE delta_merge_cardinality (
          id INT,
          value STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_cardinality
        SELECT * FROM VALUES
          (1, 't1')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_merge_cardinality AS
        SELECT * FROM VALUES
          (1, 's1'),
          (1, 's2')
        AS src(id, value)
        """

    Scenario: MERGE errors when a single target row matches multiple source rows
      When query
        """
        MERGE INTO delta_merge_cardinality AS t
        USING src_merge_cardinality AS s
        ON t.id = s.id
        WHEN MATCHED THEN
          UPDATE SET value = s.value
        WHEN NOT MATCHED THEN
          INSERT *
        """
      Then query error MERGE_CARDINALITY_VIOLATION

  Rule: Cardinality check can be skipped when source is provably unique on join keys
    Background:
      Given variable location for temporary directory merge_cardinality_skip
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_cardinality_skip
        """
      Given statement template
        """
        CREATE TABLE delta_merge_cardinality_skip (
          id INT,
          value STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_cardinality_skip
        SELECT * FROM VALUES
          (1, 't1')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_merge_cardinality_skip AS
        SELECT
          id,
          max(value) AS value
        FROM VALUES
          (1, 's1'),
          (1, 's2')
        AS src(id, value)
        GROUP BY id
        """

    Scenario: EXPLAIN EXTENDED does not include MergeCardinalityCheck when source is grouped by join keys
      When query
        """
        EXPLAIN EXTENDED
        MERGE INTO delta_merge_cardinality_skip AS t
        USING src_merge_cardinality_skip AS s
        ON t.id = s.id
        WHEN MATCHED THEN
          UPDATE SET value = s.value
        WHEN NOT MATCHED THEN
          INSERT *
        """
      Then query plan matches snapshot
      Given statement
        """
        MERGE INTO delta_merge_cardinality_skip AS t
        USING src_merge_cardinality_skip AS s
        ON t.id = s.id
        WHEN MATCHED THEN
          UPDATE SET value = s.value
        WHEN NOT MATCHED THEN
          INSERT *
        """
      When query
        """
        SELECT id, value FROM delta_merge_cardinality_skip ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | s2    |


    Scenario: MERGE succeeds when source is grouped by join keys
      Given statement
        """
        MERGE INTO delta_merge_cardinality_skip AS t
        USING src_merge_cardinality_skip AS s
        ON t.id = s.id
        WHEN MATCHED THEN
          UPDATE SET value = s.value
        WHEN NOT MATCHED THEN
          INSERT *
        """
      When query
        """
        SELECT id, value FROM delta_merge_cardinality_skip ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | s2    |

  Rule: Insert-only MERGE can fast-append without rewriting target files
    Background:
      Given variable location for temporary directory merge_insert_only
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_insert_only
        """
      Given statement template
        """
        CREATE TABLE delta_merge_insert_only (
          id INT,
          value STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_insert_only
        SELECT * FROM VALUES
          (1, 'keep'),
          (2, 'keep')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_merge_insert_only AS
        SELECT * FROM VALUES
          (2, 'ignored'),
          (3, 'inserted')
        AS src(id, value)
        """

    Scenario: Insert-only MERGE appends new rows and produces no remove actions
      Given statement
        """
        MERGE INTO delta_merge_insert_only AS t
        USING src_merge_insert_only AS s
        ON t.id = s.id
        WHEN NOT MATCHED THEN
          INSERT *
        """
      Then delta log latest commit info matches snapshot
      Then delta log latest commit info contains
        | path                                               | value                  |
        | operation                                          | "MERGE"                |
        | operationParameters.mergePredicate                 | "t . id = s . id " |
        | operationParameters.matchedPredicates              | []                     |
        | operationParameters.notMatchedBySourcePredicates   | []                     |
        | operationParameters.notMatchedPredicates[0].actionType | "insert"            |
      When query
        """
        SELECT id, value FROM delta_merge_insert_only ORDER BY id
        """
      Then query result ordered
        | id | value    |
        | 1  | keep     |
        | 2  | keep     |
        | 3  | inserted |

    Scenario: EXPLAIN EXTENDED shows fast-append plan shape for insert-only MERGE
      When query
        """
        EXPLAIN EXTENDED
        MERGE INTO delta_merge_insert_only AS t
        USING src_merge_insert_only AS s
        ON t.id = s.id
        WHEN NOT MATCHED THEN
          INSERT *
        """
      Then query plan matches snapshot
      Given statement
        """
        MERGE INTO delta_merge_insert_only AS t
        USING src_merge_insert_only AS s
        ON t.id = s.id
        WHEN NOT MATCHED THEN
          INSERT *
        """
      When query
        """
        SELECT id, value FROM delta_merge_insert_only ORDER BY id
        """
      Then query result ordered
        | id | value    |
        | 1  | keep     |
        | 2  | keep     |
        | 3  | inserted |

  Rule: Merge-on-Read MERGE on deletion-vector tables
    Background:
      Given variable location for temporary directory merge_dv
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_dv
        """
      Given statement template
        """
        CREATE TABLE delta_merge_dv
        USING DELTA LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'true'
        )
        AS SELECT * FROM VALUES
          (1, 'keep',   'target'),
          (2, 'remove', 'target'),
          (3, 'stay',   'target')
        AS t(id, value, flag)
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_merge_dv AS
        SELECT * FROM VALUES
          (2, 'updated',  'delete'),
          (4, 'inserted', 'insert')
        AS src(id, value, flag)
        """

    Scenario: EXPLAIN for insert-only MERGE on a DV table does not request row indices
      When query
        """
        EXPLAIN
        MERGE INTO delta_merge_dv AS t
        USING src_merge_dv AS s
        ON t.id = s.id
        WHEN NOT MATCHED THEN
          INSERT (id, value, flag)
          VALUES (s.id, s.value, s.flag)
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN for MERGE delete on a DV table uses sorted partitioned DV rows
      When query
        """
        EXPLAIN
        MERGE INTO delta_merge_dv AS t
        USING src_merge_dv AS s
        ON t.id = s.id
        WHEN MATCHED AND s.flag = 'delete' THEN
          DELETE
        WHEN NOT MATCHED THEN
          INSERT (id, value, flag)
          VALUES (s.id, s.value, s.flag)
        """
      Then query plan matches snapshot

    Scenario: Matched deletes use deletion vectors while unmatched rows are inserted
      Given statement
        """
        MERGE INTO delta_merge_dv AS t
        USING src_merge_dv AS s
        ON t.id = s.id
        WHEN MATCHED AND s.flag = 'delete' THEN
          DELETE
        WHEN NOT MATCHED THEN
          INSERT (id, value, flag)
          VALUES (s.id, s.value, s.flag)
        """
      Then delta log latest commit info matches snapshot
      Then delta log latest commit info contains
        | path                                               | value                  |
        | operation                                          | "MERGE"                |
        | operationParameters.mergePredicate                 | "t . id = s . id " |
        | operationParameters.matchedPredicates[0].actionType | "delete"               |
        | operationParameters.matchedPredicates[0].predicate  | "s . flag = 'delete' " |
        | operationParameters.notMatchedPredicates[0].actionType | "insert"            |
      Then file tree in location matches
        """
        📂 <hex-prefix>
          📄 deletion_vector_<uuid>.bin
        📄 part-<id>.<codec>.parquet
        📄 part-<id>.<codec>.parquet
        """
      When query
        """
        SELECT id, value, flag FROM delta_merge_dv ORDER BY id
        """
      Then query result ordered
        | id | value    | flag   |
        | 1  | keep     | target |
        | 3  | stay     | target |
        | 4  | inserted | insert |

    Scenario: Matched updates append replacement rows and a deletion vector
      Given statement
        """
        MERGE INTO delta_merge_dv AS t
        USING src_merge_dv AS s
        ON t.id = s.id
        WHEN MATCHED THEN
          UPDATE SET value = s.value
        """
      Then delta log latest commit info contains
        | path                                               | value    |
        | operation                                          | "MERGE"  |
        | operationParameters.matchedPredicates[0].actionType | "update" |
        | operationMetrics.numTargetRowsUpdated              | 1        |
        | operationMetrics.numTargetRowsMatchedUpdated       | 1        |
        | operationMetrics.numTargetRowsDeleted              | 0        |
        | operationMetrics.numTargetRowsCopied               | 0        |
        | operationMetrics.numTargetDeletionVectorsAdded     | 1        |
      Then data files in location count is 2
      Then file tree in location matches
        """
        📂 <hex-prefix>
          📄 deletion_vector_<uuid>.bin
        📄 part-<id>.<codec>.parquet
        📄 part-<id>.<codec>.parquet
        """
      When query
        """
        SELECT id, value, flag FROM delta_merge_dv ORDER BY id
        """
      Then query result ordered
        | id | value  | flag   |
        | 1  | keep   | target |
        | 2  | updated | target |
        | 3  | stay   | target |

    Scenario: Target-only updates append replacement rows and a deletion vector
      Given statement
        """
        MERGE INTO delta_merge_dv AS t
        USING src_merge_dv AS s
        ON t.id = s.id
        WHEN NOT MATCHED BY SOURCE AND t.id = 3 THEN
          UPDATE SET value = concat(t.value, '_stale')
        """
      Then delta log latest commit info contains
        | path                                                          | value    |
        | operation                                                     | "MERGE"  |
        | operationParameters.notMatchedBySourcePredicates[0].actionType | "update" |
        | operationMetrics.numTargetRowsUpdated                         | 1        |
        | operationMetrics.numTargetRowsNotMatchedBySourceUpdated       | 1        |
        | operationMetrics.numTargetRowsDeleted                         | 0        |
        | operationMetrics.numTargetRowsCopied                          | 0        |
        | operationMetrics.numTargetDeletionVectorsAdded                | 1        |
      Then data files in location count is 2
      Then file tree in location matches
        """
        📂 <hex-prefix>
          📄 deletion_vector_<uuid>.bin
        📄 part-<id>.<codec>.parquet
        📄 part-<id>.<codec>.parquet
        """
      When query
        """
        SELECT id, value, flag FROM delta_merge_dv ORDER BY id
        """
      Then query result ordered
        | id | value      | flag   |
        | 1  | keep       | target |
        | 2  | remove     | target |
        | 3  | stay_stale | target |

    Scenario: Updates and deletes share a deletion vector without mixing row metrics
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_merge_dv_mixed AS
        SELECT * FROM VALUES
          (1, 'updated',  'update'),
          (2, 'ignored',  'delete'),
          (4, 'inserted', 'insert')
        AS src(id, value, flag)
        """
      Given statement
        """
        MERGE INTO delta_merge_dv AS t
        USING src_merge_dv_mixed AS s
        ON t.id = s.id
        WHEN MATCHED AND s.flag = 'update' THEN
          UPDATE SET value = s.value
        WHEN MATCHED AND s.flag = 'delete' THEN
          DELETE
        WHEN NOT MATCHED THEN
          INSERT (id, value, flag) VALUES (s.id, s.value, s.flag)
        """
      Then delta log latest commit info contains
        | path                                               | value    |
        | operation                                          | "MERGE"  |
        | operationMetrics.numTargetRowsUpdated              | 1        |
        | operationMetrics.numTargetRowsDeleted              | 1        |
        | operationMetrics.numTargetRowsInserted             | 1        |
        | operationMetrics.numTargetRowsCopied               | 0        |
        | operationMetrics.numTargetDeletionVectorsAdded     | 1        |
        | operationParameters.matchedPredicates[0].actionType | "update" |
        | operationParameters.matchedPredicates[1].actionType | "delete" |
      Then data files in location count is at least 2
      When query
        """
        SELECT id, value, flag FROM delta_merge_dv ORDER BY id
        """
      Then query result ordered
        | id | value    | flag   |
        | 1  | updated  | target |
        | 3  | stay     | target |
        | 4  | inserted | insert |

    Scenario: Duplicate source matches for an unconditional DELETE invalidate one row index
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_merge_dv_duplicate_delete AS
        SELECT * FROM VALUES
          (2, 'first', 'delete'),
          (2, 'second', 'delete')
        AS src(id, value, flag)
        """
      Given statement
        """
        MERGE INTO delta_merge_dv AS t
        USING src_merge_dv_duplicate_delete AS s
        ON t.id = s.id
        WHEN MATCHED THEN DELETE
        """
      Then delta log latest commit info contains
        | path                                           | value   |
        | operation                                      | "MERGE" |
        | operationMetrics.numTargetRowsUpdated          | 0       |
        | operationMetrics.numTargetRowsDeleted          | 1       |
        | operationMetrics.numTargetRowsInserted         | 0       |
        | operationMetrics.numTargetDeletionVectorsAdded | 1       |
      When query
        """
        SELECT id, value, flag FROM delta_merge_dv ORDER BY id
        """
      Then query result ordered
        | id | value | flag   |
        | 1  | keep  | target |
        | 3  | stay  | target |

    Scenario: WITH SCHEMA EVOLUTION updates and inserts on a deletion-vector table
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_merge_dv_schema_evolution AS
        SELECT * FROM VALUES
          (2, 'updated', 'target', 20),
          (4, 'inserted', 'insert', 40)
        AS src(id, value, flag, extra)
        """
      Given statement
        """
        MERGE WITH SCHEMA EVOLUTION INTO delta_merge_dv AS t
        USING src_merge_dv_schema_evolution AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
      Then delta log latest commit info contains
        | path                                           | value   |
        | operation                                      | "MERGE" |
        | operationMetrics.numTargetRowsUpdated          | 1       |
        | operationMetrics.numTargetRowsInserted         | 1       |
        | operationMetrics.numTargetRowsDeleted          | 0       |
        | operationMetrics.numTargetDeletionVectorsAdded | 1       |
      When query
        """
        SELECT id, value, flag, extra
        FROM delta_merge_dv
        ORDER BY id
        """
      Then query result ordered
        | id | value    | flag   | extra |
        | 1  | keep     | target | NULL  |
        | 2  | updated  | target | 20    |
        | 3  | stay     | target | NULL  |
        | 4  | inserted | insert | 40    |

  Rule: Merge-on-Read MERGE with nullable join keys
    Background:
      Given variable location for temporary directory merge_dv_null_key
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_dv_null_key
        """
      Given statement template
        """
        CREATE TABLE delta_merge_dv_null_key (id INT, value STRING, flag STRING)
        USING DELTA LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_merge_dv_null_key VALUES
          (CAST(NULL AS INT), 'target-null', 'target'),
          (1, 'keep', 'target'),
          (2, 'old', 'target')
        """

    Scenario: NULL join keys remain unmatched during merge-on-read updates
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_merge_dv_null_key AS
        SELECT * FROM VALUES
          (CAST(NULL AS INT), 'source-null', 'insert'),
          (2, 'updated', 'target')
        AS src(id, value, flag)
        """
      Given statement
        """
        MERGE INTO delta_merge_dv_null_key AS t
        USING src_merge_dv_null_key AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value
        WHEN NOT MATCHED THEN INSERT *
        """
      Then delta log latest commit info contains
        | path                                           | value   |
        | operation                                      | "MERGE" |
        | operationMetrics.numTargetRowsUpdated          | 1       |
        | operationMetrics.numTargetRowsInserted         | 1       |
        | operationMetrics.numTargetRowsDeleted          | 0       |
        | operationMetrics.numTargetDeletionVectorsAdded | 1       |
      When query
        """
        SELECT id, value, flag
        FROM delta_merge_dv_null_key
        ORDER BY id NULLS FIRST, value
        """
      Then query result ordered
        | id   | value       | flag   |
        | NULL | source-null | insert |
        | NULL | target-null | target |
        | 1    | keep        | target |
        | 2    | updated     | target |

  Rule: Merge-on-Read MERGE across target files
    Background:
      Given config spark.sql.shuffle.partitions = 4
      Given variable location for temporary directory merge_dv_multi_file
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_dv_multi_file
        """
      Given statement template
        """
        CREATE TABLE delta_merge_dv_multi_file (
          id INT,
          value STRING,
          flag STRING,
          bucket INT
        )
        USING DELTA
        PARTITIONED BY (bucket)
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_merge_dv_multi_file VALUES
          (1, 'old-update', 'update', 0),
          (2, 'old-delete', 'delete', 1),
          (3, 'keep-two', 'keep', 2),
          (4, 'keep-three', 'keep', 3)
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_merge_dv_multi_file AS
        SELECT * FROM VALUES
          (1, 'new-update', 'update', 0),
          (2, 'ignored', 'delete', 1),
          (5, 'inserted', 'insert', 4)
        AS src(id, value, flag, bucket)
        """

    Scenario: Different files update and delete across multiple output partitions with exact metrics
      Given statement
        """
        MERGE INTO delta_merge_dv_multi_file AS t
        USING src_merge_dv_multi_file AS s
        ON t.id = s.id
        WHEN MATCHED AND s.flag = 'update' THEN UPDATE SET value = s.value
        WHEN MATCHED AND s.flag = 'delete' THEN DELETE
        WHEN NOT MATCHED THEN INSERT *
        """
      Then delta log latest commit info contains
        | path                                                   | value   |
        | operation                                              | "MERGE" |
        | operationMetrics.numTargetRowsUpdated                  | 1       |
        | operationMetrics.numTargetRowsMatchedUpdated           | 1       |
        | operationMetrics.numTargetRowsDeleted                  | 1       |
        | operationMetrics.numTargetRowsInserted                 | 1       |
        | operationMetrics.numTargetRowsCopied                   | 0       |
        | operationMetrics.numTargetDeletionVectorsAdded         | 2       |
        | operationMetrics.numTargetDeletionVectorsUpdated       | 0       |
        | operationMetrics.numTargetDeletionVectorsRemoved       | 0       |
      When query
        """
        SELECT id, value, flag, bucket
        FROM delta_merge_dv_multi_file
        ORDER BY id
        """
      Then query result ordered
        | id | value       | flag   | bucket |
        | 1  | new-update  | update | 0      |
        | 3  | keep-two    | keep   | 2      |
        | 4  | keep-three  | keep   | 3      |
        | 5  | inserted    | insert | 4      |


  Rule: Updates for rows not matched by source and explicit insert columns
    Background:
      Given variable location for temporary directory merge_extended
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_extended
        """
      Given statement template
        """
        CREATE TABLE delta_merge_extended (
          id INT,
          value STRING,
          flag STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_extended
        SELECT * FROM VALUES
          (1, 'old_keep', 'keep'),
          (2, 'old_update', 'update'),
          (3, 'old_orphan', 'orphan')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_merge_delta_ext AS
        SELECT * FROM VALUES
          (2, 'src_update', 'ignored'),
          (4, 'src_insert', 'inserted')
        AS src(id, value, flag)
        """

    Scenario: Apply not matched by source updates and insert expressions
      Given statement
        """
        MERGE INTO delta_merge_extended AS t
        USING src_merge_delta_ext AS s
        ON t.id = s.id
        WHEN MATCHED AND t.flag = 'update' THEN
          UPDATE SET value = concat(s.value, '_', t.flag),
                     flag = t.flag
        WHEN NOT MATCHED BY SOURCE AND t.flag = 'orphan' THEN
          UPDATE SET value = concat(t.value, '_orphan')
        WHEN NOT MATCHED THEN
          INSERT (id, value, flag)
          VALUES (s.id, concat(s.value, '_insert'), s.flag)
        """
      Then delta log latest commit info matches snapshot
      Then delta log latest commit info contains
        | path                                               | value                  |
        | operation                                          | "MERGE"                |
        | operationParameters.mergePredicate                 | "t . id = s . id " |
        | operationParameters.matchedPredicates[0].actionType | "update"               |
        | operationParameters.notMatchedBySourcePredicates[0].actionType | "update"      |
        | operationParameters.notMatchedPredicates[0].actionType | "insert"            |
      When query
        """
        SELECT id, value, flag FROM delta_merge_extended ORDER BY id
        """
      Then query result ordered
        | id | value               | flag     |
        | 1  | old_keep            | keep     |
        | 2  | src_update_update   | update   |
        | 3  | old_orphan_orphan   | orphan   |
        | 4  | src_insert_insert   | inserted |

  Rule: NOT MATCHED BY SOURCE clauses can remove stale rows
    Background:
      Given variable location for temporary directory merge_cleanup
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_cleanup
        """
      Given statement template
        """
        CREATE TABLE delta_merge_cleanup (
          id INT,
          value STRING,
          flag STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_cleanup
        SELECT * FROM VALUES
          (1, 'stable', 'keep'),
          (2, 'archived', 'archive'),
          (3, 'legacy', 'trim')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_merge_cleanup AS
        SELECT * FROM VALUES
          (2, 'incoming', 'keep'),
          (4, 'brand_new', 'insert')
        AS src(id, value, flag)
        """

    Scenario: Rows missing from the source are deleted after updates
      Given statement
        """
        MERGE INTO delta_merge_cleanup AS t
        USING src_merge_cleanup AS s
        ON t.id = s.id
        WHEN MATCHED AND t.flag = 'archive' THEN
          UPDATE SET value = concat('refreshed_', s.value),
                     flag = 'keep'
        WHEN NOT MATCHED BY SOURCE AND t.flag = 'trim' THEN
          DELETE
        WHEN NOT MATCHED THEN
          INSERT (id, value, flag)
          VALUES (s.id, concat('insert_', s.value), s.flag)
        """
      Then delta log latest commit info matches snapshot
      Then delta log latest commit info contains
        | path                                               | value                  |
        | operation                                          | "MERGE"                |
        | operationParameters.mergePredicate                 | "t . id = s . id " |
        | operationParameters.matchedPredicates[0].actionType | "update"               |
        | operationParameters.notMatchedBySourcePredicates[0].actionType | "delete"      |
        | operationParameters.notMatchedPredicates[0].actionType | "insert"            |
      When query
        """
        SELECT id, value, flag FROM delta_merge_cleanup ORDER BY id
        """
      Then query result ordered
        | id | value              | flag   |
        | 1  | stable             | keep   |
        | 2  | refreshed_incoming | keep   |
        | 4  | insert_brand_new   | insert |

  Rule: Conditional inserts select between multiple NOT MATCHED branches
    Background:
      Given variable location for temporary directory merge_conditional
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_conditional
        """
      Given statement template
        """
        CREATE TABLE delta_merge_conditional (
          id INT,
          category STRING,
          amount INT,
          note STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_conditional
        SELECT * FROM VALUES
          (1, 'existing', 10, 'keep'),
          (2, 'vip', 20, 'gold')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_merge_conditional AS
        SELECT * FROM VALUES
          (1, 'existing', 15, 'promote'),
          (3, 'vip', 30, 'priority'),
          (4, 'standard', 25, 'regular')
        AS src(id, category, amount, note)
        """

    Scenario: Specific NOT MATCHED inserts run before the default clause
      Given statement
        """
        MERGE INTO delta_merge_conditional AS t
        USING src_merge_conditional AS s
        ON t.id = s.id
        WHEN MATCHED THEN
          UPDATE SET amount = s.amount,
                     note = concat('updated_', s.note)
        WHEN NOT MATCHED AND s.id = 3 THEN
          INSERT (id, category, amount, note)
          VALUES (s.id, s.category, s.amount * 10, concat('priority_', s.note))
        WHEN NOT MATCHED THEN
          INSERT (id, category, amount, note)
          VALUES (s.id, s.category, s.amount, concat('default_', s.note))
        """
      Then delta log latest commit info matches snapshot
      Then delta log latest commit info contains
        | path                                               | value                   |
        | operation                                          | "MERGE"                 |
        | operationParameters.mergePredicate                 | "t . id = s . id "  |
        | operationParameters.notMatchedPredicates[0].actionType | "insert"             |
        | operationParameters.notMatchedPredicates[0].predicate | "s . id = 3 " |
      When query
        """
        SELECT id, category, amount, note FROM delta_merge_conditional ORDER BY id
        """
      Then query result ordered
        | id | category | amount | note              |
        | 1  | existing | 15     | updated_promote   |
        | 2  | vip      | 20     | gold              |
        | 3  | vip      | 300    | priority_priority |
        | 4  | standard | 25     | default_regular   |

  Rule: EXPLAIN CODEGEN shows stepwise optimization for MERGE
    Background:
      Given variable location for temporary directory merge_explain_codegen
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_explain_codegen
        """
      Given statement template
        """
        CREATE TABLE delta_merge_explain_codegen (
          id INT,
          category STRING,
          amount INT,
          note STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_merge_explain_codegen
        SELECT * FROM VALUES
          (1, 'existing', 10, 'keep'),
          (2, 'vip', 20, 'gold'),
          (3, 'stale', 30, 'old')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_merge_explain_codegen AS
        SELECT
          id,
          category,
          amount,
          note
        FROM VALUES
          (1, 'existing', 15, 'promote'),
          (2, 'vip', 22, 'platinum'),
          (4, 'standard', 25, 'regular')
        AS src(id, category, amount, note)
        """

    Scenario: EXPLAIN CODEGEN includes plan steps and merge rewrite artifacts
      When query
        """
        EXPLAIN CODEGEN
        MERGE INTO delta_merge_explain_codegen AS t
        USING (
          SELECT *
          FROM src_merge_explain_codegen
          WHERE amount + 1 > 10 AND (id = 1 OR id = 2 OR id = 4)
        ) AS s
        ON t.id = s.id AND t.category IS NOT NULL
        WHEN MATCHED AND t.category = 'vip' THEN
          UPDATE SET amount = s.amount + 1,
                     note = concat(s.note, '_', t.id)
        WHEN MATCHED AND t.category = 'stale' THEN
          DELETE
        WHEN NOT MATCHED BY SOURCE AND t.category = 'stale' THEN
          UPDATE SET note = concat(t.note, '_orphan')
        WHEN NOT MATCHED THEN
          INSERT (id, category, amount, note)
          VALUES (s.id, s.category, s.amount, concat('insert_', s.note))
        """
      Then query plan matches snapshot
      Given statement
        """
        MERGE INTO delta_merge_explain_codegen AS t
        USING (
          SELECT *
          FROM src_merge_explain_codegen
          WHERE amount + 1 > 10 AND (id = 1 OR id = 2 OR id = 4)
        ) AS s
        ON t.id = s.id AND t.category IS NOT NULL
        WHEN MATCHED AND t.category = 'vip' THEN
          UPDATE SET amount = s.amount + 1,
                     note = concat(s.note, '_', t.id)
        WHEN MATCHED AND t.category = 'stale' THEN
          DELETE
        WHEN NOT MATCHED BY SOURCE AND t.category = 'stale' THEN
          UPDATE SET note = concat(t.note, '_orphan')
        WHEN NOT MATCHED THEN
          INSERT (id, category, amount, note)
          VALUES (s.id, s.category, s.amount, concat('insert_', s.note))
        """
      When query
        """
        SELECT id, category, amount, note FROM delta_merge_explain_codegen ORDER BY id
        """
      Then query result ordered
        | id | category | amount | note           |
        | 1  | existing | 10     | keep           |
        | 2  | vip      | 23     | platinum_2     |
        | 3  | stale    | 30     | old_orphan     |
        | 4  | standard | 25     | insert_regular |


  Rule: MERGE target partition pruning respects target-only ON predicates
    Background:
      Given variable location for temporary directory merge_explain_partition_pushdown
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_explain_partition_pushdown
        """
      Given final statement
        """
        DROP VIEW IF EXISTS src_merge_explain_partition_pushdown
        """
      Given statement template
        """
        CREATE TABLE delta_merge_explain_partition_pushdown (
          id INT,
          year INT,
          value INT
        )
        USING DELTA LOCATION {{ location.sql }}
        PARTITIONED BY (year)
        """
      Given statement
        """
        INSERT INTO delta_merge_explain_partition_pushdown
        SELECT * FROM VALUES
          (1, 2023, 100),
          (2, 2024, 200),
          (3, 2024, 700),
          (5, 2024, 100)
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_merge_explain_partition_pushdown AS
        SELECT * FROM VALUES
          (2, 2024, 250),
          (4, 2024, 900)
        AS src(id, year, value)
        """

    Scenario: EXPLAIN prunes target files using a target-only ON predicate
      When query
        """
        EXPLAIN
        MERGE INTO delta_merge_explain_partition_pushdown AS t
        USING src_merge_explain_partition_pushdown AS s
        ON t.id = s.id AND t.year = 2024 AND t.value > 150
        WHEN MATCHED THEN UPDATE SET value = s.value
        WHEN NOT MATCHED THEN INSERT (id, year, value) VALUES (s.id, s.year, s.value)
        """
      Then query plan matches snapshot
      Given statement
        """
        MERGE INTO delta_merge_explain_partition_pushdown AS t
        USING src_merge_explain_partition_pushdown AS s
        ON t.id = s.id AND t.year = 2024 AND t.value > 150
        WHEN MATCHED THEN UPDATE SET value = s.value
        WHEN NOT MATCHED THEN INSERT (id, year, value) VALUES (s.id, s.year, s.value)
        """
      When query
        """
        SELECT id, year, value FROM delta_merge_explain_partition_pushdown ORDER BY id
        """
      Then query result ordered
        | id | year | value |
        | 1  | 2023 | 100   |
        | 2  | 2024 | 250   |
        | 3  | 2024 | 700   |
        | 4  | 2024 | 900   |
        | 5  | 2024 | 100   |

    Scenario: EXPLAIN prunes target files for a path-based Delta target
      When query template
        """
        EXPLAIN
        MERGE INTO delta.`{{ location.string }}` AS t
        USING src_merge_explain_partition_pushdown AS s
        ON t.id = s.id AND t.year = 2024 AND t.value > 150
        WHEN MATCHED THEN UPDATE SET value = s.value
        WHEN NOT MATCHED THEN INSERT (id, year, value) VALUES (s.id, s.year, s.value)
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN keeps all target files when NOT MATCHED BY SOURCE can act
      When query
        """
        EXPLAIN
        MERGE INTO delta_merge_explain_partition_pushdown AS t
        USING src_merge_explain_partition_pushdown AS s
        ON t.id = s.id AND t.year = 2024
        WHEN MATCHED THEN UPDATE SET value = s.value
        WHEN NOT MATCHED BY SOURCE THEN UPDATE SET value = t.value + 1
        """
      Then query plan matches snapshot
      Given statement
        """
        MERGE INTO delta_merge_explain_partition_pushdown AS t
        USING src_merge_explain_partition_pushdown AS s
        ON t.id = s.id AND t.year = 2024
        WHEN MATCHED THEN UPDATE SET value = s.value
        WHEN NOT MATCHED BY SOURCE THEN UPDATE SET value = t.value + 1
        """
      When query
        """
        SELECT id, year, value FROM delta_merge_explain_partition_pushdown ORDER BY id
        """
      Then query result ordered
        | id | year | value |
        | 1  | 2023 | 101   |
        | 2  | 2024 | 250   |
        | 3  | 2024 | 701   |
        | 5  | 2024 | 101   |

    Scenario: Non-partition target predicates do not filter rows before file rewrite
      Given statement
        """
        MERGE INTO delta_merge_explain_partition_pushdown AS t
        USING src_merge_explain_partition_pushdown AS s
        ON t.id = s.id AND t.value > 150
        WHEN MATCHED THEN UPDATE SET value = s.value
        """
      When query
        """
        SELECT id, year, value FROM delta_merge_explain_partition_pushdown ORDER BY id
        """
      Then query result ordered
        | id | year | value |
        | 1  | 2023 | 100   |
        | 2  | 2024 | 250   |
        | 3  | 2024 | 700   |
        | 5  | 2024 | 100   |


  Rule: MERGE target partition pruning applies to Merge-on-Read deletes
    Background:
      Given variable location for temporary directory merge_dv_partition_pruning
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_dv_partition_pruning
        """
      Given final statement
        """
        DROP VIEW IF EXISTS src_merge_dv_partition_pruning
        """
      Given statement template
        """
        CREATE TABLE delta_merge_dv_partition_pruning (
          id INT,
          year INT,
          value INT
        )
        USING DELTA
        PARTITIONED BY (year)
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_merge_dv_partition_pruning
        SELECT * FROM VALUES
          (1, 2023, 100),
          (2, 2024, 200),
          (3, 2024, 700),
          (5, 2024, 100)
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_merge_dv_partition_pruning AS
        SELECT * FROM VALUES
          (2, 2024, 250)
        AS src(id, year, value)
        """

    Scenario: EXPLAIN prunes partitioned Merge-on-Read target files
      When query
        """
        EXPLAIN
        MERGE INTO delta_merge_dv_partition_pruning AS t
        USING src_merge_dv_partition_pruning AS s
        ON t.id = s.id AND t.year = 2024
        WHEN MATCHED THEN DELETE
        """
      Then query plan matches snapshot

    Scenario: Partition-pruned Merge-on-Read DELETE preserves untouched rows
      Given statement
        """
        MERGE INTO delta_merge_dv_partition_pruning AS t
        USING src_merge_dv_partition_pruning AS s
        ON t.id = s.id AND t.year = 2024
        WHEN MATCHED THEN DELETE
        """
      Then delta log latest commit info contains
        | path                                               | value    |
        | operation                                          | "MERGE"  |
        | operationMetrics.numTargetDeletionVectorsAdded     | 1        |
        | operationMetrics.numTargetRowsDeleted              | 1        |
        | operationParameters.matchedPredicates[0].actionType | "delete" |
      Then data files in location count is 2
      Then file tree in location matches
        """
        📂 <hex-prefix>
          📄 deletion_vector_<uuid>.bin
        📂 year=2023
          📄 part-<id>.<codec>.parquet
        📂 year=2024
          📄 part-<id>.<codec>.parquet
        """
      When query
        """
        SELECT id, year, value FROM delta_merge_dv_partition_pruning ORDER BY id
        """
      Then query result ordered
        | id | year | value |
        | 1  | 2023 | 100   |
        | 3  | 2024 | 700   |
        | 5  | 2024 | 100   |


  Rule: MERGE INTO with path-based target (delta.`/path/to/table` syntax)
    Background:
      Given variable location for temporary directory merge_path_target
      Given final statement
        """
        DROP TABLE IF EXISTS delta_merge_path_target
        """
      Given final statement
        """
        DROP VIEW IF EXISTS src_merge_path_target
        """
      Given statement template
        """
        CREATE TABLE delta_merge_path_target (
          id INT,
          value STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.constraints.positive_id' = 'id > 0')
        """
      Given statement
        """
        INSERT INTO delta_merge_path_target
        SELECT * FROM VALUES
          (1, 'old'),
          (2, 'keep')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_merge_path_target AS
        SELECT * FROM VALUES
          (1, 'new'),
          (3, 'insert')
        AS src(id, value)
        """

    Scenario: MERGE INTO with delta.`path` target using a temp view as source
      Given statement template
        """
        MERGE INTO delta.`{{ location.string }}` AS tgt
        USING src_merge_path_target AS src
        ON tgt.id = src.id
        WHEN MATCHED THEN UPDATE SET tgt.value = src.value
        WHEN NOT MATCHED THEN INSERT *
        """
      When query
        """
        SELECT id, value FROM delta_merge_path_target ORDER BY id
        """
      Then query result ordered
        | id | value  |
        | 1  | new    |
        | 2  | keep   |
        | 3  | insert |

    Scenario: MERGE INTO with delta.`path` target enforces CHECK constraints
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_merge_path_target_bad AS
        SELECT * FROM VALUES
          (0, 'bad')
        AS src(id, value)
        """
      When query template
        """
        MERGE INTO delta.`{{ location.string }}` AS tgt
        USING src_merge_path_target_bad AS src
        ON tgt.id = src.id
        WHEN NOT MATCHED THEN INSERT *
        """
      Then query error DELTA_VIOLATE_CONSTRAINT_WITH_VALUES
