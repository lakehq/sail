Feature: Delta Lake Merge

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
