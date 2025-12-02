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
      When query
        """
        SELECT id, value, flag FROM delta_merge_basic ORDER BY id
        """
      Then query result ordered
        | id | value | flag   |
        | 1  | old   | keep   |
        | 2  | new   | update |
        | 4  | ins   | insert |

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
          (CAST(1 AS INT), 'existing', CAST(15 AS INT), 'promote'),
          (CAST(3 AS INT), 'vip', CAST(30 AS INT), 'priority'),
          (CAST(4 AS INT), 'standard', CAST(25 AS INT), 'regular')
        AS src(id, category, amount, note)
        """

    Scenario: Specific NOT MATCHED inserts run before the default clause
      Given statement
        """
        MERGE INTO delta_merge_conditional AS t
        USING src_merge_conditional AS s
        ON t.id = CAST(s.id AS INT)
        WHEN MATCHED THEN
          UPDATE SET amount = CAST(s.amount AS INT),
                     note = concat('updated_', s.note)
        WHEN NOT MATCHED AND CAST(s.id AS STRING) = '3' THEN
          INSERT (id, category, amount, note)
          VALUES (CAST(s.id AS INT), s.category, CAST(s.amount AS INT) * 10, concat('priority_', s.note))
        WHEN NOT MATCHED THEN
          INSERT (id, category, amount, note)
          VALUES (CAST(s.id AS INT), s.category, CAST(s.amount AS INT), concat('default_', s.note))
        """
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
