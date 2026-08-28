Feature: Delta Lake Update

  Rule: Copy-on-write updates
    Background:
      Given variable location for temporary directory delta_update
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_basic
        """
      Given statement template
        """
        CREATE TABLE delta_update_basic (
          id INT,
          value INT,
          previous_value INT,
          label STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_update_basic
        SELECT * FROM VALUES
          (1, 10, 100, 'keep'),
          (2, 20, 200, 'change'),
          (3, 30, 300, 'change')
        """

    Scenario: Conditional UPDATE rewrites changed rows and copies the rest of each touched file
      Given statement
        """
        UPDATE delta_update_basic AS target
        SET value = target.value + 5,
            label = concat(target.label, '-updated')
        WHERE target.id >= 2
        """
      Then delta log latest commit info contains
        | path                              | value               |
        | operation                         | "UPDATE"            |
        | operationParameters.predicate     | "target . id >= 2 " |
        | operationMetrics.numUpdatedRows   | 2                   |
        | operationMetrics.numCopiedRows    | 1                   |
      When query
        """
        SELECT id, value, previous_value, label
        FROM delta_update_basic
        ORDER BY id
        """
      Then query result ordered
        | id | value | previous_value | label          |
        | 1  | 10    | 100            | keep           |
        | 2  | 25    | 200            | change-updated |
        | 3  | 35    | 300            | change-updated |

    Scenario: UPDATE assignments use the original row values
      Given statement
        """
        UPDATE delta_update_basic
        SET value = previous_value,
            previous_value = value
        WHERE id = 1
        """
      When query
        """
        SELECT id, value, previous_value, label
        FROM delta_update_basic
        ORDER BY id
        """
      Then query result ordered
        | id | value | previous_value | label  |
        | 1  | 100   | 10             | keep   |
        | 2  | 20    | 200            | change |
        | 3  | 30    | 300            | change |

    Scenario: UPDATE validates catalog and table qualifiers on assignment targets
      Given statement
        """
        UPDATE default.delta_update_basic
        SET default.delta_update_basic.value = 11
        WHERE id = 1
        """
      When query
        """
        UPDATE delta_update_basic
        SET bogus.value = 99
        WHERE id = 1
        """
      Then query error Cannot resolve UPDATE target column `bogus.value`
      When query
        """
        SELECT id, value FROM delta_update_basic ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | 11    |
        | 2  | 20    |
        | 3  | 30    |

    Scenario: UPDATE without a predicate changes every row
      Given statement
        """
        UPDATE delta_update_basic
        SET value = value * 2
        """
      When query
        """
        SELECT id, value, previous_value, label
        FROM delta_update_basic
        ORDER BY id
        """
      Then query result ordered
        | id | value | previous_value | label  |
        | 1  | 20    | 100            | keep   |
        | 2  | 40    | 200            | change |
        | 3  | 60    | 300            | change |

    Scenario: UPDATE predicates preserve rows for which the predicate is unknown
      Given statement
        """
        UPDATE delta_update_basic
        SET label = 'updated'
        WHERE NULLIF(id, 1) > 1
        """
      When query
        """
        SELECT id, label FROM delta_update_basic ORDER BY id
        """
      Then query result ordered
        | id | label   |
        | 1  | keep    |
        | 2  | updated |
        | 3  | updated |

    Scenario: UPDATE with no matching rows does not create a commit or rewrite data files
      Given statement
        """
        UPDATE delta_update_basic
        SET value = value + 1
        WHERE id = 99
        """
      Then delta log latest commit info contains
        | path      | value   |
        | operation | "WRITE" |
      Then data files in location count is 1
      When query
        """
        SELECT id, value FROM delta_update_basic ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | 10    |
        | 2  | 20    |
        | 3  | 30    |

    Scenario: UPDATE rejects a non-deterministic predicate before writing
      When query
        """
        UPDATE delta_update_basic
        SET label = 'unstable'
        WHERE id <= 2 AND rand() > 0.5
        """
      Then query error Non-deterministic expressions are not allowed in UPDATE conditions
      When query
        """
        SELECT id, label FROM delta_update_basic ORDER BY id
        """
      Then query result ordered
        | id | label  |
        | 1  | keep   |
        | 2  | change |
        | 3  | change |

    Scenario: EXPLAIN CODEGEN shows one targeted UPDATE rewrite
      When query
        """
        EXPLAIN CODEGEN
        UPDATE delta_update_basic
        SET value = value + 1
        WHERE id = 2
        """
      Then query plan matches snapshot

  Rule: Partitioned copy-on-write updates

    Scenario: UPDATE rewrites only the matching partition and preserves partition values
      Given variable location for temporary directory delta_update_partitioned
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_partitioned
        """
      Given statement template
        """
        CREATE TABLE delta_update_partitioned (
          id INT,
          value STRING,
          category STRING
        )
        USING DELTA
        PARTITIONED BY (category)
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_update_partitioned VALUES
          (1, 'old', 'a'),
          (2, 'keep-a', 'a'),
          (3, 'keep-b', 'b')
        """
      Given statement
        """
        UPDATE delta_update_partitioned
        SET value = 'new'
        WHERE id = 1 AND category = 'a'
        """
      Then delta log latest commit info contains
        | path                            | value    |
        | operation                       | "UPDATE" |
        | operationMetrics.numUpdatedRows | 1        |
        | operationMetrics.numCopiedRows  | 1        |
      When query
        """
        SELECT id, value, category
        FROM delta_update_partitioned
        ORDER BY id
        """
      Then query result ordered
        | id | value  | category |
        | 1  | new    | a        |
        | 2  | keep-a | a        |
        | 3  | keep-b | b        |

  Rule: Path-based update targets
    Background:
      Given variable location for temporary directory delta_update_path
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_path
        """
      Given statement template
        """
        CREATE TABLE delta_update_path (id INT, value STRING)
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_update_path VALUES (1, 'old'), (2, 'keep')
        """

    Scenario: UPDATE accepts a delta path target and alias
      Given statement template
        """
        UPDATE delta.`{{ location.string }}` AS target
        SET value = concat(target.value, '-updated')
        WHERE target.id = 1
        """
      When query
        """
        SELECT id, value FROM delta_update_path ORDER BY id
        """
      Then query result ordered
        | id | value       |
        | 1  | old-updated |
        | 2  | keep        |

  Rule: Delta invariants are enforced by UPDATE

    Scenario: UPDATE nested fields preserves sibling values
      Given variable location for temporary directory delta_update_nested
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_nested
        """
      Given statement template
        """
        CREATE TABLE delta_update_nested (
          id INT,
          payload STRUCT<a: INT, b: STRING>
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_update_nested
        VALUES (1, named_struct('a', 10, 'b', 'keep'))
        """
      Given statement
        """
        UPDATE delta_update_nested AS target
        SET target.payload.a = 11
        WHERE target.id = 1
        """
      When query
        """
        SELECT id, payload.a, payload.b FROM delta_update_nested
        """
      Then query result
        | id | a  | b    |
        | 1  | 11 | keep |
      Given statement
        """
        UPDATE delta_update_nested
        SET payload = named_struct('b', 'reordered', 'a', 12)
        WHERE id = 1
        """
      When query
        """
        SELECT id, payload.a, payload.b FROM delta_update_nested
        """
      Then query result
        | id | a  | b         |
        | 1  | 12 | reordered |

    Scenario: UPDATE rejects incompatible assignment types before writing
      Given variable location for temporary directory delta_update_type_check
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_type_check
        """
      Given statement template
        """
        CREATE TABLE delta_update_type_check (id INT, value INT)
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_update_type_check VALUES (1, 10)
        """
      When query
        """
        UPDATE delta_update_type_check SET value = 'not-an-integer' WHERE id = 1
        """
      Then query error CANNOT_SAFELY_CAST
      When query
        """
        SELECT id, value FROM delta_update_type_check
        """
      Then query result
        | id | value |
        | 1  | 10    |

    Scenario: UPDATE casts compatible assignments and rejects overflow
      Given config spark.sql.ansi.enabled = true
      Given variable location for temporary directory delta_update_assignment_cast
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_assignment_cast
        """
      Given statement template
        """
        CREATE TABLE delta_update_assignment_cast (id INT, value INT)
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_update_assignment_cast VALUES (1, 10)
        """
      Given statement
        """
        UPDATE delta_update_assignment_cast
        SET value = CAST(20 AS BIGINT)
        WHERE id = 1
        """
      When query
        """
        SELECT id, value FROM delta_update_assignment_cast
        """
      Then query result
        | id | value |
        | 1  | 20    |
      When query
        """
        UPDATE delta_update_assignment_cast
        SET value = CAST(2147483648 AS BIGINT)
        WHERE id = 1
        """
      Then query error (?i).*(cast|overflow).*
      When query
        """
        SELECT id, value FROM delta_update_assignment_cast
        """
      Then query result
        | id | value |
        | 1  | 20    |

    Scenario: UPDATE rejects duplicate target assignments
      Given variable location for temporary directory delta_update_duplicate
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_duplicate
        """
      Given statement template
        """
        CREATE TABLE delta_update_duplicate (id INT, value STRING)
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_update_duplicate VALUES (1, 'old')
        """
      When query
        """
        UPDATE delta_update_duplicate
        SET value = 'first', value = 'second'
        WHERE id = 1
        """
      Then query error assigns column 'value' more than once

    Scenario: UPDATE DEFAULT uses the target default or NULL
      Given variable location for temporary directory delta_update_default
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_default
        """
      Given statement template
        """
        CREATE TABLE delta_update_default (
          id INT,
          status STRING DEFAULT 'new',
          note STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_update_default VALUES (1, 'old', 'old')
        """
      Given statement
        """
        UPDATE delta_update_default
        SET status = DEFAULT, note = DEFAULT
        WHERE id = 1
        """
      When query
        """
        SELECT id, status, note FROM delta_update_default
        """
      Then query result
        | id | status | note |
        | 1  | new    | NULL |

    Scenario: UPDATE recomputes generated columns
      Given variable location for temporary directory delta_update_generated
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_generated
        """
      Given statement template
        """
        CREATE TABLE delta_update_generated (
          id INT,
          event_time TIMESTAMP,
          event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_update_generated (id, event_time)
        VALUES (1, TIMESTAMP '2024-01-01 00:00:00')
        """
      Given statement
        """
        UPDATE delta_update_generated
        SET event_time = TIMESTAMP '2024-09-01 00:00:00'
        WHERE id = 1
        """
      When query
        """
        SELECT id, event_time, event_date FROM delta_update_generated
        """
      Then query result
        | id | event_time          | event_date |
        | 1  | 2024-09-01 00:00:00 | 2024-09-01 |

    Scenario: UPDATE validates explicitly assigned generated columns
      Given variable location for temporary directory delta_update_explicit_generated
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_explicit_generated
        """
      Given statement template
        """
        CREATE TABLE delta_update_explicit_generated (
          id INT,
          event_time TIMESTAMP,
          event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_update_explicit_generated (id, event_time)
        VALUES (1, TIMESTAMP '2024-01-01 00:00:00')
        """
      Given statement
        """
        UPDATE delta_update_explicit_generated
        SET event_time = TIMESTAMP '2024-02-01 00:00:00',
            event_date = DATE '2024-02-01'
        WHERE id = 1
        """
      When query
        """
        UPDATE delta_update_explicit_generated
        SET event_time = TIMESTAMP '2024-03-01 00:00:00',
            event_date = DATE '2024-03-02'
        WHERE id = 1
        """
      Then query error DELTA_GENERATED_COLUMNS_VALUE_MISMATCH
      Then data files in location count is 2
      When query
        """
        SELECT id, event_time, event_date
        FROM delta_update_explicit_generated
        """
      Then query result
        | id | event_time          | event_date |
        | 1  | 2024-02-01 00:00:00 | 2024-02-01 |

    Scenario: UPDATE rejects target columns that collide with row-level metadata
      Given variable location for temporary directory delta_update_internal_column
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_internal_column
        """
      Given statement template
        """
        CREATE TABLE delta_update_internal_column (
          id INT,
          `__sail_operation_type` STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      When query
        """
        UPDATE delta_update_internal_column
        SET `__sail_operation_type` = 'new'
        WHERE id = 1
        """
      Then query error reserved internal column name

    Scenario: UPDATE rejects CHECK constraint violations without changing the table
      Given variable location for temporary directory delta_update_constraint
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_constraint
        """
      Given statement template
        """
        CREATE TABLE delta_update_constraint (id INT, value STRING)
        USING DELTA LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.constraints.positive_id' = 'id > 0')
        """
      Given statement
        """
        INSERT INTO delta_update_constraint VALUES (1, 'old')
        """
      When query
        """
        UPDATE delta_update_constraint SET id = 0 WHERE id = 1
        """
      Then query error DELTA_VIOLATE_CONSTRAINT_WITH_VALUES
      Then delta log latest commit info contains
        | path      | value   |
        | operation | "WRITE" |
      Then data files in location count is 1
      When query
        """
        SELECT id, value FROM delta_update_constraint
        """
      Then query result
        | id | value |
        | 1  | old   |

  Rule: Merge-on-read updates on deletion-vector tables

    Scenario: EXPLAIN UPDATE on a DV-enabled table writes changed rows and deletion vectors
      Given variable location for temporary directory delta_update_dv_explain
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_dv_explain
        """
      Given statement template
        """
        CREATE TABLE delta_update_dv_explain (id INT, value STRING)
        USING DELTA LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_update_dv_explain VALUES (1, 'old'), (2, 'keep')
        """
      When query
        """
        EXPLAIN
        UPDATE delta_update_dv_explain SET value = 'new' WHERE id = 1
        """
      Then query plan matches snapshot

    Scenario: UPDATE writes a deletion vector without copying unaffected rows
      Given variable location for temporary directory delta_update_dv
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_dv
        """
      Given statement template
        """
        CREATE TABLE delta_update_dv (id INT, value STRING)
        USING DELTA LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_update_dv VALUES (1, 'old'), (2, 'keep')
        """
      Given statement
        """
        UPDATE delta_update_dv SET value = 'new' WHERE id = 1
        """
      Then delta log latest commit info contains
        | path                                         | value    |
        | operation                                    | "UPDATE" |
        | operationMetrics.numUpdatedRows              | 1        |
        | operationMetrics.numCopiedRows               | 0        |
        | operationMetrics.numTouchedRows              | 1        |
        | operationMetrics.numDeletionVectorsAdded     | 1        |
        | operationMetrics.numDeletionVectorsRemoved   | 0        |
        | operationMetrics.numDeletionVectorsUpdated   | 0        |
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
        SELECT id, value FROM delta_update_dv ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | new   |
        | 2  | keep  |

    Scenario: UPDATE merge-on-read accumulates an existing deletion vector
      Given variable location for temporary directory delta_update_existing_dv
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_existing_dv
        """
      Given statement template
        """
        CREATE TABLE delta_update_existing_dv (id INT, value STRING)
        USING DELTA LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_update_existing_dv
        VALUES (1, 'old'), (2, 'keep'), (3, 'delete')
        """
      Given statement
        """
        DELETE FROM delta_update_existing_dv WHERE id = 3
        """
      Given statement
        """
        UPDATE delta_update_existing_dv SET value = 'new' WHERE id = 1
        """
      Then delta log latest commit info contains
        | path                                         | value    |
        | operation                                    | "UPDATE" |
        | operationMetrics.numUpdatedRows              | 1        |
        | operationMetrics.numCopiedRows               | 0        |
        | operationMetrics.numTouchedRows              | 1        |
        | operationMetrics.numDeletionVectorsAdded     | 1        |
        | operationMetrics.numDeletionVectorsRemoved   | 1        |
        | operationMetrics.numDeletionVectorsUpdated   | 1        |
      When query
        """
        SELECT id, value FROM delta_update_existing_dv ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | new   |
        | 2  | keep  |

    Scenario: UPDATE merge-on-read with no matching rows is a no-op
      Given variable location for temporary directory delta_update_dv_noop
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_dv_noop
        """
      Given statement template
        """
        CREATE TABLE delta_update_dv_noop (id INT, value STRING)
        USING DELTA LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_update_dv_noop VALUES (1, 'keep'), (2, 'stay')
        """
      Given statement
        """
        UPDATE delta_update_dv_noop SET value = 'never-written' WHERE id = 99
        """
      Then delta log latest commit info contains
        | path      | value   |
        | operation | "WRITE" |
      Then data files in location count is 1
      When query
        """
        SELECT id, value FROM delta_update_dv_noop ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | keep  |
        | 2  | stay  |

    Scenario: UPDATE merge-on-read writes a partition-column replacement into its new partition
      Given variable location for temporary directory delta_update_dv_partition_move
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_dv_partition_move
        """
      Given statement template
        """
        CREATE TABLE delta_update_dv_partition_move (
          id INT,
          value STRING,
          bucket INT
        )
        USING DELTA
        PARTITIONED BY (bucket)
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_update_dv_partition_move VALUES
          (1, 'old', 0),
          (2, 'keep-zero', 0),
          (3, 'keep-one', 1)
        """
      Given statement
        """
        UPDATE delta_update_dv_partition_move
        SET value = 'moved', bucket = 2
        WHERE id = 1
        """
      Then delta log latest commit info contains
        | path                                       | value    |
        | operation                                  | "UPDATE" |
        | operationMetrics.numUpdatedRows            | 1        |
        | operationMetrics.numCopiedRows             | 0        |
        | operationMetrics.numDeletionVectorsAdded   | 1        |
      Then data files in location count is 3
      When query
        """
        SELECT id, value, bucket
        FROM delta_update_dv_partition_move
        ORDER BY id
        """
      Then query result ordered
        | id | value     | bucket |
        | 1  | moved     | 2      |
        | 2  | keep-zero | 0      |
        | 3  | keep-one  | 1      |

    Scenario: UPDATE merge-on-read applies defaults and generated columns while enforcing CHECK constraints
      Given variable location for temporary directory delta_update_dv_invariants
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_dv_invariants
        """
      Given statement template
        """
        CREATE TABLE delta_update_dv_invariants (
          id INT,
          event_time TIMESTAMP,
          status STRING DEFAULT 'new',
          event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
        )
        USING DELTA LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'true',
          'delta.constraints.positive_id' = 'id > 0'
        )
        """
      Given statement
        """
        INSERT INTO delta_update_dv_invariants (id, event_time, status)
        VALUES (1, TIMESTAMP '2024-01-01 00:00:00', 'old')
        """
      Given statement
        """
        UPDATE delta_update_dv_invariants
        SET event_time = TIMESTAMP '2024-09-01 00:00:00',
            status = DEFAULT
        WHERE id = 1
        """
      Then delta log latest commit info contains
        | path                                       | value    |
        | operation                                  | "UPDATE" |
        | operationMetrics.numUpdatedRows            | 1        |
        | operationMetrics.numDeletionVectorsAdded   | 1        |
      When query
        """
        SELECT id, event_time, status, event_date
        FROM delta_update_dv_invariants
        """
      Then query result
        | id | event_time          | status | event_date |
        | 1  | 2024-09-01 00:00:00 | new    | 2024-09-01 |
      When query
        """
        INSERT INTO delta_update_dv_invariants (id, event_time)
        VALUES (0, TIMESTAMP '2024-10-01 00:00:00')
        """
      Then query error DELTA_VIOLATE_CONSTRAINT_WITH_VALUES
      Then data files in location count is 2
      When query
        """
        SELECT id, event_time, status, event_date
        FROM delta_update_dv_invariants
        """
      Then query result
        | id | event_time          | status | event_date |
        | 1  | 2024-09-01 00:00:00 | new    | 2024-09-01 |
