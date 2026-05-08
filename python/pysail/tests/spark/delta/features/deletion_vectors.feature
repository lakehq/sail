Feature: Delta Lake Deletion Vectors (Merge-on-Read)

  Rule: MoR DELETE with deletion vectors
    Background:
      Given variable location for temporary directory dv_delete_basic
      Given final statement
        """
        DROP TABLE IF EXISTS delta_dv_delete
        """
      Given statement template
        """
        CREATE TABLE delta_dv_delete
        USING DELTA LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'true'
        )
        AS SELECT * FROM VALUES
          (1, 'Alice',   100),
          (2, 'Bob',     200),
          (3, 'Charlie', 300),
          (4, 'Diana',   400),
          (5, 'Eve',     500)
        AS t(id, name, value)
        """

    @sail-only
    Scenario: Delete with DV produces correct results
      When query
        """
        SELECT COUNT(*) AS cnt FROM delta_dv_delete
        """
      Then query result
        | cnt |
        | 5   |
      Given statement
        """
        DELETE FROM delta_dv_delete WHERE id = 3
        """
      When query
        """
        SELECT * FROM delta_dv_delete ORDER BY id
        """
      Then query result ordered
        | id | name  | value |
        | 1  | Alice | 100   |
        | 2  | Bob   | 200   |
        | 4  | Diana | 400   |
        | 5  | Eve   | 500   |

    @sail-only
    Scenario: Multiple DV deletes accumulate correctly
      Given statement
        """
        DELETE FROM delta_dv_delete WHERE id = 1
        """
      Given statement
        """
        DELETE FROM delta_dv_delete WHERE id = 5
        """
      When query
        """
        SELECT * FROM delta_dv_delete ORDER BY id
        """
      Then query result ordered
        | id | name    | value |
        | 2  | Bob     | 200   |
        | 3  | Charlie | 300   |
        | 4  | Diana   | 400   |

    @sail-only
    Scenario: Delete all rows via DV
      Given statement
        """
        DELETE FROM delta_dv_delete WHERE value > 0
        """
      When query
        """
        SELECT * FROM delta_dv_delete ORDER BY id
        """
      Then query result ordered
        | id | name | value |

    @sail-only
    Scenario: Delete with complex condition
      Given statement
        """
        DELETE FROM delta_dv_delete
        WHERE (id >= 2 AND id <= 4) OR name = 'Eve'
        """
      When query
        """
        SELECT * FROM delta_dv_delete ORDER BY id
        """
      Then query result ordered
        | id | name  | value |
        | 1  | Alice | 100   |

  Rule: Read tables with existing deletion vectors
    Background:
      Given variable location for temporary directory dv_read_basic
      Given final statement
        """
        DROP TABLE IF EXISTS delta_dv_read
        """
      Given statement template
        """
        CREATE TABLE delta_dv_read
        USING DELTA LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'true'
        )
        AS SELECT * FROM VALUES
          (1, 'Alpha'),
          (2, 'Beta'),
          (3, 'Gamma'),
          (4, 'Delta')
        AS t(id, name)
        """

    @sail-only
    Scenario: Read after DV delete filters deleted rows
      Given statement
        """
        DELETE FROM delta_dv_read WHERE id = 2
        """
      When query
        """
        SELECT * FROM delta_dv_read ORDER BY id
        """
      Then query result ordered
        | id | name  |
        | 1  | Alpha |
        | 3  | Gamma |
        | 4  | Delta |

    @sail-only
    Scenario: Aggregation after DV delete
      Given statement
        """
        DELETE FROM delta_dv_read WHERE name = 'Gamma'
        """
      When query
        """
        SELECT COUNT(*) AS cnt, SUM(id) AS total FROM delta_dv_read
        """
      Then query result
        | cnt | total |
        | 3   | 7     |

  Rule: EXPLAIN plans for DV-enabled tables
    Background:
      Given variable location for temporary directory dv_explain
      Given final statement
        """
        DROP TABLE IF EXISTS delta_dv_explain
        """
      Given statement template
        """
        CREATE TABLE delta_dv_explain
        USING DELTA LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'true'
        )
        AS SELECT * FROM VALUES
          (1, 'Alice',   100),
          (2, 'Bob',     200),
          (3, 'Charlie', 300)
        AS t(id, name, value)
        """

    @sail-only
    Scenario: EXPLAIN DELETE on DV table shows DeletionVectorWriterExec
      When query
        """
        EXPLAIN
        DELETE FROM delta_dv_explain WHERE id = 2
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN SELECT on DV table after delete uses metadata-as-data path
      Given statement
        """
        DELETE FROM delta_dv_explain WHERE id = 1
        """
      When query
        """
        EXPLAIN
        SELECT * FROM delta_dv_explain ORDER BY id
        """
      Then query plan matches snapshot

  Rule: File tree after DV operations
    Background:
      Given variable location for temporary directory dv_file_tree
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_dv_filetree
        """
      Given statement template
        """
        CREATE TABLE delta_dv_filetree
        USING DELTA LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'true'
        )
        AS SELECT * FROM VALUES
          (1, 'Alice',   100),
          (2, 'Bob',     200),
          (3, 'Charlie', 300)
        AS t(id, name, value)
        """

    @sail-only
    Scenario: DV delete creates DV bin file and preserves original parquet
      Given statement
        """
        DELETE FROM delta_dv_filetree WHERE id = 2
        """
      Then data files in location count is 1
      Then file tree in location matches
        """
        📂 <hex-prefix>
          📄 deletion_vector_<uuid>.bin
        📄 part-<id>.<codec>.parquet
        """

    @sail-only
    Scenario: Multiple DV deletes create separate DV bin files
      Given statement
        """
        DELETE FROM delta_dv_filetree WHERE id = 1
        """
      Given statement
        """
        DELETE FROM delta_dv_filetree WHERE id = 3
        """
      Then data files in location count is 1
      When query
        """
        SELECT * FROM delta_dv_filetree ORDER BY id
        """
      Then query result ordered
        | id | name | value |
        | 2  | Bob  | 200   |

    @sail-only
    Scenario: Delta log after DV delete contains correct commit actions
      Given statement
        """
        DELETE FROM delta_dv_filetree WHERE id = 2
        """
      Then delta log latest commit info matches snapshot
      Then delta log latest commit info contains
        | path                          | value    |
        | operation                     | "DELETE" |
        | operationParameters.predicate | "id = 2 " |

  Rule: Idempotent DV delete does not create new DV files
    Background:
      Given variable location for temporary directory dv_idempotent
      Given final statement
        """
        DROP TABLE IF EXISTS delta_dv_idempotent
        """
      Given statement template
        """
        CREATE TABLE delta_dv_idempotent
        USING DELTA LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'true'
        )
        AS SELECT * FROM VALUES
          (1, 'a'),
          (2, 'b'),
          (3, 'c'),
          (4, 'd'),
          (5, 'e')
        AS t(id, val)
        """

    @sail-only
    Scenario: Deleting an already-deleted row does not create a new DV file
      Given statement
        """
        DELETE FROM delta_dv_idempotent WHERE id = 2
        """
      Then file tree in location matches
        """
        📂 <hex-prefix>
          📄 deletion_vector_<uuid>.bin
        📄 part-<id>.<codec>.parquet
        """
      Given statement
        """
        DELETE FROM delta_dv_idempotent WHERE id = 2
        """
      Then file tree in location matches
        """
        📂 <hex-prefix>
          📄 deletion_vector_<uuid>.bin
        📄 part-<id>.<codec>.parquet
        """
      When query
        """
        SELECT * FROM delta_dv_idempotent ORDER BY id
        """
      Then query result ordered
        | id | val |
        | 1  | a   |
        | 3  | c   |
        | 4  | d   |
        | 5  | e   |

    @sail-only
    Scenario: Delete accumulating several rows then re-deleting already-deleted rows
      Given statement
        """
        DELETE FROM delta_dv_idempotent WHERE id IN (1, 3)
        """
      Then data files in location count is 1
      Given statement
        """
        DELETE FROM delta_dv_idempotent WHERE id IN (1, 2, 3)
        """
      Then data files in location count is 1
      Given statement
        """
        DELETE FROM delta_dv_idempotent WHERE id IN (1, 3)
        """
      Then data files in location count is 1
      When query
        """
        SELECT * FROM delta_dv_idempotent ORDER BY id
        """
      Then query result ordered
        | id | val |
        | 4  | d   |
        | 5  | e   |
