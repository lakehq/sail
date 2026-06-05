Feature: Delta Lake operationMetrics in commitInfo

  Rule: WRITE operationMetrics

    Background:
      Given variable location for temporary directory op_metrics_write
      Given final statement
        """
        DROP TABLE IF EXISTS delta_op_metrics_write
        """
      Given statement template
        """
        CREATE TABLE delta_op_metrics_write (id BIGINT, value STRING)
        USING DELTA LOCATION {{ location.sql }}
        """

    @sail-only
    Scenario: Append WRITE reports numFiles, numOutputRows and numOutputBytes
      Given statement
        """
        INSERT INTO delta_op_metrics_write VALUES (0,'seed')
        """
      Given statement
        """
        INSERT INTO delta_op_metrics_write VALUES (1,'a'),(2,'b'),(3,'c')
        """
      Then delta log latest commit info matches snapshot for paths
        | path             |
        | operation        |
        | operationMetrics |

    @sail-only
    Scenario: replaceWhere overwrite reports added and removed counters
      Given statement
        """
        INSERT INTO delta_op_metrics_write VALUES (1,'a'),(2,'b'),(3,'c')
        """
      Given statement
        """
        INSERT INTO delta_op_metrics_write REPLACE WHERE id >= 2
        VALUES (5,'x'),(6,'y')
        """
      Then delta log latest commit info matches snapshot for paths
        | path             |
        | operation        |
        | operationMetrics |

  Rule: DELETE operationMetrics (Copy-on-Write)

    Background:
      Given variable location for temporary directory op_metrics_delete_cow
      Given final statement
        """
        DROP TABLE IF EXISTS delta_op_metrics_delete_cow
        """
      Given statement template
        """
        CREATE TABLE delta_op_metrics_delete_cow (id INT, category STRING)
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_op_metrics_delete_cow VALUES
          (1,'a'),(2,'a'),(3,'b'),(4,'b'),(5,'c')
        """

    @sail-only
    Scenario: Data-predicate DELETE reports numDeletedRows and numCopiedRows
      Given statement
        """
        DELETE FROM delta_op_metrics_delete_cow WHERE category = 'a'
        """
      Then delta log latest commit info matches snapshot for paths
        | path             |
        | operation        |
        | operationMetrics |

  Rule: DELETE operationMetrics (Copy-on-Write, partition predicate drops entire files)

    Background:
      Given variable location for temporary directory op_metrics_delete_partition
      Given final statement
        """
        DROP TABLE IF EXISTS delta_op_metrics_delete_partition
        """
      Given statement template
        """
        CREATE TABLE delta_op_metrics_delete_partition (id INT, year INT)
        USING DELTA LOCATION {{ location.sql }}
        PARTITIONED BY (year)
        """
      Given statement
        """
        INSERT INTO delta_op_metrics_delete_partition VALUES
          (1, 2023),(2, 2023),(3, 2024),(4, 2024)
        """

    @sail-only
    Scenario: Partition-only DELETE removes whole files without scanning
      Given statement
        """
        DELETE FROM delta_op_metrics_delete_partition WHERE year = 2023
        """
      Then delta log latest commit info matches snapshot for paths
        | path             |
        | operation        |
        | operationMetrics |

  Rule: DELETE operationMetrics (Merge-on-Read with deletion vectors)

    Background:
      Given variable location for temporary directory op_metrics_delete_dv
      Given final statement
        """
        DROP TABLE IF EXISTS delta_op_metrics_delete_dv
        """
      Given statement template
        """
        CREATE TABLE delta_op_metrics_delete_dv
        USING DELTA LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
        AS SELECT * FROM VALUES
          (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')
        AS t(id, value)
        """

    @sail-only
    Scenario: DV DELETE updates an existing deletion vector
      Given statement
        """
        DELETE FROM delta_op_metrics_delete_dv WHERE id = 3
        """
      Given statement
        """
        DELETE FROM delta_op_metrics_delete_dv WHERE id = 4
        """
      Then delta log latest commit info matches snapshot for paths
        | path             |
        | operation        |
        | operationMetrics |

  Rule: MERGE operationMetrics

    Background:
      Given variable location for temporary directory op_metrics_merge
      Given final statement
        """
        DROP TABLE IF EXISTS delta_op_metrics_merge
        """
      Given statement template
        """
        CREATE TABLE delta_op_metrics_merge (id INT, value STRING, flag STRING)
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_op_metrics_merge VALUES
          (1,'old','keep'),
          (2,'old','matched_update'),
          (3,'old','matched_delete'),
          (5,'old','source_update'),
          (6,'old','source_delete')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_op_metrics_merge AS
        SELECT * FROM VALUES
          (2,'new','matched_update'),
          (3,'any','matched_delete'),
          (4,'ins','insert')
        AS src(id, value, flag)
        """

    @sail-only
    Scenario: Full MERGE reports detailed target row counters
      Given statement
        """
        MERGE INTO delta_op_metrics_merge AS t
        USING src_op_metrics_merge AS s
        ON t.id = s.id
        WHEN MATCHED AND t.flag = 'matched_update' THEN UPDATE SET value = s.value
        WHEN MATCHED AND t.flag = 'matched_delete' THEN DELETE
        WHEN NOT MATCHED BY SOURCE AND t.flag = 'source_update' THEN
          UPDATE SET value = concat(t.value, '_stale')
        WHEN NOT MATCHED BY SOURCE AND t.flag = 'source_delete' THEN DELETE
        WHEN NOT MATCHED THEN INSERT *
        """
      Then delta log latest commit info matches snapshot for paths
        | path             |
        | operation        |
        | operationMetrics |

    @sail-only
    Scenario: Conditional MERGE counts source rows without actions
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_op_metrics_merge_conditional AS
        SELECT * FROM VALUES
          (1,'same','keep'),
          (2,'new','matched_update'),
          (4,'ins','insert'),
          (7,'skip','skip')
        AS src(id, value, flag)
        """
      Given statement
        """
        MERGE INTO delta_op_metrics_merge AS t
        USING src_op_metrics_merge_conditional AS s
        ON t.id = s.id
        WHEN MATCHED AND t.flag = 'matched_update' THEN UPDATE SET value = s.value
        WHEN NOT MATCHED AND s.flag = 'insert' THEN INSERT *
        """
      Then delta log latest commit info matches snapshot for paths
        | path             |
        | operation        |
        | operationMetrics |

    @sail-only
    Scenario: Insert-only MERGE fast-appends without removing files
      Given statement
        """
        MERGE INTO delta_op_metrics_merge AS t
        USING src_op_metrics_merge AS s
        ON t.id = s.id
        WHEN NOT MATCHED THEN INSERT *
        """
      Then delta log latest commit info matches snapshot for paths
        | path             |
        | operation        |
        | operationMetrics |

    @sail-only
    Scenario: Insert-only MERGE counts duplicate target matches once per source row
      Given statement
        """
        INSERT INTO delta_op_metrics_merge VALUES (2,'duplicate','matched_update')
        """
      Given statement
        """
        MERGE INTO delta_op_metrics_merge AS t
        USING src_op_metrics_merge AS s
        ON t.id = s.id
        WHEN NOT MATCHED THEN INSERT *
        """
      Then delta log latest commit info matches snapshot for paths
        | path             |
        | operation        |
        | operationMetrics |

    @sail-only
    Scenario: Target-only MERGE actions report zero source rows
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_op_metrics_empty AS
        SELECT * FROM VALUES
          (CAST(NULL AS INT), CAST(NULL AS STRING), CAST(NULL AS STRING))
        AS src(id, value, flag)
        WHERE id IS NOT NULL
        """
      Given statement
        """
        MERGE INTO delta_op_metrics_merge AS t
        USING src_op_metrics_empty AS s
        ON t.id = s.id
        WHEN NOT MATCHED BY SOURCE AND t.flag = 'source_update' THEN
          UPDATE SET value = concat(t.value, '_stale')
        WHEN NOT MATCHED BY SOURCE AND t.flag = 'source_delete' THEN DELETE
        """
      Then delta log latest commit info matches snapshot for paths
        | path             |
        | operation        |
        | operationMetrics |

  Rule: MERGE operationMetrics (Merge-on-Read with deletion vectors)

    Background:
      Given variable location for temporary directory op_metrics_merge_dv
      Given final statement
        """
        DROP TABLE IF EXISTS delta_op_metrics_merge_dv
        """
      Given statement template
        """
        CREATE TABLE delta_op_metrics_merge_dv
        USING DELTA LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
        AS SELECT * FROM VALUES
          (1,'a'),(2,'b'),(3,'c'),(4,'d')
        AS t(id, value)
        """
      Given statement
        """
        DELETE FROM delta_op_metrics_merge_dv WHERE id = 1
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_op_metrics_merge_dv AS
        SELECT * FROM VALUES (3,'delete') AS src(id, value)
        """

    @sail-only
    Scenario: MERGE DELETE on DV table reports target deletion vector update counters
      Given statement
        """
        MERGE INTO delta_op_metrics_merge_dv AS t
        USING src_op_metrics_merge_dv AS s
        ON t.id = s.id
        WHEN MATCHED THEN DELETE
        """
      Then delta log latest commit info matches snapshot for paths
        | path             |
        | operation        |
        | operationMetrics |
