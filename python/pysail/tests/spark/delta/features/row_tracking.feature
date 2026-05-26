Feature: Delta Lake Row Tracking writer (baseRowId, defaultRowCommitVersion, rowIdHighWaterMark)

  @sail-only
  Rule: Supported-only writer stamps every add and publishes the high-water-mark each commit

    Background:
      Given variable location for temporary directory delta_rt_supported
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_rt_supported_test
        """
      Given statement template
        """
        CREATE TABLE delta_rt_supported_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.minReaderVersion' = '3',
          'delta.minWriterVersion' = '7',
          'delta.feature.rowTracking' = 'supported',
          'delta.feature.domainMetadata' = 'supported'
        )
        """
      Given statement
        """
        INSERT INTO delta_rt_supported_test VALUES (1), (2), (3)
        """
      Given statement
        """
        INSERT INTO delta_rt_supported_test VALUES (4), (5)
        """

    Scenario: First insert assigns row ids starting at 0 and emits domainMetadata
      Then delta log commit 00000000000000000000.json in location contains action
        | path                         | value |
        | add.baseRowId                | 0     |
        | add.defaultRowCommitVersion  | 0     |
        | domainMetadata.domain        | "delta.rowTracking" |
      Then delta log commit 00000000000000000000.json in location has rowTracking high-water-mark 2

    Scenario: Subsequent insert continues row ids and updates the high-water-mark
      Then delta log commit 00000000000000000001.json in location contains action
        | path                         | value |
        | add.baseRowId                | 3     |
        | add.defaultRowCommitVersion  | 1     |
        | domainMetadata.domain        | "delta.rowTracking" |
      Then delta log commit 00000000000000000001.json in location has rowTracking high-water-mark 4

  @sail-only
  Rule: Suspended row tracking does not stamp row ids on new commits

    Background:
      Given variable location for temporary directory delta_rt_suspended
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_rt_suspended_test
        """
      Given statement template
        """
        CREATE TABLE delta_rt_suspended_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.minReaderVersion' = '3',
          'delta.minWriterVersion' = '7',
          'delta.feature.rowTracking' = 'supported',
          'delta.feature.domainMetadata' = 'supported',
          'delta.rowTrackingSuspended' = 'true'
        )
        """
      Given statement
        """
        INSERT INTO delta_rt_suspended_test VALUES (1), (2)
        """

    Scenario: Suspended commit has no baseRowId on add and no rowTracking domainMetadata
      Then delta log commit 00000000000000000000.json in location has no action with sub-field add.baseRowId set
      Then delta log commit 00000000000000000000.json in location has no action with sub-field add.defaultRowCommitVersion set

  @sail-only
  Rule: ALTER TABLE enabling rowTracking backfills existing files and stamps subsequent inserts

    Background:
      Given variable location for temporary directory delta_rt_alter
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_rt_alter_test
        """
      Given statement template
        """
        CREATE TABLE delta_rt_alter_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_rt_alter_test VALUES (1)
        """
      Given statement
        """
        ALTER TABLE delta_rt_alter_test SET TBLPROPERTIES (
          'delta.minReaderVersion' = '3',
          'delta.minWriterVersion' = '7',
          'delta.feature.rowTracking' = 'supported',
          'delta.feature.domainMetadata' = 'supported'
        )
        """
      Given statement
        """
        INSERT INTO delta_rt_alter_test VALUES (2), (3)
        """

    Scenario: Pre-ALTER commit has no row id fields; post-ALTER commit carries them
      Then delta log commit 00000000000000000000.json in location has no action with sub-field add.baseRowId set
      Then delta log commit 00000000000000000002.json in location contains action
        | path                         | value |
        | add.baseRowId                | 0     |
        | add.defaultRowCommitVersion  | 2     |
        | domainMetadata.domain        | "delta.rowTracking" |

    Scenario: Enabling rowTracking backfills existing active files
      Given variable location for temporary directory delta_rt_backfill
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_rt_backfill_test
        """
      Given statement template
        """
        CREATE TABLE delta_rt_backfill_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_rt_backfill_test VALUES (1), (2)
        """
      Given statement
        """
        ALTER TABLE delta_rt_backfill_test SET TBLPROPERTIES (
          'delta.enableRowTracking' = 'true'
        )
        """
      Given statement
        """
        INSERT INTO delta_rt_backfill_test VALUES (3)
        """
      Then delta log commit 00000000000000000001.json in location contains action
        | path                         | value |
        | add.baseRowId                | 0     |
        | add.defaultRowCommitVersion  | 1     |
        | add.dataChange               | false |
        | domainMetadata.domain        | "delta.rowTracking" |
      When query
        """
        SELECT id, _metadata.row_id AS rid, _metadata.row_commit_version AS ver
        FROM delta_rt_backfill_test
        ORDER BY id
        """
      Then query result ordered
        | id | rid | ver |
        | 1  | 0   | 1   |
        | 2  | 1   | 1   |
        | 3  | 2   | 2   |

  @sail-only
  Rule: delta.enableRowTracking=true alone auto-activates features and materialized column names

    Background:
      Given variable location for temporary directory delta_rt_enable_only
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_rt_enable_only_test
        """
      Given statement template
        """
        CREATE TABLE delta_rt_enable_only_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableRowTracking' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_rt_enable_only_test VALUES (1), (2), (3)
        """

    Scenario: Protocol and metadata are auto-upgraded; add stamps baseRowId
      Then delta log commit 00000000000000000000.json in location contains action
        | path                                                                                 | value               |
        | metaData.configuration["delta.enableRowTracking"]                                    | "true"              |
        | add.baseRowId                                                                        | 0                   |
        | add.defaultRowCommitVersion                                                          | 0                   |
        | domainMetadata.domain                                                                | "delta.rowTracking" |
      Then delta log commit 00000000000000000000.json in location has rowTracking high-water-mark 2

  @sail-only
  Rule: delta.feature.rowTracking alone auto-activates required dependencies

    Background:
      Given variable location for temporary directory delta_rt_feature_only
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_rt_feature_only_test
        """
      Given statement template
        """
        CREATE TABLE delta_rt_feature_only_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.feature.rowTracking' = 'supported')
        """
      Given statement
        """
        INSERT INTO delta_rt_feature_only_test VALUES (1), (2)
        """

    Scenario: RowTracking feature property alone produces a writable row-tracking table
      Then delta log commit 00000000000000000000.json in location contains action
        | path                         | value                            |
        | protocol.writerFeatures      | ["domainMetadata", "rowTracking"] |
        | add.baseRowId                | 0                                |
        | add.defaultRowCommitVersion  | 0                                |
        | domainMetadata.domain        | "delta.rowTracking"              |
      Then delta log commit 00000000000000000000.json in location has rowTracking high-water-mark 1

  @sail-only
  Rule: SQL can read _metadata.row_id and _metadata.row_commit_version

    Background:
      Given variable location for temporary directory delta_rt_sql_reads
      Given final statement
        """
        DROP TABLE IF EXISTS delta_rt_sql_reads_test
        """
      Given statement template
        """
        CREATE TABLE delta_rt_sql_reads_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableRowTracking' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_rt_sql_reads_test VALUES (10), (20), (30)
        """
      Given statement
        """
        INSERT INTO delta_rt_sql_reads_test VALUES (40), (50)
        """

    Scenario: Row ids are contiguous per file and restart per commit; commit versions match
      When query
        """
        SELECT id, _metadata.row_id AS rid, _metadata.row_commit_version AS ver
        FROM delta_rt_sql_reads_test
        ORDER BY id
        """
      Then query result ordered
        | id | rid | ver |
        | 10 | 0   | 0   |
        | 20 | 1   | 0   |
        | 30 | 2   | 0   |
        | 40 | 3   | 1   |
        | 50 | 4   | 1   |

    Scenario: Only row_id can be projected (struct pruning)
      When query
        """
        SELECT _metadata.row_id AS rid FROM delta_rt_sql_reads_test ORDER BY rid
        """
      Then query result ordered
        | rid |
        | 0   |
        | 1   |
        | 2   |
        | 3   |
        | 4   |

    Scenario: SELECT star excludes row tracking metadata
      When query
        """
        SELECT * FROM delta_rt_sql_reads_test ORDER BY id
        """
      Then query result ordered
        | id |
        | 10 |
        | 20 |
        | 30 |
        | 40 |
        | 50 |

  @sail-only
  Rule: User data columns named _metadata do not hide explicit row-tracking metadata

    Background:
      Given variable location for temporary directory delta_rt_user_metadata
      Given final statement
        """
        DROP TABLE IF EXISTS delta_rt_user_metadata_test
        """
      Given statement template
        """
        CREATE TABLE delta_rt_user_metadata_test (`_metadata` BIGINT, id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableRowTracking' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_rt_user_metadata_test VALUES (100, 1), (200, 2)
        """

    Scenario: Plain _metadata resolves to the user column
      When query
        """
        SELECT `_metadata`, id FROM delta_rt_user_metadata_test ORDER BY id
        """
      Then query result ordered
        | _metadata | id |
        | 100       | 1  |
        | 200       | 2  |

    Scenario: Nested row-tracking metadata can still be read
      When query
        """
        SELECT id, _metadata.row_id AS rid
        FROM delta_rt_user_metadata_test
        ORDER BY id
        """
      Then query result ordered
        | id | rid |
        | 1  | 0   |
        | 2  | 1   |

  @sail-only
  Rule: Suspended row tracking does not expose row-tracking metadata

    Background:
      Given variable location for temporary directory delta_rt_sql_suspended
      Given final statement
        """
        DROP TABLE IF EXISTS delta_rt_sql_suspended_test
        """
      Given statement template
        """
        CREATE TABLE delta_rt_sql_suspended_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.minReaderVersion' = '3',
          'delta.minWriterVersion' = '7',
          'delta.feature.rowTracking' = 'supported',
          'delta.feature.domainMetadata' = 'supported',
          'delta.rowTrackingSuspended' = 'true'
        )
        """
      Given statement
        """
        INSERT INTO delta_rt_sql_suspended_test VALUES (1), (2)
        """

    Scenario: Suspended tables do not expose row_id and row_commit_version
      When query
        """
        SELECT id, _metadata.row_id AS rid, _metadata.row_commit_version AS ver
        FROM delta_rt_sql_suspended_test
        ORDER BY id
        """
      Then query error _metadata

  @sail-only
  Rule: Row tracking row ids use physical file row indices

    Background:
      Given variable location for temporary directory delta_rt_dv_row_index
      Given final statement
        """
        DROP TABLE IF EXISTS delta_rt_dv_row_index_test
        """
      Given statement template
        """
        CREATE TABLE delta_rt_dv_row_index_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.enableRowTracking' = 'true',
          'delta.enableDeletionVectors' = 'true'
        )
        """
      Given statement
        """
        INSERT INTO delta_rt_dv_row_index_test VALUES (1), (2), (3), (4), (5)
        """
      Given statement
        """
        DELETE FROM delta_rt_dv_row_index_test WHERE id = 2
        """

    Scenario: Deletion vectors do not renumber row ids
      When query
        """
        SELECT id, _metadata.row_id AS rid
        FROM delta_rt_dv_row_index_test
        ORDER BY id
        """
      Then query result ordered
        | id | rid |
        | 1  | 0   |
        | 3  | 2   |
        | 4  | 3   |
        | 5  | 4   |

  @sail-only
  Rule: Row tracking protocol validation rejects conflicting materialized columns

    Scenario: enableRowTracking cannot be combined with rowTrackingSuspended
      Given variable location for temporary directory delta_rt_enable_suspended_conflict
      Given final statement
        """
        DROP TABLE IF EXISTS delta_rt_enable_suspended_conflict_test
        """
      Given statement template
        """
        CREATE TABLE delta_rt_enable_suspended_conflict_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.enableRowTracking' = 'true',
          'delta.rowTrackingSuspended' = 'true'
        )
        """
      Given statement with error delta.enableRowTracking
        """
        INSERT INTO delta_rt_enable_suspended_conflict_test VALUES (1)
        """

    Scenario: Materialized row id column cannot conflict with a data column
      Given variable location for temporary directory delta_rt_materialized_conflict
      Given final statement
        """
        DROP TABLE IF EXISTS delta_rt_materialized_conflict_test
        """
      Given statement template
        """
        CREATE TABLE delta_rt_materialized_conflict_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.enableRowTracking' = 'true',
          'delta.rowTracking.materializedRowIdColumnName' = 'id',
          'delta.rowTracking.materializedRowCommitVersionColumnName' = '_row-commit-version-col-test'
        )
        """
      When query
        """
        INSERT INTO delta_rt_materialized_conflict_test VALUES (1)
        """
      Then query error materialized column name

  @sail-only
  Rule: Row tracking enabled rewrites preserve stable row IDs

    Scenario: Copy-on-Write DELETE preserves retained rows' row tracking metadata
      Given variable location for temporary directory delta_rt_cow_delete_preserve
      Given final statement
        """
        DROP TABLE IF EXISTS delta_rt_cow_delete_preserve_test
        """
      Given statement template
        """
        CREATE TABLE delta_rt_cow_delete_preserve_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableRowTracking' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_rt_cow_delete_preserve_test VALUES (1), (2), (3)
        """
      Given statement
        """
        DELETE FROM delta_rt_cow_delete_preserve_test WHERE id = 2
        """
      When query
        """
        SELECT id, _metadata.row_id AS rid, _metadata.row_commit_version AS ver
        FROM delta_rt_cow_delete_preserve_test
        ORDER BY id
        """
      Then query result ordered
        | id | rid | ver |
        | 1  | 0   | 0   |
        | 3  | 2   | 0   |

    Scenario: EXPLAIN plan for row tracking materialization during DELETE
      Given variable location for temporary directory delta_rt_delete_materialize_explain
      Given final statement
        """
        DROP TABLE IF EXISTS delta_rt_delete_materialize_explain_test
        """
      Given statement template
        """
        CREATE TABLE delta_rt_delete_materialize_explain_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableRowTracking' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_rt_delete_materialize_explain_test VALUES (1), (2), (3)
        """
      When query
        """
        EXPLAIN
        DELETE FROM delta_rt_delete_materialize_explain_test
        WHERE id = 2
        """
      Then query plan matches snapshot

    Scenario: MERGE preserves copied target rows and materializes changed rows
      Given variable location for temporary directory delta_rt_merge_preserve
      Given final statement
        """
        DROP TABLE IF EXISTS delta_rt_merge_preserve_test
        """
      Given statement template
        """
        CREATE TABLE delta_rt_merge_preserve_test (id INT, value STRING)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableRowTracking' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_rt_merge_preserve_test VALUES
          (1, 'keep'),
          (2, 'update'),
          (3, 'delete')
        """
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW delta_rt_merge_preserve_src AS
        SELECT * FROM VALUES
          (2, 'updated'),
          (3, 'ignored'),
          (4, 'inserted')
        AS src(id, value)
        """
      Given statement
        """
        MERGE INTO delta_rt_merge_preserve_test AS t
        USING delta_rt_merge_preserve_src AS s
        ON t.id = s.id
        WHEN MATCHED AND t.id = 2 THEN UPDATE SET value = s.value
        WHEN MATCHED AND t.id = 3 THEN DELETE
        WHEN NOT MATCHED THEN INSERT *
        """
      When query
        """
        SELECT id, value, _metadata.row_id AS rid, _metadata.row_commit_version AS ver
        FROM delta_rt_merge_preserve_test
        ORDER BY id
        """
      Then query result ordered
        | id | value    | rid | ver |
        | 1  | keep     | 0   | 0   |
        | 2  | updated  | 1   | 1   |
        | 4  | inserted | 3   | 1   |

    Scenario: REPLACE WHERE preserves row IDs for retained old rows
      Given variable location for temporary directory delta_rt_replace_preserve
      Given final statement
        """
        DROP TABLE IF EXISTS delta_rt_replace_preserve_test
        """
      Given statement template
        """
        CREATE TABLE delta_rt_replace_preserve_test (id INT, category STRING)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableRowTracking' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_rt_replace_preserve_test VALUES
          (1, 'A'),
          (2, 'B'),
          (3, 'A'),
          (4, 'B')
        """
      Given statement
        """
        INSERT INTO delta_rt_replace_preserve_test
        REPLACE WHERE category = 'A'
        SELECT * FROM VALUES
          (5, 'A'),
          (6, 'A')
        AS tab(id, category)
        """
      When query
        """
        SELECT id, category, _metadata.row_id AS rid, _metadata.row_commit_version AS ver
        FROM delta_rt_replace_preserve_test
        ORDER BY id
        """
      Then query result ordered
        | id | category | rid | ver |
        | 2  | B        | 1   | 0   |
        | 4  | B        | 3   | 0   |
        | 5  | A        | 4   | 1   |
        | 6  | A        | 5   | 1   |
