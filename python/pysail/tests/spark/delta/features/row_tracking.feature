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
  Rule: ALTER TABLE enabling rowTracking causes subsequent inserts to stamp row ids

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
  Rule: Row tracking enabled rewrites fail safely until stable IDs are preserved

    Scenario: Copy-on-Write DELETE refuses to rewrite row-tracking enabled rows
      Given variable location for temporary directory delta_rt_cow_delete_guard
      Given final statement
        """
        DROP TABLE IF EXISTS delta_rt_cow_delete_guard_test
        """
      Given statement template
        """
        CREATE TABLE delta_rt_cow_delete_guard_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableRowTracking' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_rt_cow_delete_guard_test VALUES (1), (2), (3)
        """
      Given statement with error Copy-on-Write DELETE
        """
        DELETE FROM delta_rt_cow_delete_guard_test WHERE id = 2
        """
