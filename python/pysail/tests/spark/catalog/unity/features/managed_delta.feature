Feature: Unity Catalog managed Delta table operations

  Background:
    Given statement
      """
      CREATE SCHEMA IF NOT EXISTS unity_table_test
      """
    Given final statement
      """
      DROP SCHEMA IF EXISTS unity_table_test CASCADE
      """

  Scenario: Create managed Delta table without LOCATION uses Unity staging
    Given statement
      """
      CREATE TABLE unity_table_test.managed_sql_create_t (
        id INT,
        name STRING
      )
      USING delta
      """
    When query
      """
      SHOW TABLES IN unity_table_test LIKE 'managed_sql_create_t'
      """
    Then query result
      | database                           | tableName            | isTemporary |
      | sail_test_catalog.unity_table_test | managed_sql_create_t | false       |
    Then Unity Catalog table unity_table_test.managed_sql_create_t is a managed Delta table
    Then Unity Catalog table unity_table_test.managed_sql_create_t storage location is under managed storage root

  Scenario: Managed Delta table created without LOCATION can be read before writes
    Given statement
      """
      CREATE TABLE unity_table_test.managed_sql_empty_read_t (
        id INT,
        name STRING
      )
      USING delta
      """
    Given variable location for table unity_table_test.managed_sql_empty_read_t
    When query
      """
      SELECT id, name FROM unity_table_test.managed_sql_empty_read_t ORDER BY id
      """
    Then query result ordered
      | id | name |
    Then Unity Catalog table unity_table_test.managed_sql_empty_read_t is a managed Delta table
    Then Unity Catalog table unity_table_test.managed_sql_empty_read_t storage location is under managed storage root
    Then Unity Catalog table unity_table_test.managed_sql_empty_read_t table id matches Delta metadata in location
    Then Delta commit for version 0 exists in location
    Then Delta commit for version 0 in location has catalog-managed commit info
    Then delta log first commit protocol and metadata contains
      | path                                                   | value                                  |
      | protocol.minReaderVersion                              | 3                                      |
      | protocol.minWriterVersion                              | 7                                      |
      | protocol.readerFeatures                                | ["catalogManaged","vacuumProtocolCheck"] |
      | protocol.writerFeatures                                | ["catalogManaged","inCommitTimestamp","vacuumProtocolCheck"] |
      | metaData.configuration["delta.feature.catalogManaged"]  | "supported"                            |
      | metaData.configuration["delta.enableInCommitTimestamps"] | "true"                                |

  Scenario: CTAS managed Delta table without LOCATION writes through Unity staging
    Given statement
      """
      CREATE TABLE unity_table_test.managed_ctas_t
      USING delta
      AS SELECT * FROM VALUES
        (1, 'one'),
        (2, 'two')
      AS t(id, name)
      """
    Given variable location for table unity_table_test.managed_ctas_t
    When query
      """
      SELECT id, name FROM unity_table_test.managed_ctas_t ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 1  | one  |
      | 2  | two  |
    Then Unity Catalog table unity_table_test.managed_ctas_t is a managed Delta table
    Then Unity Catalog table unity_table_test.managed_ctas_t storage location is under managed storage root
    Then Unity Catalog table unity_table_test.managed_ctas_t table id matches Delta metadata in location
    Then Delta commit for version 0 exists in location
    Then Delta commit for version 0 in location has catalog-managed commit info
    Then Unity Catalog Delta commit for table unity_table_test.managed_ctas_t version 1 references staged Delta commit in location

  Scenario: Managed Delta table created without LOCATION accepts first INSERT
    Given statement
      """
      CREATE TABLE unity_table_test.managed_sql_insert_t (
        id INT,
        name STRING
      )
      USING delta
      """
    Given statement
      """
      INSERT INTO unity_table_test.managed_sql_insert_t VALUES
        (1, 'one'),
        (2, 'two')
      """
    Given variable location for table unity_table_test.managed_sql_insert_t
    When query
      """
      SELECT id, name FROM unity_table_test.managed_sql_insert_t ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 1  | one  |
      | 2  | two  |
    Then Unity Catalog table unity_table_test.managed_sql_insert_t is a managed Delta table
    Then Unity Catalog table unity_table_test.managed_sql_insert_t storage location is under managed storage root
    Then Unity Catalog table unity_table_test.managed_sql_insert_t table id matches Delta metadata in location
    Then Delta commit for version 0 exists in location
    Then Delta commit for version 0 in location has catalog-managed commit info
    Then Unity Catalog Delta commit for table unity_table_test.managed_sql_insert_t version 1 references staged Delta commit in location

  Scenario: Managed Delta table read before first INSERT coordinates the write
    Given statement
      """
      CREATE TABLE unity_table_test.managed_sql_read_insert_t (
        id INT,
        name STRING
      )
      USING delta
      """
    Given variable location for table unity_table_test.managed_sql_read_insert_t
    When query
      """
      SELECT id, name FROM unity_table_test.managed_sql_read_insert_t ORDER BY id
      """
    Then query result ordered
      | id | name |
    Given statement
      """
      INSERT INTO unity_table_test.managed_sql_read_insert_t VALUES
        (1, 'one'),
        (2, 'two')
      """
    When query
      """
      SELECT id, name FROM unity_table_test.managed_sql_read_insert_t ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 1  | one  |
      | 2  | two  |
    Then Unity Catalog table unity_table_test.managed_sql_read_insert_t is a managed Delta table
    Then Unity Catalog table unity_table_test.managed_sql_read_insert_t storage location is under managed storage root
    Then Unity Catalog table unity_table_test.managed_sql_read_insert_t table id matches Delta metadata in location
    Then Delta commit for version 0 in location has catalog-managed commit info
    Then Unity Catalog Delta commit for table unity_table_test.managed_sql_read_insert_t version 1 references staged Delta commit in location

  Scenario: Managed Delta table writes are coordinated by Unity Catalog
    Given Unity Catalog managed Delta table unity_table_test.managed_delta_t exists with id and name columns
    Given final Unity Catalog managed table cleanup for unity_table_test.managed_delta_t
    Given statement
      """
      INSERT INTO unity_table_test.managed_delta_t VALUES
        (1, 'one'),
        (2, 'two')
      """
    Given variable location for table unity_table_test.managed_delta_t
    When query
      """
      SELECT id, name FROM unity_table_test.managed_delta_t ORDER BY id
      """
    Then query result
      | id | name |
      | 1  | one  |
      | 2  | two  |
    Then Unity Catalog table unity_table_test.managed_delta_t is a managed Delta table
    Then Unity Catalog table unity_table_test.managed_delta_t table id matches Delta metadata in location
    Then Delta commit for version 0 in location has catalog-managed commit info
    Then Unity Catalog Delta commit for table unity_table_test.managed_delta_t version 1 references staged Delta commit in location
    Then delta log first commit protocol and metadata contains
      | path                                                   | value                                      |
      | protocol.minReaderVersion                              | 3                                          |
      | protocol.minWriterVersion                              | 7                                          |
      | protocol.readerFeatures                                | ["catalogManaged","vacuumProtocolCheck"]   |
      | protocol.writerFeatures                                | ["catalogManaged","inCommitTimestamp","vacuumProtocolCheck"] |
      | metaData.configuration["delta.feature.catalogManaged"]  | "supported"                                |
      | metaData.configuration["delta.enableInCommitTimestamps"] | "true"                                    |

  Scenario: Managed Delta table reads latest Unity coordinated commits
    Given statement
      """
      CREATE TABLE unity_table_test.managed_delta_multi_commit_t
      USING delta
      AS SELECT * FROM VALUES
        (1, 'one')
      AS t(id, name)
      """
    Given statement
      """
      INSERT INTO unity_table_test.managed_delta_multi_commit_t VALUES
        (2, 'two')
      """
    Given statement
      """
      INSERT INTO unity_table_test.managed_delta_multi_commit_t VALUES
        (3, 'three')
      """
    Given variable location for table unity_table_test.managed_delta_multi_commit_t
    When query
      """
      SELECT id, name FROM unity_table_test.managed_delta_multi_commit_t ORDER BY id
      """
    Then query result ordered
      | id | name  |
      | 1  | one   |
      | 2  | two   |
      | 3  | three |
    Then Unity Catalog table unity_table_test.managed_delta_multi_commit_t is a managed Delta table
    Then Unity Catalog table unity_table_test.managed_delta_multi_commit_t storage location is under managed storage root
    Then Unity Catalog table unity_table_test.managed_delta_multi_commit_t table id matches Delta metadata in location
    Then staged Delta commit for version 1 exists in location
    Then staged Delta commit for version 2 exists in location
    Then Unity Catalog Delta commit for table unity_table_test.managed_delta_multi_commit_t version 3 references staged Delta commit in location

  Scenario: INSERT OVERWRITE on managed Delta table is coordinated by Unity Catalog
    Given statement
      """
      CREATE TABLE unity_table_test.managed_delta_overwrite_t
      USING delta
      AS SELECT * FROM VALUES
        (1, 'old'),
        (2, 'old')
      AS t(id, name)
      """
    Given statement
      """
      INSERT OVERWRITE TABLE unity_table_test.managed_delta_overwrite_t VALUES
        (3, 'new'),
        (4, 'new')
      """
    Given variable location for table unity_table_test.managed_delta_overwrite_t
    When query
      """
      SELECT id, name FROM unity_table_test.managed_delta_overwrite_t ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 3  | new  |
      | 4  | new  |
    Then Unity Catalog table unity_table_test.managed_delta_overwrite_t is a managed Delta table
    Then Unity Catalog Delta commit for table unity_table_test.managed_delta_overwrite_t version 2 references staged Delta commit in location

  Scenario: Managed Delta writes use Unity latest version when prior commit is unpublished
    Given statement
      """
      CREATE TABLE unity_table_test.managed_delta_unpublished_latest_t
      USING delta
      AS SELECT * FROM VALUES
        (1, 'one')
      AS t(id, name)
      """
    Given variable location for table unity_table_test.managed_delta_unpublished_latest_t
    Given published Delta commit for version 1 in location is removed
    Given statement
      """
      INSERT INTO unity_table_test.managed_delta_unpublished_latest_t VALUES
        (2, 'two')
      """
    Then staged Delta commit for version 1 exists in location without published backfill
    Then Unity Catalog Delta commit for table unity_table_test.managed_delta_unpublished_latest_t version 2 references staged Delta commit in location

  Scenario: DELETE on managed Delta table uses Unity latest version when prior commit is unpublished
    Given statement
      """
      CREATE TABLE unity_table_test.managed_delta_delete_unpublished_latest_t
      USING delta
      AS SELECT * FROM VALUES
        (1, 'one'),
        (2, 'two'),
        (3, 'three')
      AS t(id, name)
      """
    Given variable location for table unity_table_test.managed_delta_delete_unpublished_latest_t
    Given published Delta commit for version 1 in location is removed
    Given statement
      """
      DELETE FROM unity_table_test.managed_delta_delete_unpublished_latest_t WHERE id = 2
      """
    When query
      """
      SELECT id, name FROM unity_table_test.managed_delta_delete_unpublished_latest_t ORDER BY id
      """
    Then query result ordered
      | id | name  |
      | 1  | one   |
      | 3  | three |
    Then staged Delta commit for version 1 exists in location without published backfill
    Then Unity Catalog Delta commit for table unity_table_test.managed_delta_delete_unpublished_latest_t version 2 references staged Delta commit in location

  Scenario: Managed Delta named-table reads use Unity ratified commit when published commit is corrupted
    Given statement
      """
      CREATE TABLE unity_table_test.managed_delta_corrupt_published_t
      USING delta
      AS SELECT * FROM VALUES
        (1, 'one')
      AS t(id, name)
      """
    Given variable location for table unity_table_test.managed_delta_corrupt_published_t
    Given published Delta commit for version 1 in location is invalid JSON
    When query
      """
      SELECT id, name FROM unity_table_test.managed_delta_corrupt_published_t ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 1  | one  |
    Then Unity Catalog Delta commit for table unity_table_test.managed_delta_corrupt_published_t version 1 exists

  Scenario: Managed Delta named-table reads ignore published versions beyond Unity latest
    Given statement
      """
      CREATE TABLE unity_table_test.managed_delta_future_published_t
      USING delta
      AS SELECT * FROM VALUES
        (1, 'one')
      AS t(id, name)
      """
    Given variable location for table unity_table_test.managed_delta_future_published_t
    Given published Delta commit for version 2 in location is invalid JSON
    When query
      """
      SELECT id, name FROM unity_table_test.managed_delta_future_published_t ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 1  | one  |
    Then Unity Catalog Delta commit for table unity_table_test.managed_delta_future_published_t version 1 exists

  Scenario: INSERT OVERWRITE on managed Delta table uses Unity ratified commit when published commit is corrupted
    Given statement
      """
      CREATE TABLE unity_table_test.managed_delta_overwrite_corrupt_published_t
      USING delta
      AS SELECT * FROM VALUES
        (1, 'old'),
        (2, 'old')
      AS t(id, name)
      """
    Given variable location for table unity_table_test.managed_delta_overwrite_corrupt_published_t
    Given published Delta commit for version 1 in location is invalid JSON
    Given statement
      """
      INSERT OVERWRITE TABLE unity_table_test.managed_delta_overwrite_corrupt_published_t VALUES
        (3, 'new'),
        (4, 'new')
      """
    When query
      """
      SELECT id, name FROM unity_table_test.managed_delta_overwrite_corrupt_published_t ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 3  | new  |
      | 4  | new  |
    Then Unity Catalog Delta commit for table unity_table_test.managed_delta_overwrite_corrupt_published_t version 2 references staged Delta commit in location

  Scenario: DELETE on managed Delta table uses Unity ratified commit when published commit is corrupted
    Given statement
      """
      CREATE TABLE unity_table_test.managed_delta_delete_corrupt_published_t
      USING delta
      AS SELECT * FROM VALUES
        (1, 'one'),
        (2, 'two'),
        (3, 'three')
      AS t(id, name)
      """
    Given variable location for table unity_table_test.managed_delta_delete_corrupt_published_t
    Given published Delta commit for version 1 in location is invalid JSON
    Given statement
      """
      DELETE FROM unity_table_test.managed_delta_delete_corrupt_published_t WHERE id = 2
      """
    When query
      """
      SELECT id, name FROM unity_table_test.managed_delta_delete_corrupt_published_t ORDER BY id
      """
    Then query result ordered
      | id | name  |
      | 1  | one   |
      | 3  | three |
    Then Unity Catalog Delta commit for table unity_table_test.managed_delta_delete_corrupt_published_t version 2 references staged Delta commit in location

  Scenario: MERGE on managed Delta table uses Unity ratified commit when published commit is corrupted
    Given statement
      """
      CREATE TABLE unity_table_test.managed_delta_merge_corrupt_published_t
      USING delta
      AS SELECT * FROM VALUES
        (1, 'one'),
        (2, 'two')
      AS t(id, name)
      """
    Given variable location for table unity_table_test.managed_delta_merge_corrupt_published_t
    Given published Delta commit for version 1 in location is invalid JSON
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW managed_delta_merge_corrupt_src AS
      SELECT * FROM VALUES
        (2, 'two updated'),
        (3, 'three')
      AS s(id, name)
      """
    Given statement
      """
      MERGE INTO unity_table_test.managed_delta_merge_corrupt_published_t AS t
      USING managed_delta_merge_corrupt_src AS s
      ON t.id = s.id
      WHEN MATCHED THEN
        UPDATE SET name = s.name
      WHEN NOT MATCHED THEN
        INSERT *
      """
    When query
      """
      SELECT id, name FROM unity_table_test.managed_delta_merge_corrupt_published_t ORDER BY id
      """
    Then query result ordered
      | id | name        |
      | 1  | one         |
      | 2  | two updated |
      | 3  | three       |
    Then Unity Catalog Delta commit for table unity_table_test.managed_delta_merge_corrupt_published_t version 2 references staged Delta commit in location

  Scenario: Managed Delta write succeeds when publish backfill fails after Unity ratification
    Given statement
      """
      CREATE TABLE unity_table_test.managed_delta_publish_failure_t (
        id INT,
        name STRING
      )
      USING delta
      """
    Given variable location for table unity_table_test.managed_delta_publish_failure_t
    Given published Delta commit path for version 1 in location is blocked
    Given statement
      """
      INSERT INTO unity_table_test.managed_delta_publish_failure_t VALUES
        (1, 'one')
      """
    Then Unity Catalog Delta commit for table unity_table_test.managed_delta_publish_failure_t version 1 references staged Delta commit in location without published backfill
    Then published Delta commit for version 1 in location is a directory

  Scenario: Managed Delta schema merge updates Unity Catalog metadata
    Given statement
      """
      CREATE TABLE unity_table_test.managed_delta_schema_merge_t (
        id INT,
        name STRING
      )
      USING delta
      """
    Given append id, name, and extra rows to Unity Catalog managed Delta table unity_table_test.managed_delta_schema_merge_t with mergeSchema
    Given variable location for table unity_table_test.managed_delta_schema_merge_t
    When query
      """
      SELECT id, name, extra FROM unity_table_test.managed_delta_schema_merge_t ORDER BY id
      """
    Then query result ordered
      | id | name | extra |
      | 1  | one  | new   |
    Then Unity Catalog table unity_table_test.managed_delta_schema_merge_t has columns
      | name  | type_name |
      | id    | INT       |
      | name  | STRING    |
      | extra | STRING    |
    Then Unity Catalog Delta commit for table unity_table_test.managed_delta_schema_merge_t version 1 references staged Delta commit in location

  Scenario: DELETE on managed Delta table is coordinated by Unity Catalog
    Given statement
      """
      CREATE TABLE unity_table_test.managed_delta_delete_t
      USING delta
      AS SELECT * FROM VALUES
        (1, 'one'),
        (2, 'two'),
        (3, 'three')
      AS t(id, name)
      """
    Given statement
      """
      DELETE FROM unity_table_test.managed_delta_delete_t WHERE id = 2
      """
    Given variable location for table unity_table_test.managed_delta_delete_t
    When query
      """
      SELECT id, name FROM unity_table_test.managed_delta_delete_t ORDER BY id
      """
    Then query result ordered
      | id | name  |
      | 1  | one   |
      | 3  | three |
    Then Unity Catalog table unity_table_test.managed_delta_delete_t is a managed Delta table
    Then Unity Catalog Delta commit for table unity_table_test.managed_delta_delete_t version 2 references staged Delta commit in location

  Scenario: DELETE can infer schema from managed Delta log when catalog columns are empty
    Given Unity Catalog managed Delta table unity_table_test.managed_delta_empty_catalog_schema_t exists with no catalog columns and id and name Delta schema
    Given final Unity Catalog managed table cleanup for unity_table_test.managed_delta_empty_catalog_schema_t
    Given statement
      """
      INSERT INTO unity_table_test.managed_delta_empty_catalog_schema_t VALUES
        (1, 'one'),
        (2, 'two'),
        (3, 'three')
      """
    Given statement
      """
      DELETE FROM unity_table_test.managed_delta_empty_catalog_schema_t WHERE id = 2
      """
    When query
      """
      SELECT id, name FROM unity_table_test.managed_delta_empty_catalog_schema_t ORDER BY id
      """
    Then query result ordered
      | id | name  |
      | 1  | one   |
      | 3  | three |

  Scenario: MERGE on managed Delta table is coordinated by Unity Catalog
    Given statement
      """
      CREATE TABLE unity_table_test.managed_delta_merge_t
      USING delta
      AS SELECT * FROM VALUES
        (1, 'one'),
        (2, 'two')
      AS t(id, name)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW managed_delta_merge_src AS
      SELECT * FROM VALUES
        (2, 'two updated'),
        (3, 'three')
      AS s(id, name)
      """
    Given statement
      """
      MERGE INTO unity_table_test.managed_delta_merge_t AS t
      USING managed_delta_merge_src AS s
      ON t.id = s.id
      WHEN MATCHED THEN
        UPDATE SET name = s.name
      WHEN NOT MATCHED THEN
        INSERT *
      """
    Given variable location for table unity_table_test.managed_delta_merge_t
    When query
      """
      SELECT id, name FROM unity_table_test.managed_delta_merge_t ORDER BY id
      """
    Then query result ordered
      | id | name        |
      | 1  | one         |
      | 2  | two updated |
      | 3  | three       |
    Then Unity Catalog table unity_table_test.managed_delta_merge_t is a managed Delta table
    Then Unity Catalog Delta commit for table unity_table_test.managed_delta_merge_t version 2 references staged Delta commit in location

  Scenario: Managed Delta table time travel replays catalog commits
    Given statement
      """
      CREATE TABLE unity_table_test.managed_delta_time_travel_t
      USING delta
      AS SELECT * FROM VALUES
        (1, 'one')
      AS t(id, name)
      """
    Given statement
      """
      INSERT INTO unity_table_test.managed_delta_time_travel_t VALUES
        (2, 'two')
      """
    Given variable location for table unity_table_test.managed_delta_time_travel_t
    When query
      """
      SELECT id, name FROM unity_table_test.managed_delta_time_travel_t VERSION AS OF 0 ORDER BY id
      """
    Then query result ordered
      | id | name |
    When query
      """
      SELECT id, name FROM unity_table_test.managed_delta_time_travel_t VERSION AS OF 1 ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 1  | one  |
    When query
      """
      SELECT id, name FROM unity_table_test.managed_delta_time_travel_t ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 1  | one  |
      | 2  | two  |
    Then Unity Catalog Delta commit for table unity_table_test.managed_delta_time_travel_t version 2 references staged Delta commit in location

  Scenario: Direct path read of catalog-managed Delta table requires catalog replay context
    Given statement
      """
      CREATE TABLE unity_table_test.managed_delta_path_read_t
      USING delta
      AS SELECT * FROM VALUES
        (1, 'one')
      AS t(id, name)
      """
    Given variable location for table unity_table_test.managed_delta_path_read_t
    When query template
      """
      SELECT id, name FROM delta.`{{ location.string }}` ORDER BY id
      """
    Then query error catalog commit replay context

  Scenario: Metadata ALTER operations are rejected for catalog-managed Delta tables
    Given statement
      """
      CREATE TABLE unity_table_test.managed_delta_alter_t (
        id INT,
        name STRING
      )
      USING delta
      """
    When query
      """
      SELECT id, name FROM unity_table_test.managed_delta_alter_t ORDER BY id
      """
    Then query result ordered
      | id | name |
    Given statement with error catalog-managed Delta tables
      """
      ALTER TABLE unity_table_test.managed_delta_alter_t
      SET TBLPROPERTIES ('my.tag' = 'blocked')
      """
    Given statement with error catalog-managed Delta tables
      """
      ALTER TABLE unity_table_test.managed_delta_alter_t
      ADD CONSTRAINT positive_id CHECK (id > 0)
      """
    Given statement with error catalog-managed Delta tables
      """
      ALTER TABLE unity_table_test.managed_delta_alter_t
      ALTER COLUMN name SET DEFAULT 'unknown'
      """

  Scenario: Partitioned managed Delta table bootstraps Delta metadata from Unity Catalog
    Given statement
      """
      CREATE TABLE unity_table_test.managed_delta_partitioned_t (
        id INT,
        bucket INT,
        name STRING
      )
      USING delta
      PARTITIONED BY (bucket)
      TBLPROPERTIES (my.tag = 'managed')
      """
    Given statement
      """
      INSERT INTO unity_table_test.managed_delta_partitioned_t VALUES
        (1, 10, 'one'),
        (2, 20, 'two')
      """
    Given variable location for table unity_table_test.managed_delta_partitioned_t
    When query
      """
      SELECT id, bucket, name FROM unity_table_test.managed_delta_partitioned_t ORDER BY id
      """
    Then query result ordered
      | id | bucket | name |
      | 1  | 10     | one  |
      | 2  | 20     | two  |
    Then Unity Catalog table unity_table_test.managed_delta_partitioned_t is a managed Delta table
    Then Unity Catalog table unity_table_test.managed_delta_partitioned_t storage location is under managed storage root
    Then Unity Catalog table unity_table_test.managed_delta_partitioned_t table id matches Delta metadata in location
    Then delta log first commit protocol and metadata contains
      | path                                | value     |
      | metaData.partitionColumns           | ["bucket"] |
      | metaData.configuration["my.tag"]    | "managed" |
      | metaData.configuration["delta.feature.catalogManaged"] | "supported" |
    Then Unity Catalog Delta commit for table unity_table_test.managed_delta_partitioned_t version 1 references staged Delta commit in location
