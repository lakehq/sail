Feature: Iceberg v3 merge-on-read deletion vectors

  Scenario: V3 MOR combines DELETE UPDATE and MERGE while preserving lineage and history
    Given variable location for temporary directory iceberg_v3_mor
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_v3_mor
      """
    Given statement template
      """
      CREATE TABLE iceberg_v3_mor (id INT, value INT) USING iceberg
      LOCATION {{ location.uri }} TBLPROPERTIES (
        'format-version' = '3', 'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read',
        'write.data.path' = 'custom_data')
      """
    Given statement
      """
      INSERT INTO iceberg_v3_mor SELECT /*+ COALESCE(1) */ * FROM VALUES
      (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)
      """
    Given remember current iceberg row lineage
    Given remember current iceberg data manifest paths
    Given statement
      """
      DELETE FROM iceberg_v3_mor WHERE id = 1
      """
    Then iceberg deletion vectors delete 1 rows across 1 files
    Then iceberg metadata contains
      | path                                         | value |
      | snapshots[1].summary['added-dvs']             | "1"   |
      | snapshots[1].summary['added-position-deletes'] | "1"   |
    Given statement
      """
      UPDATE iceberg_v3_mor SET value = 200 WHERE id = 2
      """
    Then iceberg deletion vectors delete 2 rows across 1 files
    Given statement
      """
      MERGE INTO iceberg_v3_mor t
      USING (SELECT * FROM VALUES (3, 300), (4, -1), (6, 600) AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED AND s.value < 0 THEN DELETE
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *
      """
    Then iceberg deletion vectors delete 4 rows across 1 files
    Then iceberg current data manifests reuse 1 remembered paths
    Then iceberg metadata contains
      | path                                              | value |
      | snapshots[3].summary['total-position-deletes']     | "4"   |
      | snapshots[3].summary['total-delete-files']         | "1"   |
      | snapshots[3].summary['removed-position-deletes']   | "2"   |
      | snapshots[3].summary['added-dvs']                  | "1"   |
      | snapshots[3].summary['removed-dvs']                | "1"   |
    Then iceberg row lineage matches
      | id | original_id | sequence |
      | 2  | 2           | 3        |
      | 3  | 3           | 4        |
      | 5  | 5           | 1        |
      | 6  | NEW         | 4        |
    When query
      """
      SELECT * FROM iceberg_v3_mor ORDER BY id
      """
    Then query result ordered
      | id | value |
      | 2  | 200   |
      | 3  | 300   |
      | 5  | 50    |
      | 6  | 600   |
    Given variable snapshot_ids for iceberg snapshot ids in location
    When query template
      """
      SELECT * FROM iceberg_v3_mor VERSION AS OF {{ snapshot_ids[1] }} ORDER BY id
      """
    Then query result ordered
      | id | value |
      | 2  | 20    |
      | 3  | 30    |
      | 4  | 40    |
      | 5  | 50    |
    Given statement
      """
      UPDATE iceberg_v3_mor SET value = 0 WHERE id = -1
      """
    Given statement
      """
      DELETE FROM iceberg_v3_mor WHERE id = -1
      """
    Given statement
      """
      MERGE INTO iceberg_v3_mor t USING (SELECT -1 AS id) s ON t.id = s.id WHEN MATCHED THEN DELETE
      """
    Then iceberg snapshot count is 4

  Scenario: V3 MOR partition moves keep one vector per source file
    Given variable location for temporary directory iceberg_v3_move
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_v3_move
      """
    Given statement template
      """
      CREATE TABLE iceberg_v3_move (id INT, part INT) USING iceberg PARTITIONED BY (part)
      LOCATION {{ location.uri }} TBLPROPERTIES (
        'format-version' = '3', 'write.update.mode' = 'merge-on-read',
        'write.delete.mode' = 'merge-on-read')
      """
    Given statement
      """
      INSERT INTO iceberg_v3_move SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 0), (2, 0), (3, 0), (4, 0)
      """
    Given statement
      """
      UPDATE iceberg_v3_move SET part = id WHERE id <= 2
      """
    Then iceberg deletion vectors delete 2 rows across 1 files
    Given statement
      """
      DELETE FROM iceberg_v3_move WHERE id = 3
      """
    Then iceberg deletion vectors delete 3 rows across 1 files
    When query
      """
      SELECT * FROM iceberg_v3_move ORDER BY id
      """
    Then query result ordered
      | id | part |
      | 1  | 1    |
      | 2  | 2    |
      | 4  | 0    |
    Given statement
      """
      DELETE FROM iceberg_v3_move WHERE part = 0
      """
    Then iceberg deletion vectors delete 0 rows across 0 files
    When query
      """
      SELECT * FROM iceberg_v3_move ORDER BY id
      """
    Then query result ordered
      | id | part |
      | 1  | 1    |
      | 2  | 2    |

  Scenario: Copy-on-write after V3 MOR removes vectors for rewritten data files
    Given variable location for temporary directory iceberg_v3_mixed
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_v3_mixed
      """
    Given statement template
      """
      CREATE TABLE iceberg_v3_mixed (id INT, value INT) USING iceberg
      LOCATION {{ location.uri }} TBLPROPERTIES ('format-version' = '3', 'write.delete.mode' = 'merge-on-read')
      """
    Given statement
      """
      INSERT INTO iceberg_v3_mixed SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 10), (2, 20), (3, 30)
      """
    Given remember current iceberg row lineage
    Given statement
      """
      DELETE FROM iceberg_v3_mixed WHERE id = 1
      """
    Then iceberg deletion vectors delete 1 rows across 1 files
    Given statement
      """
      UPDATE iceberg_v3_mixed SET value = 200 WHERE id = 2
      """
    Then iceberg deletion vectors delete 0 rows across 0 files
    Then iceberg row lineage matches
      | id | original_id | sequence |
      | 2  | 2           | 3        |
      | 3  | 3           | 1        |
    Then iceberg metadata contains
      | path                                      | value |
      | snapshots[2].summary['removed-dvs']        | "1"   |
      | snapshots[2].summary['total-delete-files'] | "0"   |
    When query
      """
      SELECT * FROM iceberg_v3_mixed ORDER BY id
      """
    Then query result ordered
      | id | value |
      | 2  | 200   |
      | 3  | 30    |

  Scenario: V3 MOR incorporates position deletes after upgrading a V2 table
    Given variable location for temporary directory iceberg_v3_upgrade
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_v3_upgrade
      """
    Given statement template
      """
      CREATE TABLE iceberg_v3_upgrade (id INT, value INT) USING iceberg
      LOCATION {{ location.uri }} TBLPROPERTIES (
        'format-version' = '2', 'write.merge.mode' = 'merge-on-read', 'write.delete.mode' = 'merge-on-read')
      """
    Given statement
      """
      INSERT INTO iceberg_v3_upgrade SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 10), (2, 20), (3, 30)
      """
    Given statement
      """
      MERGE INTO iceberg_v3_upgrade t USING (SELECT 1 AS id) s ON t.id = s.id WHEN MATCHED THEN DELETE
      """
    Given statement
      """
      ALTER TABLE iceberg_v3_upgrade SET TBLPROPERTIES ('format-version' = '3')
      """
    Given statement
      """
      DELETE FROM iceberg_v3_upgrade WHERE id = 2
      """
    Then iceberg deletion vectors delete 2 rows across 1 files
    When query
      """
      SELECT * FROM iceberg_v3_upgrade ORDER BY id
      """
    Then query result ordered
      | id | value |
      | 3  | 30    |

  Scenario Outline: MOR preserves earlier deletes after partition type promotion
    Given variable location for temporary directory iceberg_mor_promotion
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_mor_promotion
      """
    Given statement template
      """
      CREATE TABLE iceberg_mor_promotion (id INT, part <original_type>)
      USING iceberg PARTITIONED BY (part) LOCATION {{ location.uri }}
      TBLPROPERTIES ('format-version' = '<version>',
        'write.merge.mode' = 'merge-on-read', 'write.delete.granularity' = 'file')
      """
    Given statement
      """
      INSERT INTO iceberg_mor_promotion SELECT /*+ COALESCE(1) */ * FROM VALUES
      (1, <partition_value>), (2, <partition_value>), (3, <partition_value>)
      """
    Given iceberg current schema has fields
      """
      [
        {"id": 1, "name": "id", "required": false, "type": "int"},
        {"id": 2, "name": "part", "required": false, "type": "<promoted_type>"}
      ]
      """
    Given statement
      """
      MERGE INTO iceberg_mor_promotion t USING (SELECT 1 AS id) s
      ON t.id = s.id WHEN MATCHED THEN DELETE
      """
    When query
      """
      SELECT id FROM iceberg_mor_promotion ORDER BY id
      """
    Then query result ordered
      | id |
      | 2  |
      | 3  |
    Given statement
      """
      MERGE INTO iceberg_mor_promotion t USING (SELECT 2 AS id) s
      ON t.id = s.id WHEN MATCHED THEN DELETE
      """
    When query
      """
      SELECT id FROM iceberg_mor_promotion ORDER BY id
      """
    Then query result ordered
      | id |
      | 3  |
    Then iceberg metadata contains
      | path                                          | value |
      | snapshots[2].summary['<file_count_property>']  | "1"   |
      | snapshots[2].summary['total-position-deletes'] | "2"   |

    Examples:
      | version | original_type | promoted_type | partition_value   | file_count_property         |
      | 2       | INT           | long          | 7                 | added-position-delete-files |
      | 3       | INT           | long          | 7                 | added-dvs                   |
      | 2       | FLOAT         | double        | CAST(0.1 AS FLOAT) | added-position-delete-files |
      | 3       | FLOAT         | double        | CAST(0.1 AS FLOAT) | added-dvs                   |

  Scenario: V3 MOR applies positions across scan batches
    Given variable location for temporary directory iceberg_v3_batches
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_v3_batches
      """
    Given statement template
      """
      CREATE TABLE iceberg_v3_batches (id BIGINT, value BIGINT) USING iceberg
      LOCATION {{ location.uri }} TBLPROPERTIES (
        'format-version' = '3', 'write.delete.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read')
      """
    Given statement
      """
      INSERT INTO iceberg_v3_batches SELECT /*+ COALESCE(1) */ id, id AS value FROM range(18000)
      """
    Given statement
      """
      DELETE FROM iceberg_v3_batches WHERE id IN (8191, 8192, 16383, 17999)
      """
    Given statement
      """
      UPDATE iceberg_v3_batches SET value = -1 WHERE id = 8193
      """
    Then iceberg deletion vectors delete 5 rows across 1 files
    When query
      """
      SELECT count(*) AS total, count(DISTINCT id) AS ids, sum(CASE WHEN value = -1 THEN 1 ELSE 0 END) AS updated
      FROM iceberg_v3_batches
      """
    Then query result
      | total | ids   | updated |
      | 17996 | 17996 | 1       |
    When query
      """
      SELECT id, value FROM iceberg_v3_batches WHERE id BETWEEN 8190 AND 8194 ORDER BY id
      """
    Then query result ordered
      | id   | value |
      | 8190 | 8190  |
      | 8193 | -1    |
      | 8194 | 8194  |

  Scenario: V3 MOR deletes duplicate rows with complex values by position
    Given variable location for temporary directory iceberg_v3_complex
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_v3_complex
      """
    Given statement template
      """
      CREATE TABLE iceberg_v3_complex (id INT, payload STRUCT<a: ARRAY<INT>>) USING iceberg
      LOCATION {{ location.uri }} TBLPROPERTIES ('format-version' = '3', 'write.delete.mode' = 'merge-on-read')
      """
    Given statement
      """
      INSERT INTO iceberg_v3_complex SELECT /*+ COALESCE(1) */ * FROM VALUES
      (1, named_struct('a', array(1, 2))), (1, named_struct('a', array(1, 2))), (2, NULL)
      """
    Given statement
      """
      DELETE FROM iceberg_v3_complex WHERE id = 1
      """
    Then iceberg deletion vectors delete 2 rows across 1 files
    When query
      """
      SELECT id, payload IS NULL AS missing FROM iceberg_v3_complex
      """
    Then query result
      | id | missing |
      | 2  | true    |
