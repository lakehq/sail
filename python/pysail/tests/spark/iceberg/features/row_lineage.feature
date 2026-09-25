Feature: Iceberg v3 row lineage

  Scenario Outline: MERGE after positional skips preserves inherited and stored lineage
    Given variable location for temporary directory iceberg_selected_lineage
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_selected_lineage
      """
    Given statement template
      """
      CREATE TABLE iceberg_selected_lineage (id BIGINT, value BIGINT) USING iceberg
      LOCATION {{ location.uri }} TBLPROPERTIES (
        'format-version' = '3', 'write.merge.mode' = 'merge-on-read',
        'write.parquet.row-group-size-bytes' = '16384')
      """
    Given statement
      """
      INSERT INTO iceberg_selected_lineage SELECT /*+ COALESCE(1) */ id, id AS value FROM range(18000)
      """
    Given statement
      """
      MERGE INTO iceberg_selected_lineage t
      USING (SELECT id FROM range(18000) WHERE id < 128 OR id IN (8191, 8192, 16383)) s
      ON t.id = s.id WHEN MATCHED THEN DELETE
      """
    Given remember current iceberg row lineage
    Given statement
      """
      ALTER TABLE iceberg_selected_lineage SET TBLPROPERTIES ('write.merge.mode' = '<mode>')
      """
    Given statement
      """
      MERGE INTO iceberg_selected_lineage t
      USING (SELECT * FROM VALUES (8193L, -1L), (17999L, -2L) AS s(id, value)) s
      ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value
      """
    Then iceberg row lineage preserves IDs and only changes these sequences
      | id    | sequence |
      | 8193  | 3        |
      | 17999 | 3        |
    Given statement
      """
      MERGE INTO iceberg_selected_lineage t USING (SELECT 8193L AS id) s
      ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = -3
      """
    Then iceberg row lineage preserves IDs and only changes these sequences
      | id    | sequence |
      | 8193  | 4        |
      | 17999 | 3        |
    When query
      """
      SELECT count(*) AS total, count(DISTINCT id) AS ids FROM iceberg_selected_lineage
      """
    Then query result
      | total | ids   |
      | 17869 | 17869 |
    When query
      """
      SELECT * FROM iceberg_selected_lineage WHERE id IN (8190, 8191, 8192, 8193, 16383, 17999) ORDER BY id
      """
    Then query result ordered
      | id    | value |
      | 8190  | 8190  |
      | 8193  | -3    |
      | 17999 | -2    |
    Given variable snapshot_ids for iceberg snapshot ids in location
    When query template
      """
      SELECT * FROM iceberg_selected_lineage VERSION AS OF {{ snapshot_ids[1] }}
      WHERE id IN (8193, 17999) ORDER BY id
      """
    Then query result ordered
      | id    | value |
      | 8193  | 8193  |
      | 17999 | 17999 |
    Then iceberg snapshot count is 4

    Examples:
      | mode          |
      | merge-on-read |
      | copy-on-write |

  Scenario: Partitioned COW preserves file-local row IDs across input batches
    Given variable location for temporary directory iceberg_lineage_batches
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_lineage_batches
      """
    Given statement template
      """
      CREATE TABLE iceberg_lineage_batches (id BIGINT, value BIGINT, part INT) USING iceberg
      PARTITIONED BY (part) LOCATION {{ location.uri }} TBLPROPERTIES ('format-version' = '3')
      """
    Given statement
      """
      INSERT INTO iceberg_lineage_batches
      SELECT /*+ COALESCE(1) */ id, id AS value, CASE WHEN id < 9000 THEN 0 ELSE 1 END AS part
      FROM range(18000)
      """
    Given remember current iceberg row lineage
    Given statement
      """
      UPDATE iceberg_lineage_batches SET value = -1 WHERE id IN (8999, 17999)
      """
    Then iceberg row lineage preserves IDs and only changes these sequences
      | id    | sequence |
      | 8999  | 2        |
      | 17999 | 2        |
    When query
      """
      SELECT count(*) AS total, count(DISTINCT id) AS ids,
             sum(CASE WHEN value = -1 THEN 1 ELSE 0 END) AS updated
      FROM iceberg_lineage_batches
      """
    Then query result
      | total | ids   | updated |
      | 18000 | 18000 | 2       |
    Then iceberg snapshot count is 2

  Scenario: First COW after upgrading to v3 assigns lineage to surviving rows
    Given variable location for temporary directory iceberg_lineage_upgrade
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_lineage_upgrade
      """
    Given statement template
      """
      CREATE TABLE iceberg_lineage_upgrade (id INT, value INT) USING iceberg
      LOCATION {{ location.uri }} TBLPROPERTIES ('format-version' = '2')
      """
    Given statement
      """
      INSERT INTO iceberg_lineage_upgrade
      SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 10), (2, 20), (3, 30)
      """
    Given statement
      """
      ALTER TABLE iceberg_lineage_upgrade SET TBLPROPERTIES ('format-version' = '3')
      """
    Given statement
      """
      UPDATE iceberg_lineage_upgrade SET value = 100 WHERE id = 1
      """
    Given remember current iceberg row lineage
    Then iceberg row lineage matches
      | id | original_id | sequence |
      | 1  | 1           | 2        |
      | 2  | 2           | 2        |
      | 3  | 3           | 2        |
    Given statement
      """
      UPDATE iceberg_lineage_upgrade SET value = 200 WHERE id = 2
      """
    Then iceberg row lineage matches
      | id | original_id | sequence |
      | 1  | 1           | 2        |
      | 2  | 2           | 3        |
      | 3  | 3           | 2        |
    When query
      """
      SELECT * FROM iceberg_lineage_upgrade ORDER BY id
      """
    Then query result ordered
      | id | value |
      | 1  | 100   |
      | 2  | 200   |
      | 3  | 30    |
    Then iceberg snapshot count is 3

  Scenario Outline: COW preserves row IDs and updates sequence numbers across rewrites
    Given variable location for temporary directory iceberg_lineage
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_lineage
      """
    Given statement template
      """
      CREATE TABLE iceberg_lineage (id INT, value INT) USING iceberg
      LOCATION {{ location.uri }} TBLPROPERTIES ('format-version' = '<version>')
      """
    Given statement
      """
      INSERT INTO iceberg_lineage VALUES (1, 10), (2, 20), (3, 30)
      """
    Given statement
      """
      ALTER TABLE iceberg_lineage SET TBLPROPERTIES ('format-version' = '3')
      """
    Given statement
      """
      INSERT INTO iceberg_lineage VALUES (99, 99)
      """
    Given remember current iceberg row lineage
    Given statement
      """
      UPDATE iceberg_lineage SET value = 100 WHERE id = 1
      """
    Then iceberg row lineage matches
      | id | original_id | sequence |
      | 1  | 1           | 3        |
      | 2  | 2           | 1        |
      | 3  | 3           | 1        |
      | 99 | 99          | 2        |
    Given statement
      """
      DELETE FROM iceberg_lineage WHERE id = 2
      """
    Then iceberg row lineage matches
      | id | original_id | sequence |
      | 1  | 1           | 3        |
      | 3  | 3           | 1        |
      | 99 | 99          | 2        |
    Given statement
      """
      MERGE INTO iceberg_lineage AS t
      USING (SELECT * FROM VALUES (1, 101), (4, 40) AS s(id, value)) AS s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET id = 10, value = s.value
      WHEN NOT MATCHED THEN INSERT *
      WHEN NOT MATCHED BY SOURCE AND t.id = 99 THEN DELETE
      """
    Then iceberg row lineage matches
      | id | original_id | sequence |
      | 10 | 1           | 5        |
      | 3  | 3           | 1        |
      | 4  | NEW         | 5        |
    Given statement
      """
      MERGE INTO iceberg_lineage AS t USING (SELECT 5 AS id, 50 AS value) s
      ON t.id = s.id WHEN NOT MATCHED THEN INSERT *
      """
    Then iceberg row lineage matches
      | id | original_id | sequence |
      | 10 | 1           | 5        |
      | 3  | 3           | 1        |
      | 4  | NEW         | 5        |
      | 5  | NEW         | 6        |
    When query
      """
      SELECT * FROM iceberg_lineage ORDER BY id
      """
    Then query result ordered
      | id | value |
      | 3  | 30    |
      | 4  | 40    |
      | 5  | 50    |
      | 10 | 101   |
    Then iceberg snapshot count is 6

    Examples:
      | version |
      | 2       |
      | 3       |
