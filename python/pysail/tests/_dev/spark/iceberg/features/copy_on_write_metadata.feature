Feature: Iceberg copy-on-write metadata and snapshot classification

  Background:
    Given variable location for temporary directory iceberg_cow_metadata
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_cow_metadata
      """

  Scenario Outline: COW rewrites preserve versioned metadata and manifests
    Given statement template
      """
      CREATE TABLE iceberg_cow_metadata (id INT, value INT, part STRING)
      USING iceberg PARTITIONED BY (part) LOCATION {{ location.uri }}
      TBLPROPERTIES ('format-version' = '<version>')
      """
    Given statement
      """
      INSERT INTO iceberg_cow_metadata VALUES (1, 10, 'A'), (2, 20, 'A')
      """
    Given statement
      """
      INSERT INTO iceberg_cow_metadata VALUES (3, 30, 'B'), (4, 40, 'B')
      """
    Given remember current iceberg data manifest paths
    Given statement
      """
      <statement>
      """
    Then iceberg current data manifests reuse 1 remembered paths
    Then iceberg snapshot count is 3
    Then iceberg snapshot operation is overwrite
    Then iceberg metadata matches snapshot
    Then iceberg current manifest list matches snapshot
    Then iceberg current snapshot summary matches snapshot
    When query
      """
      SELECT COUNT(*) AS count, SUM(id) AS ids, SUM(value) AS total FROM iceberg_cow_metadata
      """
    Then query result
      | count   | ids   | total   |
      | <count> | <ids> | <total> |

    Examples:
      | version | statement                                                                                                          | count | ids | total |
      | 1       | DELETE FROM iceberg_cow_metadata WHERE id = 1                                                                      | 3     | 9   | 90    |
      | 2       | DELETE FROM iceberg_cow_metadata WHERE id = 1                                                                      | 3     | 9   | 90    |
      | 1       | UPDATE iceberg_cow_metadata SET value = 100 WHERE id = 1                                                           | 4     | 10  | 190   |
      | 2       | UPDATE iceberg_cow_metadata SET value = 100 WHERE id = 1                                                           | 4     | 10  | 190   |
      | 1       | MERGE INTO iceberg_cow_metadata t USING (SELECT 1 AS id) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = 100 | 4     | 10  | 190   |
      | 2       | MERGE INTO iceberg_cow_metadata t USING (SELECT 1 AS id) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = 100 | 4     | 10  | 190   |

  Scenario Outline: A mixed MERGE with only runtime inserts creates an append snapshot
    Given statement template
      """
      CREATE TABLE iceberg_cow_metadata (id INT, value INT)
      USING iceberg LOCATION {{ location.uri }} TBLPROPERTIES ('format-version' = '<version>')
      """
    Given statement
      """
      INSERT INTO iceberg_cow_metadata VALUES (1, 10)
      """
    Given remember current iceberg data manifest paths
    Given statement
      """
      MERGE INTO iceberg_cow_metadata t USING (SELECT 2 AS id, 20 AS value) s
      ON t.id = s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
      """
    Then iceberg current data manifests reuse 1 remembered paths
    Then iceberg snapshot operation is append
    Then iceberg snapshot count is 2
    Then iceberg metadata matches snapshot
    Then iceberg current manifest list matches snapshot
    Then iceberg current snapshot summary matches snapshot
    When query
      """
      SELECT * FROM iceberg_cow_metadata ORDER BY id
      """
    Then query result ordered
      | id | value |
      | 1  | 10    |
      | 2  | 20    |

    Examples:
      | version |
      | 1       |
      | 2       |
