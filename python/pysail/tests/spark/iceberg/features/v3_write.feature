Feature: Iceberg v3 write semantics

  Background:
    Given variable location for temporary directory iceberg_v3_write
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_v3_write
      """

  Scenario: Nested UPDATE materializes null ancestors and uses original sibling values
    Given statement template
      """
      CREATE TABLE iceberg_v3_write (id INT, payload STRUCT<a:STRUCT<x:INT,y:INT>,b:STRUCT<z:INT>>)
      USING iceberg LOCATION {{ location.uri }} TBLPROPERTIES ('format-version'='3')
      """
    Given statement
      """
      INSERT INTO iceberg_v3_write VALUES
        (1, NULL),
        (2, named_struct('a', named_struct('x', 2, 'y', 3), 'b', NULL))
      """
    Given statement
      """
      UPDATE iceberg_v3_write SET payload.a.x = 9 WHERE id = 1
      """
    Given statement
      """
      UPDATE iceberg_v3_write SET payload.a.x = payload.a.y, payload.a.y = payload.a.x WHERE id = 2
      """
    When query
      """
      SELECT id, payload.a.x AS x, payload.a.y AS y, payload.b IS NULL AS b_null
      FROM iceberg_v3_write ORDER BY id
      """
    Then query result ordered
      | id | x | y    | b_null |
      | 1  | 9 | NULL | true   |
      | 2  | 3 | 2    | true   |

  Scenario: Nested partition sources retain their parent path during COW
    Given statement template
      """
      CREATE TABLE iceberg_v3_write (id INT, nested STRUCT<id:INT>, value INT)
      USING iceberg PARTITIONED BY (nested.id) LOCATION {{ location.uri }}
      TBLPROPERTIES ('format-version'='3')
      """
    Given statement
      """
      INSERT INTO iceberg_v3_write VALUES (1, named_struct('id', 10), 10), (2, NULL, 20)
      """
    Given statement
      """
      UPDATE iceberg_v3_write SET value = value + 100
      """
    When query
      """
      SELECT id, nested.id AS nested_id, value FROM iceberg_v3_write
      WHERE nested.id = 10 OR nested IS NULL ORDER BY id
      """
    Then query result ordered
      | id | nested_id | value |
      | 1  | 10        | 110   |
      | 2  | NULL      | 120   |

  Scenario: Unknown columns remain logical fields after COW
    Given statement template
      """
      CREATE TABLE iceberg_v3_write (id INT, unknown_value VOID, value INT)
      USING iceberg LOCATION {{ location.uri }} TBLPROPERTIES ('format-version'='3')
      """
    Given statement
      """
      INSERT INTO iceberg_v3_write VALUES (1, NULL, 10)
      """
    Given statement
      """
      UPDATE iceberg_v3_write SET value = 100 WHERE id = 1
      """
    When query
      """
      SELECT * FROM iceberg_v3_write
      """
    Then query result ordered
      | id | unknown_value | value |
      | 1  | NULL          | 100   |

  Scenario: Lossy casts do not prune matching identity partitions
    Given statement template
      """
      CREATE TABLE iceberg_v3_write (id INT, p DOUBLE)
      USING iceberg PARTITIONED BY (p) LOCATION {{ location.uri }}
      TBLPROPERTIES ('format-version'='3')
      """
    Given statement
      """
      INSERT INTO iceberg_v3_write VALUES (1, 16777217.0D)
      """
    When query
      """
      SELECT id FROM iceberg_v3_write WHERE CAST(p AS FLOAT) = CAST(16777216 AS FLOAT)
      """
    Then query result ordered
      | id |
      | 1  |
