Feature: Iceberg initial and write defaults

  Scenario: SQL writes distinguish missing fields and DEFAULT from explicit NULL
    Given variable location for temporary directory iceberg_defaults
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_defaults
      """
    Given statement template
      """
      CREATE TABLE iceberg_defaults (id BIGINT) USING iceberg
      LOCATION {{ location.uri }} TBLPROPERTIES ('format-version' = '3')
      """
    Given statement
      """
      INSERT INTO iceberg_defaults VALUES (1L)
      """
    Given iceberg current schema has fields
      """
      [
        {"id": 1, "name": "id", "required": false, "type": "long"},
        {"id": 2, "name": "value", "required": true, "type": "long", "initial-default": 7, "write-default": 9},
        {"id": 3, "name": "point", "required": false,
         "type": {"type": "struct", "fields": [
           {"id": 4, "name": "x", "required": true, "type": "long", "initial-default": 19, "write-default": 23}
         ]}, "initial-default": {}, "write-default": {}},
        {"id": 5, "name": "tags", "required": false,
         "type": {"type": "list", "element-id": 6, "element": "long", "element-required": false},
         "initial-default": [1, 2], "write-default": [3]},
        {"id": 7, "name": "read_only", "required": false, "type": "long", "initial-default": 11}
      ]
      """
    Given statement
      """
      INSERT INTO iceberg_defaults (id) VALUES (2L)
      """
    Given statement
      """
      INSERT INTO iceberg_defaults VALUES (3L, DEFAULT, DEFAULT, DEFAULT, DEFAULT)
      """
    Given statement
      """
      INSERT INTO iceberg_defaults VALUES (4L, 40L, NULL, NULL, NULL)
      """
    Given statement
      """
      UPDATE iceberg_defaults SET value = DEFAULT WHERE id = 1
      """
    Given statement
      """
      MERGE INTO iceberg_defaults t USING (SELECT 5L AS id) s ON t.id = s.id
      WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)
      """
    When query
      """
      SELECT id, value, point.x AS x, tags, read_only FROM iceberg_defaults ORDER BY id
      """
    Then query result ordered
      | id | value | x    | tags   | read_only |
      | 1  | 9     | 19   | [1, 2] | 11        |
      | 2  | 9     | 23   | [3]    | NULL      |
      | 3  | 9     | 23   | [3]    | NULL      |
      | 4  | 40    | NULL | NULL   | NULL      |
      | 5  | 9     | 23   | [3]    | NULL      |

  Scenario: Named INSERT fills missing nullable fields without a write default
    Given variable location for temporary directory iceberg_missing_fields
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_missing_fields
      """
    Given statement template
      """
      CREATE TABLE iceberg_missing_fields (id BIGINT, value STRING) USING iceberg
      LOCATION {{ location.uri }}
      """
    Given statement
      """
      INSERT INTO iceberg_missing_fields (id) VALUES (1L), (2L)
      """
    When query
      """
      SELECT * FROM iceberg_missing_fields ORDER BY id
      """
    Then query result ordered
      | id | value |
      | 1  | NULL  |
      | 2  | NULL  |
