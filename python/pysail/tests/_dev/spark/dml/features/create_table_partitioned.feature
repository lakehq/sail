Feature: CREATE TABLE with PARTITIONED BY clause

  Scenario: CREATE TABLE with typed partition column not in column list
    Given variable location for temporary directory create_table_hive_partition
    Given final statement
      """
      DROP TABLE IF EXISTS create_table_hive_partition
      """
    Given statement template
      """
      CREATE TABLE create_table_hive_partition (id INT)
      USING PARQUET
      LOCATION {{ location.sql }}
      PARTITIONED BY (part STRING)
      """
    Given statement template
      """
      INSERT INTO create_table_hive_partition VALUES (1, 'a'), (2, 'b')
      """
    When query
      """
      SELECT * FROM create_table_hive_partition ORDER BY id
      """
    Then query result ordered
      | id | part |
      | 1  | a    |
      | 2  | b    |


  Scenario: CREATE TABLE with typed partition column already in column list
    Given variable location for temporary directory create_table_typed_part_match
    Given final statement
      """
      DROP TABLE IF EXISTS create_table_typed_part_match
      """
    Given statement template
      """
      CREATE TABLE create_table_typed_part_match (id INT, part STRING)
      USING PARQUET
      LOCATION {{ location.sql }}
      PARTITIONED BY (part STRING)
      """
    Given statement template
      """
      INSERT INTO create_table_typed_part_match VALUES (1, 'a')
      """
    When query
      """
      SELECT * FROM create_table_typed_part_match ORDER BY id
      """
    Then query result ordered
      | id | part |
      | 1  | a    |


  Scenario: CREATE TABLE fails when PARTITIONED BY type conflicts with column definition
    Given variable location for temporary directory create_table_type_mismatch
    Given final statement
      """
      DROP TABLE IF EXISTS create_table_type_mismatch
      """
    Given statement template with error (?i)partition column 'part' has incompatible type
      """
      CREATE TABLE create_table_type_mismatch (id INT, part STRING)
      USING PARQUET
      LOCATION {{ location.sql }}
      PARTITIONED BY (part INT)
      """
